"""Kubernetes access layer.

Everything the web app knows about the cluster comes through this adapter.
The simulator in ``simulator/`` speaks the same API shapes, so the app can be
exercised end to end without a real control plane.

Two behaviours are deliberate:

* ``cordon`` uses a merge patch so that a node is marked unschedulable before
  any pod is listed.  The UI never offers pods for eviction before that call
  succeeded.
* ``evict`` uses the ``policy/v1`` Eviction subresource with a UID
  precondition.  A pod that was deleted and re-created under the same name is a
  different object; the UID precondition makes the API server refuse to evict
  the replacement on behalf of the old decision.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.watch.watch import iter_resp_lines
from urllib3.exceptions import HTTPError as HttpTransportError

from .models import NodeInfo, Pod, PodBudget


class KubeError(RuntimeError):
    pass


class ResourceExpired(KubeError):
    """The watch's resourceVersion is older than the server still retains."""


class WatchUnavailable(KubeError):
    """A watch could not be established (for example, a rehearsed outage)."""


class EvictionBlocked(Exception):
    """The API server refused the eviction because of a PodDisruptionBudget."""


class EvictionConflict(Exception):
    """The pod object changed or vanished between listing and evicting."""


class WatchStream:
    """A live watch response, parsed one JSON event at a time.

    The Kubernetes generated client's ``Watch`` helper silently retries once
    after a 410 and hides whether a watch connection was actually accepted.
    The NodePort console needs both facts: 410 must trigger a full list (so
    deleted slices disappear), and a broken connection must be visible as
    "待同步".  Reading the response ourselves keeps that control.
    """

    def __init__(self, response):
        self.response = response
        self._closed = False

    def __iter__(self):
        try:
            for line in iter_resp_lines(self.response):
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if isinstance(event, dict):
                    yield event
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        response = self.response
        self.response = None
        try:
            response.close()
        except Exception:
            pass
        try:
            response.release_conn()
        except Exception:
            pass


def _controller_kind(pod: client.V1Pod) -> str:
    for ref in pod.metadata.owner_references or []:
        if ref.controller:
            return ref.kind
    return ""


def _is_static(pod: client.V1Pod) -> bool:
    annotations = pod.metadata.annotations or {}
    if "kubernetes.io/config.mirror" in annotations:
        return True
    source = annotations.get("kubernetes.io/config.source", "")
    return not pod.metadata.owner_references and source in ("file", "http")


def _labels_match(selector: client.V1LabelSelector | None, labels: dict[str, str]) -> bool:
    if selector is None:
        return False
    for key, value in (selector.match_labels or {}).items():
        if labels.get(key) != value:
            return False
    for expr in selector.match_expressions or []:
        values = expr.values or []
        present = expr.key in labels
        if expr.operator == "In" and labels.get(expr.key) not in values:
            return False
        if expr.operator == "NotIn" and present and labels.get(expr.key) in values:
            return False
        if expr.operator == "Exists" and not present:
            return False
        if expr.operator == "DoesNotExist" and present:
            return False
    return True


def _budget_from_pdb(pdb: client.V1PodDisruptionBudget) -> PodBudget:
    status = pdb.status
    stale = bool(
        status is None
        or (
            status.observed_generation is not None
            and pdb.metadata.generation is not None
            and status.observed_generation != pdb.metadata.generation
        )
    )
    return PodBudget(
        name=pdb.metadata.name,
        namespace=pdb.metadata.namespace,
        allowed=max(0, status.disruptions_allowed or 0) if status else 0,
        healthy=(status.current_healthy or 0) if status else 0,
        desired=(status.desired_healthy or 0) if status else 0,
        expected=(status.expected_pods or 0) if status else 0,
        stale=stale,
    )


def pods_matching_budgets(
    pod: Pod, budgets: list[client.V1PodDisruptionBudget]
) -> list[PodBudget]:
    """Budgets whose selector covers this pod.

    Informational for the UI only.  The authoritative decision about whether
    an eviction is within budget belongs to the API server: it rejects the
    eviction with HTTP 429 and the app marks that pod blocked for this round.
    """
    matched: list[PodBudget] = []
    for pdb in budgets:
        if pdb.metadata.namespace != pod.namespace:
            continue
        if not _labels_match(getattr(pdb.spec, "selector", None), pod.labels):
            continue
        # A stale status is still shown (flagged) because the API server may
        # still refuse the eviction; only it has the authoritative numbers.
        matched.append(_budget_from_pdb(pdb))
    return matched


def _status_message(exc: ApiException) -> str:
    body = exc.body
    if isinstance(body, bytes):
        try:
            body = body.decode()
        except UnicodeDecodeError:
            body = ""
    if isinstance(body, str) and body:
        try:
            parsed = json.loads(body)
        except ValueError:
            return body[:200]
        if isinstance(parsed, dict) and parsed.get("message"):
            return str(parsed["message"])
    return exc.reason or f"HTTP {exc.status}"


class KubeClient:
    def __init__(self, api: client.ApiClient):
        self._api = api
        self.core = client.CoreV1Api(api)
        self.policy = client.PolicyV1Api(api)
        self.discovery = client.DiscoveryV1Api(api)

    # -- construction ------------------------------------------------------
    @classmethod
    def in_cluster(cls) -> "KubeClient":
        try:
            config.load_incluster_config()
        except Exception as exc:  # pragma: no cover - environment dependent
            raise KubeError(f"无法加载集群内配置: {exc}") from exc
        return cls(client.ApiClient())

    @classmethod
    def from_incluster(
        cls, host: str | None = None, token: str | None = None, ca_cert: str | None = None
    ) -> "KubeClient":
        """In-cluster config, with optional explicit host/token for tests."""
        host = host or os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        base = f"https://{host}:{port}" if host and not host.startswith("http") else host
        sa_dir = Path("/var/run/secrets/kubernetes.io/serviceaccount")
        sa_token = sa_dir / "token"
        if not token and sa_token.exists():
            token = sa_token.read_text().strip()
        sa_ca = sa_dir / "ca.crt"
        if not ca_cert and sa_ca.exists():
            ca_cert = str(sa_ca)
        if not base:
            try:
                config.load_incluster_config()
                return cls(client.ApiClient())
            except Exception as exc:
                raise KubeError(f"无法加载集群内配置: {exc}") from exc
        if not token:
            raise KubeError("集群内模式缺少 service account token")
        return cls.from_host(base, token=token, ca_cert=ca_cert)

    @classmethod
    def from_kubeconfig(
        cls, path: str | None = None, context: str | None = None
    ) -> "KubeClient":
        try:
            config.load_kube_config(config_file=path, context=context)
        except Exception as exc:
            raise KubeError(f"无法加载 kubeconfig: {exc}") from exc
        return cls(client.ApiClient())

    @classmethod
    def from_host(
        cls,
        host: str,
        token: str | None = None,
        verify_ssl: bool = True,
        ca_cert: str | None = None,
    ) -> "KubeClient":
        conf = client.Configuration(host=host)
        conf.verify_ssl = verify_ssl
        if ca_cert:
            conf.ssl_ca_cert = ca_cert
        if token:
            conf.api_key["authorization"] = token
            conf.api_key_prefix["authorization"] = "Bearer"
        return cls(client.ApiClient(conf))

    # -- reads -------------------------------------------------------------
    @staticmethod
    def _to_node_info(node: client.V1Node) -> NodeInfo:
        ready = any(
            condition.type == "Ready" and condition.status == "True"
            for condition in (node.status.conditions or [])
        ) if node.status else False
        addresses: list[str] = []
        for address in (node.status.addresses or []) if node.status else []:
            if address.type in ("InternalIP", "ExternalIP") and address.address:
                addresses.append(address.address)
        return NodeInfo(
            name=node.metadata.name,
            unschedulable=bool(node.spec.unschedulable),
            ready=ready,
            addresses=addresses,
        )

    @staticmethod
    def _is_worker(node: client.V1Node) -> bool:
        labels = node.metadata.labels or {}
        return "node-role.kubernetes.io/control-plane" not in labels

    def _list_all_pods(self) -> list[client.V1Pod]:
        """All pods, following pagination when the server sets a continue token."""
        items: list[client.V1Pod] = []
        token: str | None = None
        while True:
            page = self.core.list_pod_for_all_namespaces(limit=500, _continue=token)
            items.extend(page.items or [])
            token = page.metadata._continue if page.metadata else None
            if not token:
                return items

    def list_nodes(self) -> list[NodeInfo]:
        try:
            nodes = [n for n in self.core.list_node().items if self._is_worker(n)]
            pods = self._list_all_pods()
        except ApiException as exc:
            raise KubeError(f"读取节点失败: {exc.status} {exc.reason}") from exc
        counts: dict[str, int] = {}
        for pod in pods:
            node_name = pod.spec.node_name
            if node_name:
                counts[node_name] = counts.get(node_name, 0) + 1
        infos = []
        for node in nodes:
            info = self._to_node_info(node)
            info.pod_count = counts.get(node.metadata.name, 0)
            infos.append(info)
        return sorted(infos, key=lambda n: n.name)

    def get_node(self, name: str) -> NodeInfo | None:
        try:
            return self._to_node_info(self.core.read_node(name))
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise KubeError(f"读取节点 {name} 失败: {exc.status} {exc.reason}") from exc

    def list_pods_on_node(self, node: str) -> list[Pod]:
        items: list[client.V1Pod] = []
        token: str | None = None
        try:
            while True:
                page = self.core.list_pod_for_all_namespaces(
                    field_selector=f"spec.nodeName={node}", limit=500, _continue=token
                )
                items.extend(page.items or [])
                token = page.metadata._continue if page.metadata else None
                if not token:
                    break
        except ApiException as exc:
            raise KubeError(
                f"读取节点 {node} 上的 Pod 失败: {exc.status} {exc.reason}"
            ) from exc
        return [self._to_pod(pod) for pod in items]

    @staticmethod
    def _to_pod(pod: client.V1Pod) -> Pod:
        deleted = pod.metadata.deletion_timestamp
        return Pod(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            uid=pod.metadata.uid,
            node=pod.spec.node_name,
            phase=(pod.status.phase or "") if pod.status else "",
            terminating=deleted is not None,
            daemonset=_controller_kind(pod) == "DaemonSet",
            static=_is_static(pod),
            owner_kind=_controller_kind(pod),
            labels=dict(pod.metadata.labels or {}),
            deletion_timestamp=deleted.isoformat() if deleted else None,
        )

    def get_pod(self, namespace: str, name: str) -> Pod | None:
        try:
            return self._to_pod(self.core.read_namespaced_pod(name, namespace))
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise KubeError(
                f"读取 Pod {namespace}/{name} 失败: {exc.status} {exc.reason}"
            ) from exc

    def list_budgets(self) -> list[client.V1PodDisruptionBudget]:
        try:
            return self.policy.list_pod_disruption_budget_for_all_namespaces().items
        except ApiException as exc:
            raise KubeError(f"读取 PDB 失败: {exc.status} {exc.reason}") from exc

    # -- writes ------------------------------------------------------------
    def cordon(self, node: str) -> None:
        try:
            self.core.patch_node(
                node,
                {"spec": {"unschedulable": True}},
                _content_type="application/merge-patch+json",
            )
        except ApiException as exc:
            raise KubeError(
                f"封锁节点 {node} 失败: {exc.status} {_status_message(exc)}"
            ) from exc

    def evict(self, pod: Pod) -> None:
        """Evict one pod by UID.

        Raises ``EvictionBlocked`` when the API server refuses because of a
        disruption budget, ``EvictionConflict`` when the pod object changed
        underneath us, and ``KubeError`` for anything else.
        """
        eviction = client.V1Eviction(
            api_version="policy/v1",
            kind="Eviction",
            metadata=client.V1ObjectMeta(name=pod.name, namespace=pod.namespace),
            delete_options=client.V1DeleteOptions(
                api_version="v1",
                kind="DeleteOptions",
                preconditions=client.V1Preconditions(uid=pod.uid),
            ),
        )
        try:
            self.core.create_namespaced_pod_eviction(pod.name, pod.namespace, eviction)
        except ApiException as exc:
            if exc.status == 429:
                raise EvictionBlocked(_status_message(exc)) from exc
            if exc.status in (404, 409):
                raise EvictionConflict(_status_message(exc)) from exc
            raise KubeError(
                f"驱逐 {pod.namespace}/{pod.name} 失败: {exc.status} "
                f"{_status_message(exc)}"
            ) from exc

    # -- NodePort entry points ---------------------------------------------
    #
    # The console only understands the strict subset of Services where the
    # node's own kube-proxy is the thing answering on the NodePort: an IPv4
    # TCP service with ``externalTrafficPolicy: Local``.  With ``Cluster`` a
    # node may forward to a backend on another node, so "which backends are
    # reachable from *this* node" has no per-node answer; anything else is
    # rejected rather than guessed at.
    def get_service(self, namespace: str, name: str) -> client.V1Service | None:
        try:
            return self.core.read_namespaced_service(name, namespace)
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise KubeError(
                f"读取 Service {namespace}/{name} 失败: {exc.status} {_status_message(exc)}"
            ) from exc
        except HttpTransportError as exc:
            raise KubeError(f"无法连接 API Server：{exc}") from exc

    def list_endpoint_slices(
        self, namespace: str, service: str, timeout: float | None = None
    ) -> tuple[list[dict], str]:
        """One consistent list of the slices for a service.

        Returns ``(slices, resource_version)``.  Raw dictionaries are used so
        the same shape feeds both the first render and the watch's ``UPDATE``
        events.  ``resourceVersion`` is taken from the list itself: a watch
        started from it is guaranteed to include every change after the
        snapshot, so nothing is missed between list and watch.

        ``timeout`` bounds the request so an unreachable API server cannot
        stall the first page render for the client's default timeout.
        """
        selector = f"kubernetes.io/service-name={service}"
        items: list[dict] = []
        token: str | None = None
        resource_version = ""
        try:
            while True:
                page = self.discovery.list_namespaced_endpoint_slice(
                    namespace,
                    label_selector=selector,
                    limit=500,
                    _continue=token,
                    _request_timeout=timeout,
                )
                items.extend(self._api.sanitize_for_serialization(item) for item in page.items or [])
                metadata = page.metadata
                if metadata is not None and metadata.resource_version:
                    resource_version = str(metadata.resource_version)
                token = metadata._continue if metadata else None
                if not token:
                    break
        except ApiException as exc:
            raise KubeError(
                f"读取 EndpointSlice {namespace}/{service} 失败: "
                f"{exc.status} {_status_message(exc)}"
            ) from exc
        except HttpTransportError as exc:
            raise KubeError(f"无法连接 API Server：{exc}") from exc
        return items, resource_version

    def watch_endpoint_slices(
        self, namespace: str, service: str, resource_version: str
    ) -> WatchStream:
        """Open a watch on the service's slices from a known resourceVersion.

        ``WatchUnavailable`` means no stream was established at all (HTTP
        error, unreachable server).  The caller marks its view stale and
        retries.  A ``410`` that arrives *after* the stream opened is surfaced
        by ``WatchStream``'s caller as ``ResourceExpired``.
        """
        selector = f"kubernetes.io/service-name={service}"
        try:
            response = self.discovery.list_namespaced_endpoint_slice(
                namespace,
                label_selector=selector,
                resource_version=resource_version or None,
                watch=True,
                allow_watch_bookmarks=True,
                _preload_content=False,
            )
        except ApiException as exc:
            if exc.status == 410:
                raise ResourceExpired(
                    f"EndpointSlice 版本 {resource_version} 已过期"
                ) from exc
            raise WatchUnavailable(
                f"订阅 EndpointSlice 失败: {exc.status} {_status_message(exc)}"
            ) from exc
        except HttpTransportError as exc:
            raise WatchUnavailable(f"无法连接 API Server：{exc}") from exc
        return WatchStream(response)
