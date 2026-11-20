"""NodePort local-backend inspection.

When a NodePort does not answer from a particular node, the question is
narrow: *which backends would this node's kube-proxy send the connection to?*
Only ``externalTrafficPolicy: Local`` IPv4 TCP services have an answer that
is specific to a node.  With ``Cluster`` policy a node happily forwards to a
pod on another node, so "local backend" is not a meaningful notion and the
service is rejected instead of showing a misleading answer.

The answer itself lives in the service's EndpointSlices.  This module keeps
one list+watch subscription per ``(namespace, service)`` and shares it among
every page looking at that service:

* the first page performs a list, remembers the list's ``resourceVersion``
  and opens a watch from that exact version, so no change can fall between
  snapshot and stream;
* a second page simply attaches to the same subscription; closing one page
  only drops its reference, the stream (and the other page) keeps updating;
* a broken watch marks the retained snapshot as ``待同步`` (stale) and the
  worker keeps retrying;
* a ``410 ResourceExpired`` (either as an HTTP response when opening, or as
  an ``ERROR`` event mid-stream) discards the snapshot in favour of a fresh
  full list, then resumes watching from the new list's version.  A slice
  deleted while the stream was down therefore cannot survive the relist.
"""

from __future__ import annotations

import ipaddress
import secrets
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .kube import KubeClient, KubeError, ResourceExpired, WatchUnavailable
from .models import NodeInfo

# How long a snapshot may be labelled "live" without a bookmark/message is
# not tracked: Kubernetes watches are silent while nothing changes.  The
# status only changes when the stream actually ends.
STATUS_LIVE = "live"
STATUS_STALE = "stale"


class NodePortError(ValueError):
    """The requested service/NodePort is outside what this console supports."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_ipv4(text: str) -> bool:
    try:
        return ipaddress.ip_address(text).version == 4
    except ValueError:
        return False


@dataclass(frozen=True)
class ServiceEntry:
    """The service/port pair the operator asked about, after validation."""

    namespace: str
    name: str
    node_port: int
    port_name: str
    service_port: int | None
    target_port: int | str | None
    policy: str = "Local"
    address_type: str = "IPv4"

    @property
    def key(self) -> str:
        return f"{self.namespace}/{self.name}"


def describe_service(service, node_port: int) -> ServiceEntry:
    """Validate the strict subset the console supports and locate the port.

    Raises ``NodePortError`` with an operator-readable reason for anything
    else (wrong type, Cluster policy, non-IPv4, wrong protocol, unknown
    NodePort).
    """
    if service is None:
        raise NodePortError("找不到该 Service")
    meta = service.metadata
    namespace = meta.namespace or ""
    name = meta.name or ""
    spec = service.spec
    if spec is None:
        raise NodePortError("Service 缺少 spec")
    kind = getattr(spec, "type", None) or "ClusterIP"
    if kind != "NodePort":
        raise NodePortError(f"只支持 type=NodePort 的 Service（当前为 {kind}）")
    policy = getattr(spec, "external_traffic_policy", None) or "Cluster"
    if policy != "Local":
        raise NodePortError(
            "只支持 externalTrafficPolicy=Local 的 IPv4 TCP 入口"
            f"（当前为 {policy}）"
        )

    families = list(getattr(spec, "ip_families", None) or [])
    cluster_ip = getattr(spec, "cluster_ip", "") or ""
    if families and families != ["IPv4"]:
        raise NodePortError(f"只支持 IPv4（Service ipFamilies={','.join(families)}）")
    if cluster_ip and ":" in cluster_ip:
        raise NodePortError(f"只支持 IPv4（Service clusterIP={cluster_ip}）")

    try:
        wanted = int(node_port)
    except (TypeError, ValueError):
        raise NodePortError("NodePort 必须是数字") from None

    for port in getattr(spec, "ports", None) or []:
        if getattr(port, "node_port", None) != wanted:
            continue
        protocol = getattr(port, "protocol", None) or "TCP"
        if protocol != "TCP":
            raise NodePortError(
                f"NodePort {wanted} 的协议是 {protocol}，只支持 TCP"
            )
        return ServiceEntry(
            namespace=namespace,
            name=name,
            node_port=wanted,
            port_name=getattr(port, "name", None) or "",
            service_port=getattr(port, "port", None),
            target_port=getattr(port, "target_port", None),
        )
    raise NodePortError(f"Service {namespace}/{name} 未暴露 NodePort {wanted}")


def resolve_port(slice_ports: list[dict], entry: ServiceEntry) -> int | None:
    """The real port an endpoint serves, taken from the EndpointSlice.

    EndpointSlice ports describe the *target* port (what the pod listens on),
    which is the port the NodePort ultimately forwards to.  Ports are matched
    by the Service port name; a single unnamed port is unambiguous.
    """
    ports = slice_ports or []
    if entry.port_name:
        for port in ports:
            if port.get("name") == entry.port_name and port.get("port") is not None:
                return int(port["port"])
    if len(ports) == 1 and ports[0].get("port") is not None:
        return int(ports[0]["port"])
    if isinstance(entry.target_port, int):
        for port in ports:
            if port.get("port") == entry.target_port:
                return entry.target_port
    return None


@dataclass
class Backend:
    address: str
    port: int | None
    node: str
    pod: str
    ready: bool
    serving: bool
    terminating: bool
    slice: str

    @property
    def endpoint(self) -> str:
        return f"{self.address}:{self.port}" if self.port is not None else self.address

    def to_dict(self) -> dict:
        return {
            "address": self.address,
            "port": self.port,
            "endpoint": self.endpoint,
            "node": self.node,
            "pod": self.pod,
            "ready": self.ready,
            "serving": self.serving,
            "terminating": self.terminating,
            "slice": self.slice,
        }


@dataclass
class NodeView:
    """What one node would use for the NodePort.

    ``mode`` is ``ready`` when at least one endpoint is ready (then only
    ready endpoints are listed), ``fallback`` when none is ready but
    terminating endpoints are still serving (listed as the fallback), and
    ``none`` when neither exists.
    """

    node: str
    mode: str
    backends: list[Backend] = field(default_factory=list)

    @property
    def label(self) -> str:
        return {
            "ready": "就绪后端",
            "fallback": "备用（终止中）",
            "none": "无可用本地后端",
        }[self.mode]

    @property
    def tone(self) -> str:
        return {"ready": "ok", "fallback": "warn", "none": "danger"}[self.mode]

    def to_dict(self) -> dict:
        return {
            "name": self.node,
            "mode": self.mode,
            "label": self.label,
            "backends": [backend.to_dict() for backend in self.backends],
        }


def _endpoint_conditions(endpoint: dict) -> tuple[bool, bool, bool]:
    """Interpret EndpointConditions the way Kubernetes does.

    * ``ready`` unset means "unknown", treated as ready; a terminating
      endpoint may never be considered ready.
    * ``serving`` unset falls back to ``ready`` for a non-terminating endpoint
      (older API servers did not send the field).  For a terminating endpoint
      an unset ``serving`` means "unknown" and is treated as not serving:
      claiming a draining pod still answers is the unsafe direction.
    * ``terminating`` unset means false.
    """
    conditions = endpoint.get("conditions") or {}
    terminating = bool(conditions.get("terminating"))
    raw_ready = conditions.get("ready", True) is not False
    raw_serving = conditions.get("serving")
    if raw_serving is not None:
        serving = bool(raw_serving)
    else:
        serving = raw_ready and not terminating
    ready = raw_ready and not terminating
    return ready, serving, terminating


def build_node_views(
    slices: list[dict],
    entry: ServiceEntry,
    node_names: list[str],
) -> list[NodeView]:
    """Group a service's endpoints by node and apply the ready/fallback rule."""
    by_node: dict[str, list[Backend]] = {name: [] for name in node_names}
    seen: set[tuple[str, str, int | None]] = set()
    for item in slices:
        if (item.get("addressType") or "IPv4") != "IPv4":
            continue  # the console is IPv4-only by design
        metadata = item.get("metadata") or {}
        slice_name = metadata.get("name") or ""
        port = resolve_port(item.get("ports") or [], entry)
        for endpoint in item.get("endpoints") or []:
            node = endpoint.get("nodeName") or ""
            if not node:
                continue
            ready, serving, terminating = _endpoint_conditions(endpoint)
            target = endpoint.get("targetRef") or {}
            pod = ""
            if target.get("kind") == "Pod" and target.get("name"):
                pod = f"{target.get('namespace') or entry.namespace}/{target['name']}"
            for address in endpoint.get("addresses") or []:
                if not _is_ipv4(address):
                    continue
                identity = (node, address, port)
                if identity in seen:
                    continue
                seen.add(identity)
                by_node.setdefault(node, []).append(
                    Backend(
                        address=address,
                        port=port,
                        node=node,
                        pod=pod,
                        ready=ready,
                        serving=serving,
                        terminating=terminating,
                        slice=slice_name,
                    )
                )

    views: list[NodeView] = []
    for node in sorted(by_node):
        backends = by_node[node]
        ready = [b for b in backends if b.ready]
        if ready:
            chosen, mode = ready, "ready"
        else:
            fallback = [b for b in backends if b.terminating and b.serving]
            if fallback:
                chosen, mode = fallback, "fallback"
            else:
                chosen, mode = [], "none"
        chosen.sort(key=lambda b: (b.address, b.port or 0))
        views.append(NodeView(node=node, mode=mode, backends=chosen))
    return views


def snapshot_is_stale(status: str) -> bool:
    return status != STATUS_LIVE


class SliceSubscription:
    """One list+watch stream for one service, shared by its observers.

    All mutable state is guarded by ``_lock``.  ``revision`` increases on
    every accepted change (including a status transition) so readers can tell
    whether they need to re-render.
    """

    def __init__(
        self,
        kube: KubeClient,
        namespace: str,
        service: str,
        initial_list_timeout: float = 8.0,
    ):
        self.kube = kube
        self.namespace = namespace
        self.service = service
        self.initial_list_timeout = initial_list_timeout
        self._lock = threading.RLock()
        self._slices: dict[str, dict] = {}
        self.resource_version = ""
        self.status = STATUS_STALE
        self.last_error = "尚未建立订阅。"
        self.revision = 0
        self.created_at = _now()
        self.updated_at = self.created_at
        self.observers = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._stream = None
        self._loaded = False

    @property
    def key(self) -> tuple[str, str]:
        return (self.namespace, self.service)

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    # -- snapshot access ---------------------------------------------------
    def slices(self) -> list[dict]:
        with self._lock:
            return [dict(item) for item in self._slices.values()]

    def state(self) -> dict:
        with self._lock:
            return {
                "status": self.status,
                "stale": snapshot_is_stale(self.status),
                "last_error": self.last_error,
                "revision": self.revision,
                "resource_version": self.resource_version,
                "observers": self.observers,
                "slice_count": len(self._slices),
                "updated_at": self.updated_at,
            }

    def _touch(self, status: str | None = None, error: str | None = None) -> None:
        """Bump the revision.  Caller holds the lock."""
        if status is not None:
            self.status = status
        if error is not None:
            self.last_error = error
        self.revision += 1
        self.updated_at = _now()

    def _replace(self, items: list[dict], resource_version: str) -> None:
        with self._lock:
            self._slices = {
                self._slice_key(item): item for item in items if self._belongs(item)
            }
            if resource_version:
                self.resource_version = resource_version
            self._loaded = True
            self._touch(STATUS_LIVE, "")

    @staticmethod
    def _slice_key(item: dict) -> str:
        metadata = item.get("metadata") or {}
        return str(metadata.get("uid") or metadata.get("name") or "")

    def _belongs(self, item: dict) -> bool:
        metadata = item.get("metadata") or {}
        labels = metadata.get("labels") or {}
        # Both halves of the subscription's scope must match: a watch is
        # namespace-scoped and label-scoped, and another namespace can host a
        # service with the same name.
        return (
            labels.get("kubernetes.io/service-name") == self.service
            and metadata.get("namespace") == self.namespace
        )

    def _apply(self, event_type: str, item: dict) -> bool:
        """Apply one watch event.  Returns True when the revision changed."""
        if not isinstance(item, dict) or not self._belongs(item):
            return False
        metadata = item.get("metadata") or {}
        key = self._slice_key(item)
        if not key:
            return False
        with self._lock:
            if event_type == "DELETED":
                if key not in self._slices:
                    return False
                del self._slices[key]
            else:
                if event_type not in ("ADDED", "MODIFIED"):
                    return False
                previous = self._slices.get(key)
                self._slices[key] = item
                if previous == item:
                    # A bookmarked or replayed event with no change: keep the
                    # resourceVersion fresh but do not force a re-render.
                    if metadata.get("resourceVersion"):
                        self.resource_version = str(metadata["resourceVersion"])
                    return False
            if metadata.get("resourceVersion"):
                self.resource_version = str(metadata["resourceVersion"])
            self._touch()
            return True

    # -- worker ------------------------------------------------------------
    def start(self) -> None:
        """Load the initial snapshot, then keep the watch alive."""
        try:
            items, resource_version = self.kube.list_endpoint_slices(
                self.namespace, self.service, timeout=self.initial_list_timeout
            )
        except KubeError as exc:
            with self._lock:
                self._touch(STATUS_STALE, f"读取 EndpointSlice 失败：{exc}")
            items, resource_version = None, ""
        if items is not None:
            self._replace(items, resource_version)
        self._thread = threading.Thread(
            target=self._run, name=f"nodeport-watch-{self.namespace}-{self.service}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._close_stream()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3)

    def _close_stream(self) -> None:
        with self._lock:
            stream, self._stream = self._stream, None
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass

    def _run(self) -> None:
        backoff = 0.2
        need_list = not self._loaded
        while not self._stop.is_set():
            try:
                if need_list or not self.resource_version:
                    items, resource_version = self.kube.list_endpoint_slices(
                        self.namespace, self.service
                    )
                    if self._stop.is_set():
                        return
                    self._replace(items, resource_version)
                    need_list = False
                    backoff = 0.2
                stream = self.kube.watch_endpoint_slices(
                    self.namespace, self.service, self.resource_version
                )
                with self._lock:
                    self._stream = stream
                    self._touch(STATUS_LIVE, "")
                # A stream was accepted, so the next drop should reconnect
                # promptly rather than inherit the previous failure's delay.
                backoff = 0.2
                expired = False
                unexpected = ""
                try:
                    for event in stream:
                        if self._stop.is_set():
                            return
                        event_type = str(event.get("type") or "").upper()
                        if event_type == "ERROR":
                            obj = event.get("object") or event.get("raw_object") or {}
                            code = int(obj.get("code") or 0)
                            if code == 410:
                                expired = True
                            else:
                                unexpected = str(
                                    obj.get("message")
                                    or obj.get("reason")
                                    or "watch 返回错误"
                                )
                            break
                        if event_type == "BOOKMARK":
                            continue
                        self._apply(event_type, event.get("object") or {})
                finally:
                    self._close_stream()
                if self._stop.is_set():
                    return
                if expired:
                    # The server no longer retains our version.  The next
                    # iteration performs a full list and replaces everything,
                    # which is what makes deleted slices disappear.
                    with self._lock:
                        self._touch(STATUS_STALE, "版本已过期，正在用完整列表重新同步。")
                    need_list = True
                    continue
                if unexpected:
                    raise KubeError(unexpected)
                with self._lock:
                    self._touch(STATUS_STALE, "订阅连接已断开，等待重新同步。")
            except ResourceExpired:
                with self._lock:
                    self._touch(STATUS_STALE, "版本已过期，正在用完整列表重新同步。")
                need_list = True
                continue
            except WatchUnavailable as exc:
                with self._lock:
                    self._touch(STATUS_STALE, f"订阅不可用，正在重试：{exc}")
            except KubeError as exc:
                need_list = True
                with self._lock:
                    self._touch(STATUS_STALE, f"同步失败，正在重试：{exc}")
            except Exception as exc:  # pragma: no cover - defensive
                with self._lock:
                    self._touch(STATUS_STALE, f"订阅异常，正在重试：{exc}")
            if self._stop.wait(backoff):
                return
            backoff = min(backoff * 2, 2.0)


class NodePortHub:
    """Owns the shared subscriptions and the small node-name cache.

    Browsers are not trusted to keep a subscription alive by themselves: a
    page that is closed, crashed or simply stops polling must not leak a
    watch forever.  Each page therefore holds a *session* (a token).  Sessions
    count as references to the subscription; a background sweeper drops
    sessions that have not been refreshed within ``session_ttl`` and stops a
    subscription once its last session is gone.  Crucially the sweeper never
    stops a subscription that still has any live session, so closing one of
    two pages leaves the other one updating.
    """

    def __init__(
        self,
        kube: KubeClient,
        *,
        session_ttl: float = 120.0,
        sweep_interval: float = 30.0,
    ):
        self.kube = kube
        self.session_ttl = session_ttl
        self.sweep_interval = sweep_interval
        self._lock = threading.RLock()
        self._subscriptions: dict[tuple[str, str], SliceSubscription] = {}
        self._sessions: dict[str, tuple[tuple[str, str], float]] = {}
        self._node_names: list[str] = []
        self._nodes_at = 0.0
        self._sweeper: threading.Thread | None = None
        self._sweep_stop = threading.Event()

    # -- node roster -------------------------------------------------------
    def node_names(self, ttl: float = 5.0) -> list[str]:
        with self._lock:
            stale = (time.monotonic() - self._nodes_at) > ttl
            cached = list(self._node_names)
        if not stale:
            return cached
        try:
            nodes: list[NodeInfo] = self.kube.list_nodes()
            names = sorted(node.name for node in nodes)
            with self._lock:
                self._node_names = names
                self._nodes_at = time.monotonic()
            return names
        except Exception:
            # The roster is a convenience for showing "no backend" per node;
            # a transient API failure must not break the page.
            return cached

    # -- sessions ----------------------------------------------------------
    def open_session(self, namespace: str, service: str) -> tuple[str, SliceSubscription]:
        """Attach a fresh session (the first one opens the subscription)."""
        key = (namespace, service)
        with self._lock:
            subscription = self._subscriptions.get(key)
            if subscription is None or subscription.stopped:
                # A subscription whose last session was reaped is no longer
                # being watched; start a fresh one rather than attaching to a
                # dead worker.
                if subscription is not None:
                    subscription.stop()
                subscription = SliceSubscription(self.kube, namespace, service)
                self._subscriptions[key] = subscription
                subscription.observers = 0
                subscription.start()
            subscription.observers += 1
            session = secrets.token_urlsafe(18)
            self._sessions[session] = (key, time.monotonic())
            self._ensure_sweeper()
            return session, subscription

    def touch_session(
        self, session: str, namespace: str, service: str
    ) -> SliceSubscription | None:
        """Refresh a session on every poll; returns its subscription.

        A session only counts for the service it was opened for: passing it
        with another namespace/service is treated as missing, so slices can
        never be attributed to the wrong Service.
        """
        now = time.monotonic()
        with self._lock:
            entry = self._sessions.get(session)
            if entry is None:
                return None
            key, _ = entry
            if key != (namespace, service):
                return None
            self._sessions[session] = (key, now)
            return self._subscriptions.get(key)

    def close_session(self, session: str) -> None:
        with self._lock:
            entry = self._sessions.pop(session, None)
            if entry is None:
                return
            self._release_locked(entry[0])

    def _release_locked(self, key: tuple[str, str]) -> None:
        subscription = self._subscriptions.get(key)
        if subscription is None:
            return
        subscription.observers = max(0, subscription.observers - 1)
        if subscription.observers > 0:
            return
        # Re-check: a session for this key may have been opened concurrently.
        if any(existing == key for existing, _ in self._sessions.values()):
            return
        self._subscriptions.pop(key, None)
        subscription.stop()

    # -- idle reaping ------------------------------------------------------
    def _ensure_sweeper(self) -> None:
        if self._sweeper is not None and self._sweeper.is_alive():
            return
        self._sweep_stop.clear()
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name="nodeport-session-sweeper", daemon=True
        )
        self._sweeper.start()

    def _sweep_loop(self) -> None:
        while not self._sweep_stop.wait(self.sweep_interval):
            self.sweep()

    def sweep(self) -> None:
        now = time.monotonic()
        with self._lock:
            expired = [
                session
                for session, (_, seen) in self._sessions.items()
                if now - seen > self.session_ttl
            ]
            keys = set()
            for session in expired:
                entry = self._sessions.pop(session, None)
                if entry is not None:
                    keys.add(entry[0])
            for key in keys:
                self._release_locked(key)

    def stop(self) -> None:
        self._sweep_stop.set()
        with self._lock:
            subscriptions = list(self._subscriptions.values())
            self._subscriptions.clear()
            self._sessions.clear()
        for subscription in subscriptions:
            subscription.stop()

    def _active_subscriptions(self) -> list[SliceSubscription]:
        with self._lock:
            return list(self._subscriptions.values())

    # -- introspection -----------------------------------------------------
    def describe(self) -> list[dict]:
        described = []
        for subscription in self._active_subscriptions():
            entry = subscription.state()
            entry.update(
                {
                    "namespace": subscription.namespace,
                    "service": subscription.service,
                    "key": f"{subscription.namespace}/{subscription.service}",
                }
            )
            described.append(entry)
        return sorted(described, key=lambda item: item["key"])
