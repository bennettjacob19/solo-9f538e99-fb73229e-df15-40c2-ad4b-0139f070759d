"""A small, stateful Kubernetes API look-alike for rehearsing node drains.

It is not a Kubernetes distribution.  It implements only the handful of
endpoints the drain UI uses, but it implements their *semantics* faithfully
where the exercise depends on them:

* ``PATCH /api/v1/nodes/{name}`` for cordoning (merge patch).
* ``GET /api/v1/pods?fieldSelector=spec.nodeName=...`` for listing.
* ``POST /api/v1/namespaces/{ns}/pods/{name}/eviction`` implementing the
  ``policy/v1`` Eviction subresource, including the UID precondition.
* ``GET /apis/policy/v1/poddisruptionbudgets`` with live budget accounting.

Eviction decisions are made under one lock against freshly computed budget
status, so two nodes drained concurrently cannot jointly exceed a shared
PodDisruptionBudget.

The ``/api/sim`` endpoints and the page at ``/`` let an operator change a
budget, replace a pod under the same name (new UID), make a replacement ready,
or let an evicted pod finish terminating.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

NODE_NAMES = ["node-a", "node-b", "node-c"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_uid() -> str:
    return uuid.uuid4().hex


@dataclass
class SimPod:
    name: str
    namespace: str
    node: str
    uid: str = field(default_factory=_new_uid)
    labels: dict[str, str] = field(default_factory=dict)
    owner_kind: str | None = None
    static: bool = False
    ready: bool = True
    phase: str = "Running"
    terminating: bool = False
    deletion_timestamp: str | None = None
    resource_version: int = 1

    @property
    def key(self) -> tuple[str, str]:
        return (self.namespace, self.name)

    @property
    def healthy(self) -> bool:
        return self.phase == "Running" and self.ready and not self.terminating


@dataclass
class SimPDB:
    name: str
    namespace: str
    selector: dict[str, str]
    min_available: int | None = None
    max_unavailable: int | None = None
    generation: int = 1
    resource_version: int = 1

    def budget(self, pods: list[SimPod]) -> dict[str, int]:
        matching = [
            pod
            for pod in pods
            if pod.namespace == self.namespace
            and all(pod.labels.get(k) == v for k, v in self.selector.items())
        ]
        expected = len(matching)
        healthy = sum(1 for pod in matching if pod.healthy)
        if self.min_available is not None:
            desired = self.min_available
        else:
            desired = max(0, expected - (self.max_unavailable or 0))
        return {
            "disruptionsAllowed": max(0, healthy - desired),
            "currentHealthy": healthy,
            "desiredHealthy": desired,
            "expectedPods": expected,
        }


@dataclass
class SimNode:
    name: str
    unschedulable: bool = False
    ready: bool = True
    uid: str = field(default_factory=_new_uid)
    resource_version: int = 1


@dataclass
class SimServicePort:
    port: int
    node_port: int
    target_port: int
    name: str = "http"
    protocol: str = "TCP"


@dataclass
class SimService:
    name: str
    namespace: str
    ports: list[SimServicePort]
    type: str = "NodePort"
    external_traffic_policy: str = "Local"
    selector: dict[str, str] = field(default_factory=dict)
    ip_families: list[str] = field(default_factory=lambda: ["IPv4"])
    uid: str = field(default_factory=_new_uid)
    resource_version: int = 1


@dataclass
class SimEndpoint:
    """One address in one slice, tied to the pod behind it."""

    address: str
    pod: str
    namespace: str
    node: str
    ready: bool = True
    serving: bool = True
    terminating: bool = False


@dataclass
class SimSlice:
    """A hand-managed EndpointSlice.

    In a real cluster the endpoint-controller owns these.  The simulator
    keeps them explicit so a rehearsal can drive every interesting state:
    ready, terminating-but-serving, an unready pod, a slice that disappears.
    """

    name: str
    namespace: str
    service: str
    address_type: str = "IPv4"
    endpoints: list[SimEndpoint] = field(default_factory=list)
    port: int = 80
    port_name: str = "http"
    uid: str = field(default_factory=_new_uid)
    resource_version: int = 1

    @property
    def key(self) -> tuple[str, str]:
        return (self.namespace, self.name)

    def label(self) -> str:
        return f"{self.service}:{self.port_name}/{self.port}"


class ClusterState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.nodes: dict[str, SimNode] = {}
        self.pods: dict[tuple[str, str], SimPod] = {}
        self.pdbs: dict[tuple[str, str], SimPDB] = {}
        self.services: dict[tuple[str, str], SimService] = {}
        self.slices: dict[tuple[str, str], SimSlice] = {}
        # Watch bookkeeping: a monotonically increasing resourceVersion, the
        # history kept so an old watch can be resumed, and the "sync" gate the
        # console is asked to perform when an old version is expired.
        self.revision = 1
        self.history: list[tuple[int, str, dict[str, Any]]] = []
        self.expired_before = 0
        self.watch_enabled = True
        self.watch_generation = 0
        self.events: list[dict[str, Any]] = []
        self.reset()

    # -- seed --------------------------------------------------------------
    def reset(self) -> None:
        with self.lock:
            self.nodes = {name: SimNode(name=name) for name in NODE_NAMES}
            self.pods = {}
            self.pdbs = {}
            self.services = {}
            self.slices = {}
            self.revision = 1
            self.history = []
            self.expired_before = 0
            self.watch_enabled = True
            self.watch_generation += 1
            self.events = []
            self.condition = threading.Condition(self.lock)

            def add(
                namespace: str,
                name: str,
                node: str,
                labels: dict[str, str],
                owner_kind: str | None = None,
                static: bool = False,
            ) -> None:
                pod = SimPod(
                    namespace=namespace,
                    name=name,
                    node=node,
                    labels=labels,
                    owner_kind=owner_kind,
                    static=static,
                )
                self.pods[pod.key] = pod

            # A ReplicaSet-like workload spread over all three nodes.
            for index, node in enumerate(["node-a", "node-a", "node-b", "node-b", "node-c"]):
                add("shop", f"web-{index}", node, {"app": "web"}, owner_kind="ReplicaSet")
            # A two-replica cache, one pod on each of the two target nodes.
            add("shop", "cache-0", "node-a", {"app": "cache"}, owner_kind="StatefulSet")
            add("shop", "cache-1", "node-b", {"app": "cache"}, owner_kind="StatefulSet")
            # DaemonSets mirror nodes and must never be evicted.
            for suffix, node in zip("abc", ["node-a", "node-b", "node-c"]):
                add(
                    "kube-system",
                    f"log-agent-{suffix}",
                    node,
                    {"app": "log-agent"},
                    owner_kind="DaemonSet",
                )
            # A static pod owned by the kubelet.
            add("kube-system", "kube-vip-a", "node-a", {"app": "kube-vip"}, static=True)

            self.pdbs[("shop", "web-pdb")] = SimPDB(
                name="web-pdb", namespace="shop", selector={"app": "web"}, min_available=3
            )
            self.pdbs[("shop", "cache-pdb")] = SimPDB(
                name="cache-pdb", namespace="shop", selector={"app": "cache"}, min_available=1
            )

            # Two NodePort services for the local-backend console.
            #
            # shop/web (30080) is the healthy case: every node has at least
            # one ready endpoint, so each node lists exactly its ready ones.
            self.services[("shop", "web")] = SimService(
                name="web",
                namespace="shop",
                ports=[SimServicePort(port=80, node_port=30080, target_port=8080)],
                selector={"app": "web"},
            )
            self._seed_slice(
                "web-node-a",
                "web",
                [
                    ("10.244.1.11", "web-0", "node-a", True, True, False),
                    ("10.244.1.12", "web-1", "node-a", True, True, False),
                ],
            )
            self._seed_slice(
                "web-node-b",
                "web",
                [
                    ("10.244.2.21", "web-2", "node-b", True, True, False),
                    ("10.244.2.22", "web-3", "node-b", True, True, False),
                ],
            )
            self._seed_slice(
                "web-node-c",
                "web",
                [("10.244.3.31", "web-4", "node-c", True, True, False)],
            )

            # shop/api (30081) starts in the awkward states on purpose:
            # node-a only has a terminating-but-serving backend (fallback),
            # node-b has none at all, and node-c's endpoint is neither ready
            # nor terminating (no usable local backend).
            self.services[("shop", "api")] = SimService(
                name="api",
                namespace="shop",
                ports=[
                    SimServicePort(
                        port=8081, node_port=30081, target_port=9090, name="http"
                    )
                ],
                selector={"app": "api"},
            )
            self._seed_slice(
                "api-node-a",
                "api",
                [
                    ("10.244.1.40", "cache-0", "node-a", False, True, True),
                ],
                port=9090,
            )
            self._seed_slice(
                "api-node-c",
                "api",
                [
                    ("10.244.3.50", "web-4", "node-c", False, False, False),
                ],
                port=9090,
            )

            # A Cluster-policy service exists so the console can show that it
            # refuses anything but Local.
            self.services[("shop", "metrics")] = SimService(
                name="metrics",
                namespace="shop",
                ports=[
                    SimServicePort(
                        port=9091, node_port=30082, target_port=9091, name="metrics"
                    )
                ],
                external_traffic_policy="Cluster",
                selector={"app": "metrics"},
            )
            self._event("reset", "演练环境已恢复到初始状态")

    def _seed_slice(
        self,
        name: str,
        service: str,
        endpoints: list[tuple[str, str, str, bool, bool, bool]],
        port: int = 8080,
    ) -> None:
        """Add a slice directly during reset (no history, no event)."""
        slice_ = SimSlice(
            name=name,
            namespace="shop",
            service=service,
            port=port,
            endpoints=[
                SimEndpoint(
                    address=address,
                    pod=pod,
                    namespace="shop",
                    node=node,
                    ready=ready,
                    serving=serving,
                    terminating=terminating,
                )
                for address, pod, node, ready, serving, terminating in endpoints
            ],
        )
        self.slices[slice_.key] = slice_

    def _event(self, kind: str, message: str, **extra: Any) -> None:
        self.events.insert(
            0,
            {"at": _now_iso(), "kind": kind, "message": message, **extra},
        )
        del self.events[200:]

    # -- EndpointSlice watch bookkeeping ----------------------------------
    #
    # The simulator mimics the API server's list+watch contract closely
    # enough to rehearse the hard cases:
    #
    # * every accepted slice change gets a new, increasing resourceVersion;
    # * the list returns the current version *and* records it in history;
    # * a watch from that version replays everything after it, so no change
    #   between list and watch can be lost;
    # * a watch from a version older than ``expired_before`` is refused with
    #   410, which forces the console to relist — exactly what makes a slice
    #   deleted during the outage disappear;
    # * ``watch_enabled=False`` makes the initial watch request fail, which
    #   rehearses "connection down": the console must mark its snapshot
    #   stale and keep it until the subscription is restored.
    def bump_version(self) -> int:
        self.revision += 1
        return self.revision

    def publish(self, event_type: str, slice_: SimSlice) -> int:
        """Record one slice change and wake every waiting watch."""
        with self.lock:
            version = self.bump_version()
            slice_.resource_version = version
            payload = self.slice_json(slice_) if event_type != "DELETED" else {
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "name": slice_.name,
                    "namespace": slice_.namespace,
                    "uid": slice_.uid,
                    "resourceVersion": str(version),
                    "labels": {"kubernetes.io/service-name": slice_.service},
                },
                "addressType": slice_.address_type,
                "endpoints": [],
                "ports": [],
            }
            self.history.append((version, event_type, payload))
            del self.history[:-500]
            self.condition.notify_all()
            return version

    def expire_older_than_current(self) -> int:
        """Discard watch history; older resourceVersions now yield 410.

        A watch from the *current* version stays valid (there are simply no
        changes to replay yet), which mirrors a real API server: expiring the
        history does not invalidate a version the caller just listed.
        """
        with self.lock:
            self.expired_before = self.revision
            self.history = []
            self.condition.notify_all()
            return self.expired_before

    def touch_slices(self) -> None:
        self.condition.notify_all()

    def _history_from(self, resource_version: int) -> list[tuple[int, str, dict[str, Any]]]:
        return [item for item in self.history if item[0] > resource_version]

    def slice_json(self, slice_: SimSlice) -> dict[str, Any]:
        return {
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {
                "name": slice_.name,
                "namespace": slice_.namespace,
                "uid": slice_.uid,
                "resourceVersion": str(slice_.resource_version),
                "creationTimestamp": "2026-01-01T00:00:00Z",
                "labels": {
                    "kubernetes.io/service-name": slice_.service,
                    "endpointslice.kubernetes.io/managed-by": "simulator",
                },
            },
            "addressType": slice_.address_type,
            "endpoints": [
                {
                    "addresses": [endpoint.address],
                    "conditions": {
                        "ready": endpoint.ready,
                        "serving": endpoint.serving,
                        "terminating": endpoint.terminating,
                    },
                    "nodeName": endpoint.node,
                    "targetRef": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": endpoint.pod,
                        "namespace": endpoint.namespace,
                    },
                }
                for endpoint in slice_.endpoints
            ],
            "ports": [
                {
                    "name": slice_.port_name,
                    "port": slice_.port,
                    "protocol": "TCP",
                }
            ],
        }

    def service_json(self, service: SimService) -> dict[str, Any]:
        ports = []
        for port in service.ports:
            ports.append(
                {
                    "name": port.name,
                    "port": port.port,
                    "targetPort": port.target_port,
                    "nodePort": port.node_port,
                    "protocol": port.protocol,
                }
            )
        return {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": service.name,
                "namespace": service.namespace,
                "uid": service.uid,
                "resourceVersion": str(service.resource_version),
                "creationTimestamp": "2026-01-01T00:00:00Z",
            },
            "spec": {
                "type": service.type,
                "selector": dict(service.selector),
                "externalTrafficPolicy": service.external_traffic_policy,
                "ipFamilies": list(service.ip_families),
                "clusterIP": "10.96.0.10",
                "ports": ports,
            },
        }

    # -- serialisation -----------------------------------------------------
    def node_json(self, node: SimNode) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": node.name,
                "uid": node.uid,
                "resourceVersion": str(node.resource_version),
                "creationTimestamp": "2026-01-01T00:00:00Z",
                "labels": {"kubernetes.io/hostname": node.name},
            },
            "spec": {"unschedulable": node.unschedulable},
            "status": {
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True" if node.ready else "False",
                        "reason": "KubeletReady" if node.ready else "KubeletNotReady",
                    }
                ],
                "capacity": {"cpu": "8", "memory": "32Gi", "pods": "110"},
            },
        }

    def pod_json(self, pod: SimPod) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "name": pod.name,
            "namespace": pod.namespace,
            "uid": pod.uid,
            "resourceVersion": str(pod.resource_version),
            "creationTimestamp": "2026-01-01T00:00:00Z",
            "labels": dict(pod.labels),
        }
        if pod.terminating and pod.deletion_timestamp:
            metadata["deletionTimestamp"] = pod.deletion_timestamp
            metadata["deletionGracePeriodSeconds"] = 30
        if pod.owner_kind:
            metadata["ownerReferences"] = [
                {
                    "apiVersion": "apps/v1",
                    "kind": pod.owner_kind,
                    "name": f"{pod.name.rsplit('-', 1)[0]}-owner",
                    "uid": _new_uid(),
                    "controller": True,
                }
            ]
        if pod.static:
            metadata["annotations"] = {
                "kubernetes.io/config.mirror": _new_uid(),
                "kubernetes.io/config.source": "file",
            }
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": metadata,
            "spec": {
                "nodeName": pod.node,
                "containers": [{"name": "app", "image": "registry.local/app:1.0"}],
            },
            "status": {
                "phase": pod.phase,
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True" if pod.healthy else "False",
                    }
                ],
                "containerStatuses": [
                    {
                        "name": "app",
                        "ready": pod.healthy,
                        "restartCount": 0,
                        "image": "registry.local/app:1.0",
                        "imageID": "registry.local/app@sha256:"
                        + ("0" * 64),
                        "containerID": f"containerd://{pod.uid[:16]}",
                        "started": True if pod.healthy else False,
                        "state": {"running": {}} if pod.healthy else {"waiting": {"reason": "ContainerCreating"}},
                    }
                ],
            },
        }

    def pdb_json(self, pdb: SimPDB) -> dict[str, Any]:
        status = pdb.budget(list(self.pods.values()))
        spec: dict[str, Any] = {"selector": {"matchLabels": dict(pdb.selector)}}
        if pdb.min_available is not None:
            spec["minAvailable"] = pdb.min_available
        else:
            spec["maxUnavailable"] = pdb.max_unavailable or 0
        return {
            "apiVersion": "policy/v1",
            "kind": "PodDisruptionBudget",
            "metadata": {
                "name": pdb.name,
                "namespace": pdb.namespace,
                "uid": f"pdb-{pdb.namespace}-{pdb.name}",
                "generation": pdb.generation,
                "resourceVersion": str(pdb.resource_version),
                "creationTimestamp": "2026-01-01T00:00:00Z",
            },
            "spec": spec,
            "status": {
                "observedGeneration": pdb.generation,
                **status,
                "disruptionsAllowed": status["disruptionsAllowed"],
            },
        }


STATE = ClusterState()
app = FastAPI(title="drain rehearsal simulator", docs_url=None, redoc_url=None)


# -- helpers ---------------------------------------------------------------
def status_response(code: int, reason: str, message: str) -> JSONResponse:
    kind = "Status"
    return JSONResponse(
        status_code=code,
        content={
            "apiVersion": "v1",
            "kind": kind,
            "status": "Failure" if code >= 400 else "Success",
            "message": message,
            "reason": reason,
            "code": code,
        },
        media_type="application/json",
    )


def _find_pod(namespace: str, name: str) -> SimPod | None:
    return STATE.pods.get((namespace, name))


def _field_selector_matches(pod: SimPod, selector: str) -> bool:
    if not selector:
        return True
    for term in selector.split(","):
        if "=" not in term:
            continue
        key, _, value = term.partition("=")
        key = key.strip()
        value = value.lstrip("=")
        if key == "spec.nodeName" and pod.node != value:
            return False
        if key == "metadata.namespace" and pod.namespace != value:
            return False
        if key == "metadata.name" and pod.name != value:
            return False
        if key == "status.phase" and pod.phase != value:
            return False
    return True


def _eviction_precondition_uid(body: dict[str, Any]) -> str | None:
    options = body.get("deleteOptions") or {}
    preconditions = options.get("preconditions") or {}
    return preconditions.get("uid")


def _label_selector_matches(labels: dict[str, str], selector: str) -> bool:
    if not selector:
        return True
    for term in selector.split(","):
        if "!=" in term:
            key, _, value = term.partition("!=")
            if labels.get(key.strip()) == value.strip():
                return False
        elif "=" in term:
            key, _, value = term.partition("=")
            if labels.get(key.strip()) != value.strip():
                return False
    return True


def _parse_version(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _slices_for(namespace: str, selector: str) -> list[SimSlice]:
    return [
        slice_
        for slice_ in STATE.slices.values()
        if slice_.namespace == namespace
        and _label_selector_matches(
            {"kubernetes.io/service-name": slice_.service}, selector
        )
    ]


def _watch_expired(resource_version: int | None) -> bool:
    """Mirror the API server's 410 ResourceExpired for a stale version.

    No resourceVersion means "start from now" and is always accepted.
    """
    if resource_version is None:
        return False
    return resource_version < STATE.expired_before or resource_version < (
        STATE.revision - 500
    )


def _watch_line(event_type: str, item: dict[str, Any]) -> str:
    # Kubernetes watch responses are newline-delimited JSON; clients read one
    # object per line, so the newline is part of the wire format.
    return json.dumps({"type": event_type, "object": item}, ensure_ascii=False) + "\n"


def _gone_response(resource_version: int) -> Response:
    return status_response(
        410,
        "Expired",
        f"too old resource version: {resource_version} ({STATE.revision})",
    )


def _stream_watch(
    namespace: str,
    selector: str,
    resource_version: int | None,
    generation: int,
) -> StreamingResponse:
    """Replay history, then block until new changes arrive.

    ``watch_enabled`` only gates *new* watch connections.  Turning it off
    while a stream is open closes that stream, which is exactly the
    "connection dropped" rehearsal: the client must mark its last snapshot
    stale instead of pretending it is live.

    Replayed and live events are filtered by namespace and label selector,
    matching the query the client subscribed with.
    """

    def matches(payload: dict[str, Any]) -> bool:
        metadata = payload.get("metadata") or {}
        labels = metadata.get("labels") or {}
        return (
            metadata.get("namespace") == namespace
            and _label_selector_matches(labels, selector)
        )

    def pending_events(cursor: int) -> list[tuple[int, str, dict[str, Any]]]:
        with STATE.lock:
            return [
                item
                for item in STATE._history_from(cursor)
                if item[1] != "BOOKMARK" and matches(item[2])
            ]

    def frames():
        cursor = resource_version or STATE.revision
        last_beat = time.monotonic()
        # The lock is held only while inspecting/waiting on state, never
        # across a yield: a slow reader must not stall the whole simulator.
        while True:
            with STATE.condition:
                if STATE.watch_generation != generation or not STATE.watch_enabled:
                    return
                pending = pending_events(cursor)
                # Advance past unrelated history too, so the cursor tracks
                # the server's version rather than stalling on other objects.
                all_new = STATE._history_from(cursor)
                if all_new:
                    cursor = max(cursor, max(v for v, _, _ in all_new))
            if pending:
                for version, event_type, payload in pending:
                    cursor = max(cursor, version)
                    yield _watch_line(event_type, payload)
                continue
            with STATE.condition:
                if STATE.watch_generation != generation or not STATE.watch_enabled:
                    return
                if not STATE._history_from(cursor):
                    STATE.condition.wait(timeout=0.5)
            now = time.monotonic()
            if now - last_beat >= 2.0 and not pending_events(cursor):
                last_beat = now
                with STATE.lock:
                    revision = STATE.revision
                yield _watch_line(
                    "BOOKMARK",
                    {
                        "apiVersion": "discovery.k8s.io/v1",
                        "kind": "EndpointSlice",
                        "metadata": {"resourceVersion": str(revision)},
                    },
                )

    return StreamingResponse(frames(), media_type="application/json")


def _endpoint_slice_list(
    items: list[dict[str, Any]], resource_version: int, token: str | None = None
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"resourceVersion": str(resource_version)}
    if token:
        metadata["continue"] = token
    return {
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSliceList",
        "metadata": metadata,
        "items": items,
    }


# -- Kubernetes API surface ------------------------------------------------
@app.get("/api/v1/nodes")
def list_nodes() -> dict[str, Any]:
    with STATE.lock:
        items = [STATE.node_json(node) for node in STATE.nodes.values()]
    return {
        "apiVersion": "v1",
        "kind": "NodeList",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


@app.get("/api/v1/nodes/{name}")
def read_node(name: str) -> Response:
    with STATE.lock:
        node = STATE.nodes.get(name)
        if node is None:
            return status_response(404, "NotFound", f'nodes "{name}" not found')
        return JSONResponse(STATE.node_json(node))


@app.patch("/api/v1/nodes/{name}")
async def patch_node(name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        node = STATE.nodes.get(name)
        if node is None:
            return status_response(404, "NotFound", f'nodes "{name}" not found')
        if isinstance(body, dict):
            desired = body.get("spec", {}).get("unschedulable")
            if desired is not None:
                node.unschedulable = bool(desired)
                node.resource_version += 1
                STATE._event(
                    "cordon" if node.unschedulable else "uncordon",
                    f"节点 {name} 已{'封锁' if node.unschedulable else '解除封锁'}",
                )
        return JSONResponse(STATE.node_json(node))


@app.get("/api/v1/pods")
def list_pods(fieldSelector: str | None = None, labelSelector: str | None = None) -> dict[str, Any]:
    with STATE.lock:
        pods = [
            STATE.pod_json(pod)
            for pod in STATE.pods.values()
            if _field_selector_matches(pod, fieldSelector or "")
        ]
    if labelSelector:
        # The drain UI does not use this, but accept the usual k=v form.
        required = {}
        for term in labelSelector.split(","):
            if "=" in term:
                key, _, value = term.partition("=")
                required[key.strip()] = value.strip()
        pods = [
            pod
            for pod in pods
            if all(pod["metadata"].get("labels", {}).get(k) == v for k, v in required.items())
        ]
    return {
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "1"},
        "items": pods,
    }


@app.get("/api/v1/namespaces/{namespace}/pods/{name}")
def read_pod(namespace: str, name: str) -> Response:
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(
                404, "NotFound", f'pods "{name}" not found in namespace "{namespace}"'
            )
        return JSONResponse(STATE.pod_json(pod))


@app.get("/api/v1/pods/{name}")
def read_pod_any_namespace(name: str) -> Response:
    with STATE.lock:
        matches = [pod for pod in STATE.pods.values() if pod.name == name]
        if not matches:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        return JSONResponse(STATE.pod_json(matches[0]))


@app.post("/api/v1/namespaces/{namespace}/pods/{name}/eviction")
async def evict_pod(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    precondition_uid = _eviction_precondition_uid(body)
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(
                404, "NotFound", f'pods "{name}" not found in namespace "{namespace}"'
            )
        if precondition_uid and precondition_uid != pod.uid:
            STATE._event(
                "eviction-rejected",
                f"{namespace}/{name} 的 UID 前置条件不匹配（请求 {precondition_uid[:8]}，当前 {pod.uid[:8]}）",
            )
            return status_response(
                409,
                "Conflict",
                f'Eviction of pod "{name}" rejected: UID precondition failed',
            )
        if pod.static:
            return status_response(
                500, "InternalError", f'cannot evict static pod "{namespace}/{name}"'
            )
        if pod.terminating:
            STATE._event("eviction-idempotent", f"{namespace}/{name} 已在退出中")
            return JSONResponse(
                {"apiVersion": "policy/v1", "kind": "Eviction", "metadata": {"name": name}},
                status_code=201,
            )
        matching_pdbs = [
            pdb
            for pdb in STATE.pdbs.values()
            if pdb.namespace == namespace
            and all(pod.labels.get(k) == v for k, v in pdb.selector.items())
        ]
        # Kubernetes counts the candidate pod as healthy when deciding; the
        # eviction is allowed while disruptionsAllowed is positive and the
        # status recomputes once the pod starts terminating.
        for pdb in matching_pdbs:
            allowed = pdb.budget(list(STATE.pods.values()))["disruptionsAllowed"]
            if allowed <= 0:
                STATE._event(
                    "eviction-blocked",
                    f"{namespace}/{name} 受 {pdb.namespace}/{pdb.name} 阻挡",
                )
                return status_response(
                    429,
                    "TooManyRequests",
                    "Cannot evict pod as it would violate the pod's disruption budget.",
                )
        pod.terminating = True
        pod.deletion_timestamp = _now_iso()
        pod.resource_version += 1
        STATE._event("eviction", f"{namespace}/{name} 获准驱逐")
        return JSONResponse(
            {
                "apiVersion": "policy/v1",
                "kind": "Eviction",
                "metadata": {"name": name, "namespace": namespace},
            },
            status_code=201,
        )


@app.get("/apis/policy/v1/poddisruptionbudgets")
def list_pdbs() -> dict[str, Any]:
    with STATE.lock:
        items = [STATE.pdb_json(pdb) for pdb in STATE.pdbs.values()]
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudgetList",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


@app.get("/apis/policy/v1/namespaces/{namespace}/poddisruptionbudgets")
def list_pdbs_in_namespace(namespace: str) -> dict[str, Any]:
    with STATE.lock:
        items = [
            STATE.pdb_json(pdb) for pdb in STATE.pdbs.values() if pdb.namespace == namespace
        ]
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudgetList",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


@app.get("/apis/policy/v1/namespaces/{namespace}/poddisruptionbudgets/{name}")
def read_pdb(namespace: str, name: str) -> Response:
    with STATE.lock:
        pdb = STATE.pdbs.get((namespace, name))
        if pdb is None:
            return status_response(404, "NotFound", f'poddisruptionbudgets "{name}" not found')
        return JSONResponse(STATE.pdb_json(pdb))


@app.get("/api/v1/namespaces/{namespace}/services/{name}")
def read_service(namespace: str, name: str) -> Response:
    with STATE.lock:
        service = STATE.services.get((namespace, name))
        if service is None:
            return status_response(
                404, "NotFound", f'services "{name}" not found in namespace "{namespace}"'
            )
        return JSONResponse(STATE.service_json(service))


@app.get("/apis/discovery.k8s.io/v1/namespaces/{namespace}/endpointslices")
def list_endpoint_slices(
    namespace: str,
    labelSelector: str | None = None,
    watch: bool | None = None,
    resourceVersion: str | None = None,
    allowWatchBookmarks: bool | None = None,
    limit: int | None = None,
    continue_: str | None = Query(default=None, alias="continue"),
):
    """List or watch the EndpointSlices of one namespace.

    ``labelSelector=kubernetes.io/service-name=<name>`` is how the console
    scopes a subscription to one Service.  Watch responses stream
    ``ADDED``/``MODIFIED``/``DELETED``/``BOOKMARK`` events and are refused
    with 410 when the requested version is no longer retained.
    """
    selector = labelSelector or ""
    version = _parse_version(resourceVersion)
    with STATE.lock:
        if watch:
            if not STATE.watch_enabled:
                return status_response(
                    503, "ServiceUnavailable", "watch endpoint rehearsed as down"
                )
            if _watch_expired(version):
                STATE._event(
                    "watch-expired",
                    f"watch 版本 {version} 已过期，返回 410 要求重新列表",
                )
                return _gone_response(version or 0)
            STATE._event(
                "watch-open",
                f"建立 EndpointSlice 订阅（from={version if version else 'now'}）",
            )
            return _stream_watch(
                namespace, selector, version, STATE.watch_generation
            )

        items = [
            STATE.slice_json(slice_) for slice_ in _slices_for(namespace, selector)
        ]
        # The list's resourceVersion is the point a subsequent watch resumes
        # from; recording it in history is what guarantees no event is lost.
        listed = STATE.revision
        STATE.history.append(
            (listed, "BOOKMARK", {"metadata": {"resourceVersion": str(listed)}})
        )
        del STATE.history[:-500]
        offset = 0
        if continue_:
            try:
                offset = int(continue_)
            except ValueError:
                offset = 0
        page = items[offset:] if limit is None else items[offset : offset + limit]
        next_token = ""
        if limit is not None and offset + limit < len(items):
            next_token = str(offset + limit)
        return JSONResponse(_endpoint_slice_list(page, listed, next_token or None))


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


# -- rehearsal controls ----------------------------------------------------
@app.get("/api/sim/state")
def sim_state() -> dict[str, Any]:
    with STATE.lock:
        pods = list(STATE.pods.values())
        return {
            "nodes": [
                {
                    "name": node.name,
                    "unschedulable": node.unschedulable,
                    "ready": node.ready,
                }
                for node in sorted(STATE.nodes.values(), key=lambda n: n.name)
            ],
            "pods": [
                {
                    "namespace": pod.namespace,
                    "name": pod.name,
                    "uid": pod.uid,
                    "node": pod.node,
                    "labels": pod.labels,
                    "owner_kind": pod.owner_kind,
                    "static": pod.static,
                    "ready": pod.ready,
                    "phase": pod.phase,
                    "terminating": pod.terminating,
                    "healthy": pod.healthy,
                }
                for pod in sorted(pods, key=lambda p: (p.namespace, p.name))
            ],
            "budgets": [
                {
                    "namespace": pdb.namespace,
                    "name": pdb.name,
                    "selector": pdb.selector,
                    "minAvailable": pdb.min_available,
                    "maxUnavailable": pdb.max_unavailable,
                    "generation": pdb.generation,
                    **pdb.budget(pods),
                }
                for pdb in sorted(STATE.pdbs.values(), key=lambda p: (p.namespace, p.name))
            ],
            "services": [
                {
                    "namespace": service.namespace,
                    "name": service.name,
                    "type": service.type,
                    "externalTrafficPolicy": service.external_traffic_policy,
                    "ports": [
                        {
                            "name": port.name,
                            "port": port.port,
                            "nodePort": port.node_port,
                            "targetPort": port.target_port,
                            "protocol": port.protocol,
                        }
                        for port in service.ports
                    ],
                }
                for service in sorted(
                    STATE.services.values(), key=lambda s: (s.namespace, s.name)
                )
            ],
            "slices": [
                {
                    "namespace": slice_.namespace,
                    "name": slice_.name,
                    "service": slice_.service,
                    "addressType": slice_.address_type,
                    "port": slice_.port,
                    "portName": slice_.port_name,
                    "resourceVersion": slice_.resource_version,
                    "endpoints": [
                        {
                            "address": endpoint.address,
                            "pod": endpoint.pod,
                            "node": endpoint.node,
                            "ready": endpoint.ready,
                            "serving": endpoint.serving,
                            "terminating": endpoint.terminating,
                        }
                        for endpoint in slice_.endpoints
                    ],
                }
                for slice_ in sorted(
                    STATE.slices.values(), key=lambda s: (s.namespace, s.name)
                )
            ],
            "watch": {
                "enabled": STATE.watch_enabled,
                "resourceVersion": STATE.revision,
                "expiredBefore": STATE.expired_before,
                "generation": STATE.watch_generation,
            },
            "events": STATE.events[:40],
        }


@app.post("/api/sim/budgets/{namespace}/{name}")
async def update_budget(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        pdb = STATE.pdbs.get((namespace, name))
        if pdb is None:
            return status_response(404, "NotFound", f'poddisruptionbudgets "{name}" not found')
        if "minAvailable" in body:
            value = body["minAvailable"]
            if value is None:
                pdb.min_available = None
            else:
                value = int(value)
                if value < 0:
                    return status_response(422, "Invalid", "minAvailable 不能为负")
                pdb.min_available = value
                pdb.max_unavailable = None
        elif "maxUnavailable" in body:
            value = int(body["maxUnavailable"])
            if value < 0:
                return status_response(422, "Invalid", "maxUnavailable 不能为负")
            pdb.max_unavailable = value
            pdb.min_available = None
        else:
            return status_response(422, "Invalid", "需要 minAvailable 或 maxUnavailable")
        pdb.generation += 1
        pdb.resource_version += 1
        STATE._event(
            "budget",
            f"预算 {namespace}/{name} 调整为 "
            + (
                f"minAvailable={pdb.min_available}"
                if pdb.min_available is not None
                else f"maxUnavailable={pdb.max_unavailable}"
            ),
        )
        return JSONResponse(STATE.pdb_json(pdb))


@app.post("/api/sim/pods/{namespace}/{name}/replace")
async def replace_pod(namespace: str, name: str, request: Request) -> Response:
    """Re-create the pod under the same name with a new UID.

    This is how a StatefulSet behaves when its pod is evicted: the name is
    reused, the object identity is not.
    """
    body: dict[str, Any] = {}
    if request.headers.get("content-length") not in (None, "0"):
        body = await request.json()
    with STATE.lock:
        old = _find_pod(namespace, name)
        if old is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        node = body.get("node") or old.node
        if node not in STATE.nodes:
            return status_response(422, "Invalid", f"节点 {node} 不存在")
        ready = bool(body.get("ready", True))
        replacement = SimPod(
            namespace=namespace,
            name=name,
            node=node,
            labels=dict(old.labels),
            owner_kind=old.owner_kind,
            static=old.static,
            ready=ready,
        )
        STATE.pods[replacement.key] = replacement
        STATE._event(
            "replace",
            f"{namespace}/{name} 同名重建：UID {old.uid[:8]} → {replacement.uid[:8]}，节点 {node}",
        )
        return JSONResponse(
            {
                "previous_uid": old.uid,
                "pod": {
                    "namespace": namespace,
                    "name": name,
                    "uid": replacement.uid,
                    "node": node,
                    "ready": ready,
                },
            },
            status_code=201,
        )


@app.post("/api/sim/pods")
async def create_pod(request: Request) -> Response:
    """Add a pod directly, for scenarios such as a finished Job pod.

    This is a rehearsal convenience, not a Kubernetes API endpoint.
    """
    body = await request.json()
    namespace = str(body.get("namespace") or "default")
    name = str(body.get("name") or "")
    node = body.get("node") or NODE_NAMES[0]
    if not name:
        return status_response(422, "Invalid", "需要 name")
    with STATE.lock:
        if node not in STATE.nodes:
            return status_response(422, "Invalid", f"节点 {node} 不存在")
        if (namespace, name) in STATE.pods:
            return status_response(
                409, "AlreadyExists", f'pods "{name}" already exists'
            )
        pod = SimPod(
            namespace=namespace,
            name=name,
            node=node,
            labels=dict(body.get("labels") or {}),
            owner_kind=body.get("owner_kind"),
            static=bool(body.get("static", False)),
            ready=bool(body.get("ready", True)),
            phase=str(body.get("phase") or "Running"),
        )
        STATE.pods[pod.key] = pod
        STATE._event("create", f"创建 Pod {namespace}/{name} 于 {node}")
        return JSONResponse(STATE.pod_json(pod), status_code=201)


@app.post("/api/sim/pods/{namespace}/{name}/ready")
async def set_ready(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        pod.ready = bool(body.get("ready", True))
        pod.resource_version += 1
        STATE._event(
            "ready",
            f"{namespace}/{name} 标记为{'就绪' if pod.ready else '未就绪'}",
        )
        return JSONResponse(STATE.pod_json(pod))


@app.post("/api/sim/pods/{namespace}/{name}/exit")
async def finish_terminating(namespace: str, name: str) -> Response:
    """Let a pod that already has a deletion timestamp finish terminating."""
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        if not pod.terminating:
            return status_response(409, "Conflict", "Pod 尚未进入退出流程，拒绝直接移除")
        del STATE.pods[pod.key]
        STATE._event("exit", f"{namespace}/{name} 已退出节点 {pod.node}")
        return JSONResponse({"exited": True, "namespace": namespace, "name": name})


@app.post("/api/sim/pods/exit-terminating")
def finish_all_terminating() -> dict[str, Any]:
    with STATE.lock:
        finished = [
            f"{pod.namespace}/{pod.name}"
            for pod in list(STATE.pods.values())
            if pod.terminating
        ]
        for pod in [p for p in STATE.pods.values() if p.terminating]:
            STATE.pods.pop(pod.key, None)
        if finished:
            STATE._event("exit", f"{len(finished)} 个待退出 Pod 已退出")
        return {"exited": finished}


@app.post("/api/sim/reset")
def reset_cluster() -> dict[str, str]:
    STATE.reset()
    return {"status": "reset"}


# -- EndpointSlice rehearsal controls -------------------------------------
def _find_slice(namespace: str, name: str) -> SimSlice | None:
    return STATE.slices.get((namespace, name))


def _find_endpoint(slice_: SimSlice, address: str) -> SimEndpoint | None:
    for endpoint in slice_.endpoints:
        if endpoint.address == address:
            return endpoint
    return None


@app.post("/api/sim/endpoints/{namespace}/{slice_name}/{address}")
async def set_endpoint_status(
    namespace: str, slice_name: str, address: str, request: Request
) -> Response:
    """Switch an endpoint's ready / serving / terminating bits.

    This is the "endpoint status" control: marking the last ready endpoint
    on a node as not-ready-with-terminating rehearses the fallback rule, and
    clearing everything rehearses "无可用本地后端".
    """
    body: dict[str, Any] = {}
    if request.headers.get("content-length") not in (None, "0"):
        body = await request.json()
    with STATE.lock:
        slice_ = _find_slice(namespace, slice_name)
        if slice_ is None:
            return status_response(
                404, "NotFound", f'endpointslices "{slice_name}" not found'
            )
        endpoint = _find_endpoint(slice_, address)
        if endpoint is None:
            return status_response(404, "NotFound", f'endpoint "{address}" not found')
        if "ready" in body:
            endpoint.ready = bool(body["ready"])
        if "serving" in body:
            endpoint.serving = bool(body["serving"])
        if "terminating" in body:
            endpoint.terminating = bool(body["terminating"])
        STATE.publish("MODIFIED", slice_)
        STATE._event(
            "endpoint",
            f"{namespace}/{slice_.name} 端点 {address} → "
            f"ready={endpoint.ready} serving={endpoint.serving} "
            f"terminating={endpoint.terminating}",
        )
        return JSONResponse(STATE.slice_json(slice_))


@app.post("/api/sim/endpoints/{namespace}/{slice_name}/{address}/delete")
def delete_endpoint(namespace: str, slice_name: str, address: str) -> Response:
    """Remove just one address; the slice and its siblings stay."""
    with STATE.lock:
        slice_ = _find_slice(namespace, slice_name)
        if slice_ is None:
            return status_response(
                404, "NotFound", f'endpointslices "{slice_name}" not found'
            )
        endpoint = _find_endpoint(slice_, address)
        if endpoint is None:
            return status_response(404, "NotFound", f'endpoint "{address}" not found')
        slice_.endpoints = [e for e in slice_.endpoints if e.address != address]
        STATE.publish("MODIFIED", slice_)
        STATE._event("endpoint", f"{namespace}/{slice_.name} 删除端点 {address}")
        return JSONResponse(STATE.slice_json(slice_))


@app.post("/api/sim/slices/{namespace}/{name}/delete")
def delete_slice(namespace: str, name: str) -> Response:
    """Delete a whole EndpointSlice (a DELETED watch event)."""
    with STATE.lock:
        slice_ = _find_slice(namespace, name)
        if slice_ is None:
            return status_response(
                404, "NotFound", f'endpointslices "{name}" not found'
            )
        STATE.publish("DELETED", slice_)
        del STATE.slices[slice_.key]
        STATE._event("slice-deleted", f"EndpointSlice {namespace}/{name} 已删除")
        return JSONResponse({"deleted": True, "namespace": namespace, "name": name})


@app.post("/api/sim/slices")
async def create_slice(request: Request) -> Response:
    """Re-create a slice, e.g. to rehearse a backend that must reappear."""
    body = await request.json()
    namespace = str(body.get("namespace") or "shop")
    name = str(body.get("name") or "")
    service = str(body.get("service") or "web")
    if not name:
        return status_response(422, "Invalid", "需要 name")
    with STATE.lock:
        if (namespace, name) in STATE.slices:
            return status_response(
                409, "AlreadyExists", f'endpointslices "{name}" already exists'
            )
        slice_ = SimSlice(
            name=name,
            namespace=namespace,
            service=service,
            port=int(body.get("port") or 8080),
            port_name=str(body.get("port_name") or "http"),
            endpoints=[
                SimEndpoint(
                    address=str(item["address"]),
                    pod=str(item.get("pod") or ""),
                    namespace=namespace,
                    node=str(item["node"]),
                    ready=bool(item.get("ready", True)),
                    serving=bool(item.get("serving", True)),
                    terminating=bool(item.get("terminating", False)),
                )
                for item in body.get("endpoints") or []
            ],
        )
        STATE.slices[slice_.key] = slice_
        STATE.publish("ADDED", slice_)
        STATE._event("slice-created", f"EndpointSlice {namespace}/{name} 已创建")
        return JSONResponse(STATE.slice_json(slice_), status_code=201)


@app.post("/api/sim/slices/expire")
def expire_versions() -> dict[str, Any]:
    """Forget watch history so the console's version now yields 410.

    The console must answer by throwing its snapshot away and relisting;
    a slice deleted in the meantime must not survive that relist.
    """
    with STATE.lock:
        before = STATE.expire_older_than_current()
        STATE._event(
            "watch-expired",
            f"已丢弃 watch 历史；早于 rv={before} 的版本将返回 410",
        )
        return {
            "expiredBefore": before,
            "resourceVersion": STATE.revision,
        }


@app.post("/api/sim/watch")
async def set_watch_state(request: Request) -> Response:
    """Break or restore the watch transport.

    Breaking while a stream is open closes it; restoring lets the console
    reconnect.  A snapshot taken before the break must be shown stale until
    then.
    """
    body = await request.json()
    enabled = bool(body.get("enabled", True))
    with STATE.lock:
        STATE.watch_enabled = enabled
        STATE.watch_generation += 1
        STATE.condition.notify_all()
        STATE._event(
            "watch-connected" if enabled else "watch-disconnected",
            "订阅通道已恢复" if enabled else "订阅通道已断开（演练）",
        )
        return JSONResponse(
            {"enabled": STATE.watch_enabled, "generation": STATE.watch_generation}
        )


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "sim.html", {"request": request})


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
