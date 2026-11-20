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

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
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


class ClusterState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.nodes: dict[str, SimNode] = {}
        self.pods: dict[tuple[str, str], SimPod] = {}
        self.pdbs: dict[tuple[str, str], SimPDB] = {}
        self.events: list[dict[str, Any]] = []
        self.reset()

    # -- seed --------------------------------------------------------------
    def reset(self) -> None:
        with self.lock:
            self.nodes = {name: SimNode(name=name) for name in NODE_NAMES}
            self.pods = {}
            self.pdbs = {}
            self.events = []

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
            self._event("reset", "演练环境已恢复到初始状态")

    def _event(self, kind: str, message: str, **extra: Any) -> None:
        self.events.insert(
            0,
            {"at": _now_iso(), "kind": kind, "message": message, **extra},
        )
        del self.events[200:]

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


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "sim.html", {"request": request})


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
