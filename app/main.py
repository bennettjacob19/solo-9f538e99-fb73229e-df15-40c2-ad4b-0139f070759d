"""FastAPI web front end for node drain.

Routes are intentionally thin: all cluster semantics live in
``app.drain.DrainManager``.  Every mutating action is a POST followed by a
redirect, so a browser refresh never repeats an eviction.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import Settings, build_client
from .drain import DrainError, DrainManager
from .kube import KubeClient, KubeError
from .models import DrainPhase, PodState
from .nodeport import (
    NodePortError,
    NodePortHub,
    build_node_views,
    describe_service,
)
from .store import RoundStore

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def create_app(manager: DrainManager | None = None) -> FastAPI:
    settings = Settings.from_env()
    app = FastAPI(title=settings.title, docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
    app.state.settings = settings
    app.state.manager = manager
    app.state.startup_error = ""
    app.state.kube = manager.kube if manager is not None else None
    app.state.nodeport = None

    def get_manager(request: Request) -> DrainManager:
        if request.app.state.manager is None:
            try:
                request.app.state.manager = DrainManager(
                    build_client(request.app.state.settings),
                    RoundStore.from_env(),
                )
            except Exception as exc:  # surfaced on the page instead of a 500
                request.app.state.startup_error = str(exc)
                raise
        return request.app.state.manager

    def get_kube(request: Request) -> KubeClient:
        kube = request.app.state.kube
        if kube is None:
            kube = get_manager(request).kube
            request.app.state.kube = kube
        return kube

    def get_hub(request: Request) -> NodePortHub:
        hub = request.app.state.nodeport
        if hub is None:
            hub = NodePortHub(get_kube(request))
            request.app.state.nodeport = hub
        return hub

    def render(request: Request, template: str, **context) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            template,
            {
                "request": request,
                "title": request.app.state.settings.title,
                "simulator_url": os.environ.get("DRAIN_SIM_URL", ""),
                "phase": DrainPhase,
                "state": PodState,
                **context,
            },
        )

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> HTMLResponse:
        try:
            manager = get_manager(request)
            nodes = manager.nodes()
            rounds = manager.list_rounds()
            error = ""
        except Exception as exc:
            nodes, rounds, error = [], [], str(exc)
        return render(request, "index.html", nodes=nodes, rounds=rounds, error=error)

    @app.post("/drain")
    def start_drain(request: Request, node: str = Form(...)) -> RedirectResponse:
        try:
            manager = get_manager(request)
            rnd = manager.start(node.strip())
        except (DrainError, KubeError) as exc:
            return RedirectResponse(f"/?error={quote(str(exc))}", status_code=303)
        return RedirectResponse(f"/drain/{rnd.round_id}", status_code=303)

    @app.get("/drain/{round_id}", response_class=HTMLResponse)
    def round_page(request: Request, round_id: str) -> HTMLResponse:
        try:
            manager = get_manager(request)
        except Exception as exc:
            return render(request, "missing.html", round_id=round_id, error=str(exc))  # type: ignore[arg-type]
        rnd = manager.get(round_id)
        if rnd is None:
            return render(request, "missing.html", round_id=round_id)  # type: ignore[arg-type]
        # Reflection only; it never evicts.  This keeps "待退出" / "可下线"
        # honest when the operator refreshes the page.
        manager.observe(round_id)
        return render(request, "round.html", round=rnd, error=request.query_params.get("error", ""))

    def _require(manager: DrainManager, round_id: str):
        rnd = manager.get(round_id)
        if rnd is None:
            raise DrainError("排空轮次不存在")
        return rnd

    @app.post("/drain/{round_id}/run")
    def run_round(request: Request, round_id: str) -> RedirectResponse:
        try:
            manager = get_manager(request)
            rnd = _require(manager, round_id)
            if rnd.phase is not DrainPhase.RUNNING:
                # Continuing always starts from a fresh read of the cluster.
                manager.resume(round_id)
            manager.run(round_id)
        except (DrainError, KubeError) as exc:
            return RedirectResponse(
                f"/drain/{round_id}?error={quote(str(exc))}", status_code=303
            )
        return RedirectResponse(f"/drain/{round_id}", status_code=303)

    @app.post("/drain/{round_id}/interrupt")
    def interrupt_round(request: Request, round_id: str) -> RedirectResponse:
        try:
            manager = get_manager(request)
            manager.interrupt(round_id)
        except (DrainError, KubeError) as exc:
            return RedirectResponse(
                f"/drain/{round_id}?error={quote(str(exc))}", status_code=303
            )
        return RedirectResponse(f"/drain/{round_id}", status_code=303)

    @app.post("/drain/{round_id}/recheck")
    def recheck_round(request: Request, round_id: str) -> RedirectResponse:
        try:
            manager = get_manager(request)
            _require(manager, round_id)
            manager.recheck(round_id)
        except (DrainError, KubeError) as exc:
            return RedirectResponse(
                f"/drain/{round_id}?error={quote(str(exc))}", status_code=303
            )
        return RedirectResponse(f"/drain/{round_id}", status_code=303)

    @app.get("/api/rounds", response_class=JSONResponse)
    def api_rounds(request: Request):
        try:
            manager = get_manager(request)
        except Exception:
            return JSONResponse({"error": request.app.state.startup_error}, status_code=503)
        return [
            {
                "round_id": rnd.round_id,
                "node": rnd.node,
                "phase": rnd.phase.value,
                "offline": rnd.offline,
                "offline_reason": rnd.offline_reason,
                "message": rnd.message,
                "updated_at": rnd.updated_at,
                "counts": rnd.counts,
                "pods": [
                    {
                        "namespace": p.namespace,
                        "name": p.name,
                        "uid": p.uid,
                        "state": p.state.value,
                        "message": p.message,
                        "exempt_reason": p.exempt_reason,
                    }
                    for p in rnd.evictable + rnd.exempt
                ],
            }
            for rnd in manager.list_rounds()
        ]

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    # -- NodePort local backends ------------------------------------------
    #
    # Each page holds a server-side session that references the shared
    # list+watch subscription for its service.  Two pages watching the same
    # service share one upstream watch; closing one session leaves the other
    # untouched.  The browser polls ``/api/nodeport/view`` and re-renders only
    # when the subscription revision changes.
    def _open_nodeport(request: Request, namespace: str, name: str, node_port: int):
        kube = get_kube(request)
        service = kube.get_service(namespace, name)
        entry = describe_service(service, node_port)
        hub = get_hub(request)
        session, subscription = hub.open_session(namespace, name)
        return entry, hub, session, subscription

    def _nodeport_payload(
        hub: NodePortHub, entry, subscription, session: str
    ) -> tuple[dict, list]:
        slices = subscription.slices()
        views = build_node_views(slices, entry, hub.node_names())
        state = subscription.state()
        state["session"] = session
        payload = {
            "service": {
                "namespace": entry.namespace,
                "name": entry.name,
                "node_port": entry.node_port,
                "port_name": entry.port_name,
                "service_port": entry.service_port,
                "target_port": entry.target_port,
                "policy": entry.policy,
                "address_type": entry.address_type,
            },
            "snapshot": state,
            "nodes": [view.to_dict() for view in views],
        }
        return payload, views

    @app.get("/nodeport", response_class=HTMLResponse)
    def nodeport_page(
        request: Request,
        namespace: str = "",
        service: str = "",
        port: str = "",
        nodeport: str = "",
        node_port: str = "",
    ) -> HTMLResponse:
        namespace = namespace.strip()
        service = service.strip()
        # Accept the port under either the short name used by the form or the
        # Kubernetes spelling, so deep links from runbooks keep working.
        port = (port or nodeport or node_port).strip()
        context = {
            "namespace": namespace,
            "service": service,
            "port": port,
            "entry": None,
            "payload": None,
            "views": [],
            "error": "",
        }
        if namespace and service and port:
            try:
                entry, hub, session, subscription = _open_nodeport(
                    request, namespace, service, int(port)
                )
            except NodePortError as exc:
                context["error"] = str(exc)
            except (KubeError, ValueError) as exc:
                context["error"] = str(exc)
            else:
                payload, views = _nodeport_payload(
                    hub, entry, subscription, session
                )
                context.update(entry=entry, payload=payload, views=views)
        elif namespace or service or port:
            context["error"] = "请填写命名空间、Service 名和 NodePort。"
        return render(request, "nodeport.html", **context)

    @app.get("/api/nodeport/view", response_class=JSONResponse)
    def nodeport_api(
        request: Request,
        namespace: str,
        service: str,
        port: int | None = None,
        nodeport: int | None = None,
        node_port: int | None = None,
        session: str = "",
    ):
        node_port_value = port if port is not None else (nodeport or node_port)
        if node_port_value is None:
            return JSONResponse({"error": "需要 NodePort"}, status_code=422)
        hub = get_hub(request)
        subscription = (
            hub.touch_session(session, namespace, service) if session else None
        )
        if subscription is None:
            # The page's session expired or the process restarted; hand back
            # a fresh one so the page keeps updating without a reload.
            try:
                entry, hub, session, subscription = _open_nodeport(
                    request, namespace, service, node_port_value
                )
            except NodePortError as exc:
                return JSONResponse({"error": str(exc)}, status_code=422)
            except (KubeError, ValueError) as exc:
                return JSONResponse({"error": str(exc)}, status_code=503)
            payload, _ = _nodeport_payload(hub, entry, subscription, session)
            return payload
        try:
            service_obj = get_kube(request).get_service(namespace, service)
            entry = describe_service(service_obj, node_port_value)
        except NodePortError as exc:
            return JSONResponse({"error": str(exc)}, status_code=422)
        except KubeError as exc:
            return JSONResponse({"error": str(exc)}, status_code=503)
        payload, _ = _nodeport_payload(hub, entry, subscription, session)
        return payload

    @app.post("/api/nodeport/close", response_class=JSONResponse)
    def nodeport_close(request: Request, session: str = Form(...)):
        get_hub(request).close_session(session)
        return {"closed": True}

    @app.get("/api/nodeport/subscriptions", response_class=JSONResponse)
    def nodeport_subscriptions(request: Request):
        return {"subscriptions": get_hub(request).describe()}

    return app


app = create_app()
