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
from .kube import KubeError
from .models import DrainPhase, PodState
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

    return app


app = create_app()
