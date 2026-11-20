"""Shared fixtures: run the simulator in-process on a real HTTP port."""

from __future__ import annotations

import socket
import threading
import time

import pytest
import uvicorn

from app.drain import DrainManager
from app.kube import KubeClient


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def simulator_url():
    from simulator.main import STATE, app as simulator_app

    STATE.reset()
    port = _free_port()
    config = uvicorn.Config(
        simulator_app, host="127.0.0.1", port=port, log_level="warning", lifespan="off"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started and time.time() < deadline:
        time.sleep(0.02)
    if not server.started:
        raise RuntimeError("simulator did not start")
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)


@pytest.fixture()
def kube(simulator_url):
    return KubeClient.from_host(simulator_url)


@pytest.fixture()
def manager(kube):
    return DrainManager(kube)


@pytest.fixture()
def sim(simulator_url):
    """A tiny client for the simulator's rehearsal controls."""

    import httpx

    return httpx.Client(base_url=simulator_url, timeout=10.0)
