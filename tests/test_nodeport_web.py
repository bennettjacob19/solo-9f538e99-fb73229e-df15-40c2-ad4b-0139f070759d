"""HTTP-level tests for the NodePort console page and its polling API."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture()
def client(simulator_url, monkeypatch):
    monkeypatch.setenv("DRAIN_KUBE_MODE", "host")
    monkeypatch.setenv("DRAIN_KUBE_HOST", simulator_url)
    return TestClient(create_app())


def view(client, namespace="shop", service="web", port=30080, session=None):
    params = {"namespace": namespace, "service": service, "port": port}
    if session:
        params["session"] = session
    return client.get("/api/nodeport/view", params=params)


def modes(payload):
    return {node["name"]: node["mode"] for node in payload["nodes"]}


def test_page_form_and_nav(client):
    page = client.get("/nodeport")
    assert page.status_code == 200
    assert "NodePort 本地后端排查" in page.text
    assert 'name="namespace"' in page.text
    # The top navigation reaches it from the drain page too.
    assert "NodePort 本地后端" in client.get("/").text


def test_page_renders_ready_backends(client):
    page = client.get(
        "/nodeport", params={"namespace": "shop", "service": "web", "port": 30080}
    )
    assert page.status_code == 200
    assert "10.244.1.11" in page.text
    assert "8080" in page.text
    assert "就绪" in page.text
    assert "已同步" in page.text


def test_page_renders_fallback_and_no_backend(client):
    page = client.get(
        "/nodeport", params={"namespace": "shop", "service": "api", "port": 30081}
    ).text
    assert "备用（终止中）" in page
    assert "无可用本地后端" in page
    assert "10.244.1.40" in page


def test_page_rejects_cluster_policy(client):
    page = client.get(
        "/nodeport", params={"namespace": "shop", "service": "metrics", "port": 30082}
    )
    assert page.status_code == 200
    assert "externalTrafficPolicy=Local" in page.text


def test_api_reports_status_and_nodes(client):
    response = view(client)
    assert response.status_code == 200
    payload = response.json()
    assert payload["snapshot"]["status"] == "live"
    assert payload["snapshot"]["slice_count"] == 3
    assert modes(payload) == {
        "node-a": "ready",
        "node-b": "ready",
        "node-c": "ready",
    }
    node_a = next(n for n in payload["nodes"] if n["name"] == "node-a")
    assert [b["address"] for b in node_a["backends"]] == [
        "10.244.1.11",
        "10.244.1.12",
    ]
    assert {b["port"] for b in node_a["backends"]} == {8080}


def test_api_rejects_unsupported_service(client):
    response = view(client, service="metrics", port=30082)
    assert response.status_code == 422
    assert "externalTrafficPolicy=Local" in response.json()["error"]


def test_api_rejects_unknown_nodeport(client):
    response = view(client, port=39999)
    assert response.status_code == 422
    assert "未暴露 NodePort" in response.json()["error"]


def test_a_session_only_counts_for_its_own_service(client):
    session = view(client, service="api", port=30081).json()["snapshot"]["session"]
    # Using that session for another service must mint a new one rather than
    # attributing another service's slices to it.
    payload = view(client, service="web", port=30080, session=session).json()
    assert payload["snapshot"]["session"] != session
    keys = {
        item["key"]
        for item in client.get("/api/nodeport/subscriptions").json()["subscriptions"]
    }
    assert keys == {"shop/api", "shop/web"}


def test_unreachable_api_server_shows_an_error(monkeypatch):
    monkeypatch.setenv("DRAIN_KUBE_MODE", "host")
    monkeypatch.setenv("DRAIN_KUBE_HOST", "http://127.0.0.1:1")
    offline = TestClient(create_app())
    page = offline.get(
        "/nodeport", params={"namespace": "shop", "service": "web", "port": 30080}
    )
    assert page.status_code == 200
    assert "无法显示" in page.text


def test_two_pages_share_one_subscription(client):
    first = view(client).json()["snapshot"]["session"]
    second = view(client).json()["snapshot"]["session"]
    assert first != second
    subscriptions = client.get("/api/nodeport/subscriptions").json()["subscriptions"]
    assert len(subscriptions) == 1
    assert subscriptions[0]["key"] == "shop/web"
    assert subscriptions[0]["observers"] == 2


def test_closing_one_page_leaves_the_other_polling(client, sim):
    payload = view(client).json()
    session_a = payload["snapshot"]["session"]
    session_b = view(client).json()["snapshot"]["session"]

    client.post("/api/nodeport/close", data={"session": session_a})
    subscriptions = client.get("/api/nodeport/subscriptions").json()["subscriptions"]
    assert len(subscriptions) == 1
    assert subscriptions[0]["observers"] == 1

    sim.post(
        "/api/sim/endpoints/shop/web-node-a/10.244.1.11",
        json={"ready": False, "terminating": True},
    )
    deadline = time.time() + 4
    while time.time() < deadline:
        current = view(client, session=session_b).json()
        node_a = next(n for n in current["nodes"] if n["name"] == "node-a")
        if [b["address"] for b in node_a["backends"]] == ["10.244.1.12"]:
            break
        time.sleep(0.05)
    else:  # pragma: no cover
        raise AssertionError("the still-open page stopped updating")


def test_expired_session_is_replaced_transparently(client):
    payload = view(client).json()
    session = payload["snapshot"]["session"]
    client.post("/api/nodeport/close", data={"session": session})

    response = view(client, session=session)
    assert response.status_code == 200
    fresh = response.json()["snapshot"]["session"]
    assert fresh != session
    # One page is watching again.
    subscriptions = client.get("/api/nodeport/subscriptions").json()["subscriptions"]
    assert subscriptions[0]["observers"] >= 1


def test_break_and_restore_marks_stale_then_live(client, sim):
    payload = view(client).json()
    session = payload["snapshot"]["session"]

    sim.post("/api/sim/watch", json={"enabled": False})
    deadline = time.time() + 4
    while time.time() < deadline:
        current = view(client, session=session).json()
        if current["snapshot"]["stale"]:
            break
        time.sleep(0.05)
    else:  # pragma: no cover
        raise AssertionError("snapshot was never marked stale")
    # The old result is preserved (not blanked) while stale.
    assert current["snapshot"]["slice_count"] == 3
    assert current["snapshot"]["last_error"]

    sim.post("/api/sim/watch", json={"enabled": True})
    deadline = time.time() + 4
    while time.time() < deadline:
        current = view(client, session=session).json()
        if not current["snapshot"]["stale"]:
            return
        time.sleep(0.05)
    raise AssertionError("snapshot did not recover")  # pragma: no cover


def test_410_relist_drops_deleted_backend_via_api(client, sim):
    payload = view(client).json()
    session = payload["snapshot"]["session"]
    assert payload["snapshot"]["slice_count"] == 3

    sim.post("/api/sim/watch", json={"enabled": False})
    deadline = time.time() + 4
    while time.time() < deadline:
        if view(client, session=session).json()["snapshot"]["stale"]:
            break
        time.sleep(0.05)

    sim.post("/api/sim/slices/shop/web-node-c/delete")
    sim.post("/api/sim/slices/expire")
    sim.post("/api/sim/watch", json={"enabled": True})

    deadline = time.time() + 4
    while time.time() < deadline:
        current = view(client, session=session).json()
        if not current["snapshot"]["stale"] and current["snapshot"]["slice_count"] == 2:
            break
        time.sleep(0.05)
    else:  # pragma: no cover
        raise AssertionError("relist did not happen")

    node_c = next(n for n in current["nodes"] if n["name"] == "node-c")
    assert node_c["mode"] == "none"
    assert "10.244.3.31" not in client.get(
        "/nodeport", params={"namespace": "shop", "service": "web", "port": 30080}
    ).text
