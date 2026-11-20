"""Behavioural tests for the NodePort local-backend console.

The key properties, straight from the task:

* only ``externalTrafficPolicy: Local`` IPv4 TCP services are accepted;
* each node lists its ready endpoints, or — if it has none — its
  terminating-but-serving ones, or reports "no usable local backend";
* two pages on the same service share one subscription, and closing one
  leaves the other updating;
* a dropped connection marks the old result as stale;
* a 410 after an outage replaces the snapshot with a full list, including
  slices deleted while the watch was down, and then resumes the stream.
"""

from __future__ import annotations

import time

import pytest

from app.kube import KubeClient
from app.nodeport import (
    NodePortError,
    NodePortHub,
    ServiceEntry,
    _endpoint_conditions,
    build_node_views,
    describe_service,
    resolve_port,
)


def wait_until(predicate, timeout=4.0, interval=0.03):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_endpoint_condition_defaults():
    # Unset conditions mean "ready and serving" (older API servers).
    assert _endpoint_conditions({}) == (True, True, False)
    # Terminating endpoints are never ready, even if ready is unset.
    assert _endpoint_conditions(
        {"conditions": {"terminating": True}}
    ) == (False, False, True)
    # A terminating endpoint that still serves is the fallback.
    assert _endpoint_conditions(
        {"conditions": {"terminating": True, "serving": True}}
    ) == (False, True, True)
    # ready=false, not terminating -> not ready; serving defaults to ready.
    assert _endpoint_conditions(
        {"conditions": {"ready": False}}
    ) == (False, False, False)


def test_resolve_port_prefers_named_slice_port():
    entry = ServiceEntry(
        namespace="shop",
        name="web",
        node_port=30080,
        port_name="http",
        service_port=80,
        target_port=8080,
    )
    ports = [
        {"name": "metrics", "port": 9999},
        {"name": "http", "port": 8080},
    ]
    assert resolve_port(ports, entry) == 8080
    # A single unnamed port is unambiguous.
    single = ServiceEntry("shop", "web", 30080, "", 80, None)
    assert resolve_port([{"port": 7000}], single) == 7000
    # Several ports, none matching, and no numeric target -> no guess.
    assert resolve_port(
        [{"name": "a", "port": 1}, {"name": "b", "port": 2}], entry
    ) is None


def view_for(hub, entry):
    subscription = hub._subscriptions[(entry.namespace, entry.name)]
    return {
        view.node: view
        for view in build_node_views(
            subscription.slices(), entry, hub.node_names()
        )
    }, subscription


def ports(view):
    return [(backend.address, backend.port) for backend in view.backends]


def test_accepts_local_ipv4_tcp_nodeport(kube):
    entry = describe_service(kube.get_service("shop", "web"), 30080)
    assert entry.policy == "Local"
    assert entry.port_name == "http"
    assert entry.service_port == 80
    assert entry.target_port == 8080


def test_rejects_cluster_policy(kube):
    service = kube.get_service("shop", "metrics")
    with pytest.raises(NodePortError, match="externalTrafficPolicy=Local"):
        describe_service(service, 30082)


def test_rejects_unknown_nodeport(kube):
    with pytest.raises(NodePortError, match="未暴露 NodePort"):
        describe_service(kube.get_service("shop", "web"), 39999)


def test_rejects_missing_service(kube):
    with pytest.raises(NodePortError, match="找不到"):
        describe_service(kube.get_service("shop", "nope"), 30080)


def test_ready_endpoints_are_listed_per_node(kube, hub):
    entry = describe_service(kube.get_service("shop", "web"), 30080)
    session, _ = hub.open_session("shop", "web")
    assert wait_until(lambda: not hub._subscriptions[("shop", "web")].state()["stale"])
    views, _ = view_for(hub, entry)

    assert set(views) == {"node-a", "node-b", "node-c"}
    assert views["node-a"].mode == "ready"
    assert ports(views["node-a"]) == [("10.244.1.11", 8080), ("10.244.1.12", 8080)]
    assert views["node-c"].mode == "ready"
    assert ports(views["node-c"]) == [("10.244.3.31", 8080)]
    hub.close_session(session)


def test_terminating_only_endpoints_are_the_fallback(kube, hub, sim):
    entry = describe_service(kube.get_service("shop", "api"), 30081)
    session, subscription = hub.open_session("shop", "api")
    assert wait_until(lambda: not subscription.state()["stale"])
    views, _ = view_for(hub, entry)

    # node-a has no ready endpoint, only a terminating one that still serves.
    assert views["node-a"].mode == "fallback"
    assert ports(views["node-a"]) == [("10.244.1.40", 9090)]
    # node-b has no endpoints at all; node-c's endpoint is neither ready nor
    # serving.
    assert views["node-b"].mode == "none"
    assert views["node-c"].mode == "none"
    assert views["node-b"].backends == []
    hub.close_session(session)


def test_a_slice_from_another_namespace_never_leaks_in(hub, sim):
    """Two namespaces may host a service with the same name and label.

    The subscription is namespace-scoped; a slice in another namespace must
    not appear in this service's backends.
    """
    from simulator.main import SimEndpoint, SimSlice
    from simulator.main import STATE

    entry = describe_service(hub.kube.get_service("shop", "web"), 30080)
    session, subscription = hub.open_session("shop", "web")
    assert wait_until(lambda: not subscription.state()["stale"])

    with STATE.lock:
        other = SimSlice(
            name="web-other",
            namespace="prod",
            service="web",
            endpoints=[SimEndpoint("10.9.9.9", "x", "node-a", "node-a")],
        )
        STATE.slices[other.key] = other
        STATE.publish("ADDED", other)
    time.sleep(0.4)

    addresses = [
        backend.address
        for view in view_for(hub, entry)[0].values()
        for backend in view.backends
    ]
    assert "10.9.9.9" not in addresses
    assert subscription.state()["slice_count"] == 3
    hub.close_session(session)


def test_ready_endpoints_hide_the_terminating_ones(kube, hub, sim):
    entry = describe_service(kube.get_service("shop", "web"), 30080)
    session, subscription = hub.open_session("shop", "web")
    assert wait_until(lambda: not subscription.state()["stale"])

    # Ask node-a to keep a terminating backend next to its ready ones.
    sim.post(
        "/api/sim/endpoints/shop/web-node-a/10.244.1.11",
        json={"ready": False, "serving": True, "terminating": True},
    )
    assert wait_until(
        lambda: view_for(hub, entry)[0]["node-a"].mode == "ready"
        and len(view_for(hub, entry)[0]["node-a"].backends) == 1
    )
    v = view_for(hub, entry)[0]["node-a"]
    assert ports(v) == [("10.244.1.12", 8080)]
    hub.close_session(session)


def test_no_ready_and_no_terminating_is_none(kube, hub, sim):
    entry = describe_service(kube.get_service("shop", "web"), 30080)
    session, subscription = hub.open_session("shop", "web")
    assert wait_until(lambda: not subscription.state()["stale"])

    sim.post(
        "/api/sim/endpoints/shop/web-node-c/10.244.3.31",
        json={"ready": False, "serving": False, "terminating": True},
    )
    assert wait_until(
        lambda: view_for(hub, entry)[0]["node-c"].mode == "none"
    )
    assert view_for(hub, entry)[0]["node-c"].backends == []
    hub.close_session(session)


def test_two_pages_share_one_subscription_and_one_close_keeps_the_other_live(
    kube, hub, sim
):
    entry = describe_service(kube.get_service("shop", "web"), 30080)
    first, subscription = hub.open_session("shop", "web")
    second, same = hub.open_session("shop", "web")

    assert same is subscription
    assert subscription.observers == 2
    assert len(hub.describe()) == 1
    assert wait_until(lambda: not subscription.state()["stale"])

    hub.close_session(first)
    assert subscription.observers == 1
    assert len(hub.describe()) == 1

    # The remaining page must still see changes.
    before = subscription.revision
    sim.post(
        "/api/sim/endpoints/shop/web-node-b/10.244.2.21",
        json={"ready": False, "terminating": True},
    )
    assert wait_until(lambda: subscription.revision > before)
    assert view_for(hub, entry)[0]["node-b"].mode == "ready"

    hub.close_session(second)
    assert wait_until(lambda: not hub.describe())


def test_closing_the_last_page_stops_the_watch(kube, hub):
    session, subscription = hub.open_session("shop", "web")
    assert wait_until(lambda: subscription._thread is not None)
    hub.close_session(session)
    assert wait_until(lambda: not hub.describe())
    # A new page opens a fresh subscription instead of reusing a stopped one.
    session2, subscription2 = hub.open_session("shop", "web")
    assert subscription2 is not subscription
    hub.close_session(session2)


def test_broken_watch_marks_the_snapshot_stale(kube, hub, sim):
    entry = describe_service(kube.get_service("shop", "web"), 30080)
    session, subscription = hub.open_session("shop", "web")
    assert wait_until(lambda: not subscription.state()["stale"])

    sim.post("/api/sim/watch", json={"enabled": False})
    assert wait_until(lambda: subscription.state()["stale"])
    assert subscription.state()["last_error"]
    # The old result is kept, not blanked.
    assert subscription.state()["slice_count"] == 3

    sim.post("/api/sim/watch", json={"enabled": True})
    assert wait_until(lambda: not subscription.state()["stale"])
    hub.close_session(session)


def test_410_replaces_the_snapshot_and_drops_deleted_slices(kube, hub, sim):
    """Delete a slice during an outage; after the 410 relist it must be gone.

    The subscription broke, a slice was deleted and the server forgot the old
    resourceVersion.  On reconnect the console must issue a full list rather
    than resume from the stale position — otherwise the deleted backend would
    linger forever.
    """
    entry = describe_service(kube.get_service("shop", "web"), 30080)
    session, subscription = hub.open_session("shop", "web")
    assert wait_until(lambda: not subscription.state()["stale"])
    assert subscription.state()["slice_count"] == 3

    sim.post("/api/sim/watch", json={"enabled": False})
    assert wait_until(lambda: subscription.state()["stale"])

    sim.post("/api/sim/slices/shop/web-node-c/delete")
    sim.post("/api/sim/slices/expire")
    assert subscription.state()["stale"]

    sim.post("/api/sim/watch", json={"enabled": True})
    assert wait_until(lambda: not subscription.state()["stale"])
    assert subscription.state()["slice_count"] == 2

    views, _ = view_for(hub, entry)
    # node-c still exists as a node, but its backend is gone for good.
    assert views["node-c"].mode == "none"
    assert all(
        backend.address != "10.244.3.31"
        for view in views.values()
        for backend in view.backends
    )

    # ...and the stream is live again: a later change is picked up.
    before = subscription.revision
    sim.post(
        "/api/sim/endpoints/shop/web-node-a/10.244.1.11",
        json={"ready": False, "terminating": True},
    )
    assert wait_until(lambda: subscription.revision > before)
    views, _ = view_for(hub, entry)
    assert ports(views["node-a"]) == [("10.244.1.12", 8080)]
    hub.close_session(session)


def test_deleted_endpoint_disappears_while_live(kube, hub, sim):
    entry = describe_service(kube.get_service("shop", "web"), 30080)
    session, subscription = hub.open_session("shop", "web")
    assert wait_until(lambda: not subscription.state()["stale"])

    sim.post("/api/sim/endpoints/shop/web-node-a/10.244.1.11/delete")
    assert wait_until(
        lambda: ports(view_for(hub, entry)[0]["node-a"]) == [("10.244.1.12", 8080)]
    )
    hub.close_session(session)


def test_port_comes_from_the_endpoint_slice(kube, hub):
    entry = describe_service(kube.get_service("shop", "api"), 30081)
    session, _ = hub.open_session("shop", "api")
    subscription = hub._subscriptions[("shop", "api")]
    assert wait_until(lambda: not subscription.state()["stale"])
    views, _ = view_for(hub, entry)
    # shop/api targets 9090, and the slice says 9090 — not the NodePort and
    # not the Service port (8081).
    assert ports(views["node-a"]) == [("10.244.1.40", 9090)]
    hub.close_session(session)


def test_sessions_expire_without_a_live_page(kube):
    hub = NodePortHub(kube, session_ttl=0.3, sweep_interval=0.1)
    session, _ = hub.open_session("shop", "web")
    assert wait_until(lambda: bool(hub.describe()))
    # Never refreshed -> the sweeper drops the session and its watch.
    assert wait_until(lambda: not hub.describe(), timeout=3.0)
    assert hub.touch_session(session, "shop", "web") is None
    hub.stop()
