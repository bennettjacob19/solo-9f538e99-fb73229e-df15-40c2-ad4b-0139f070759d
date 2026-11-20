"""Behavioural tests for the drain rules, run against the rehearsal API."""

from __future__ import annotations

from app.drain import DrainError
from app.models import DrainPhase, PodState


def states(round_):
    return {pod.name: pod.state for pod in round_.evictable}


def _labels(round_, pod):
    return "web" if pod.name.startswith("web") else "cache"


def test_cordon_happens_before_pods_are_listed(manager, kube):
    node = kube.get_node("node-a")
    assert node.unschedulable is False

    round_ = manager.start("node-a")

    assert kube.get_node("node-a").unschedulable is True
    assert round_.phase is DrainPhase.RUNNING
    assert round_.evictable, "pods are listed after cordon"

    # Starting the same node twice is refused instead of evicting twice.
    try:
        manager.start("node-a")
    except DrainError:
        pass
    else:  # pragma: no cover
        raise AssertionError("duplicate round was allowed")


def test_daemonsets_and_static_pods_are_never_evicted(manager):
    round_ = manager.start("node-a")
    names = {pod.name for pod in round_.evictable}
    exempt = {pod.name: pod.exempt_reason for pod in round_.exempt}

    assert "log-agent-a" not in names
    assert "kube-vip-a" not in names
    assert exempt == {"log-agent-a": "DaemonSet", "kube-vip-a": "静态 Pod"}


def test_pods_are_evicted_by_uid(manager, kube):
    round_ = manager.start("node-a")
    manager.run(round_.round_id)

    for pod in round_.evictable:
        live = kube.get_pod(pod.namespace, pod.name)
        # Either the pod still exists in terminating state or it is gone; in
        # no case did the eviction touch a different object.
        assert live is None or live.uid == pod.uid


def test_blocked_pods_do_not_stop_the_round(manager, sim):
    """A budget refusal on one pod must not abort the rest of the round.

    With web-pdb at minAvailable=4 (5 web pods -> one disruption allowed),
    node-a's pass evicts cache-0 and web-0, is refused for web-1, and still
    reports the refusal on that row only.
    """
    sim.post("/api/sim/budgets/shop/web-pdb", json={"minAvailable": 4})
    round_ = manager.start("node-a")
    manager.run(round_.round_id)

    result = states(round_)
    assert result["cache-0"] is PodState.TERMINATING
    assert result["web-0"] is PodState.TERMINATING
    assert result["web-1"] is PodState.BLOCKED
    blocked = round_.blocked[0]
    assert "disruption budget" in blocked.message
    assert "PodDisruptionBudget" in round_.message


def test_two_nodes_cannot_exceed_a_shared_budget(manager):
    round_a = manager.start("node-a")
    round_b = manager.start("node-b")

    manager.run(round_a.round_id)
    manager.run(round_b.round_id)

    attempted = {
        pod.name
        for round_ in (round_a, round_b)
        for pod in round_.evictable
        if pod.state is PodState.TERMINATING
    }
    # node-a takes web-0, web-1; node-b gets none of the remaining web pods
    # because minAvailable=3 of 5 leaves exactly two disruptions.
    assert attempted == {"web-0", "web-1", "cache-0"}
    assert {pod.name for pod in round_b.blocked} == {"web-2", "web-3", "cache-1"}


def test_waiting_exit_then_verification_marks_node_offline(manager, sim):
    round_ = manager.start("node-a")
    manager.run(round_.round_id)

    assert round_.phase is DrainPhase.RUNNING
    assert round_.offline is False
    assert {pod.name for pod in round_.waiting_exit} == {"web-0", "web-1", "cache-0"}
    assert "尚有普通 Pod" in round_.offline_reason

    sim.post("/api/sim/pods/exit-terminating")
    manager.recheck(round_.round_id)

    assert round_.phase is DrainPhase.COMPLETE
    assert round_.offline is True
    assert "已确认" in round_.offline_reason
    # DaemonSet and static pod stay, but they do not block going offline.
    assert {pod.exempt_reason for pod in round_.exempt} == {"DaemonSet", "静态 Pod"}


def test_offline_requires_node_to_still_be_unschedulable(manager, sim):
    round_ = manager.start("node-a")
    manager.run(round_.round_id)
    sim.post("/api/sim/pods/exit-terminating")

    # Someone uncordons the node behind our back.
    sim.patch("/api/v1/nodes/node-a", json={"spec": {"unschedulable": False}})
    manager.recheck(round_.round_id)

    assert round_.offline is False
    assert round_.phase is not DrainPhase.COMPLETE
    assert "可调度" in round_.offline_reason


def test_interrupt_keeps_cordon_and_evictions_then_resume_rereads(manager, kube, sim):
    round_ = manager.start("node-a")
    manager.run(round_.round_id)
    manager.interrupt(round_.round_id)

    assert round_.phase is DrainPhase.INTERRUPTED
    assert kube.get_node("node-a").unschedulable is True
    admitted = {pod.name for pod in round_.waiting_exit}
    assert admitted == {"web-0", "web-1", "cache-0"}

    # A replacement appears while the round is interrupted.
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})
    manager.resume(round_.round_id)

    assert round_.phase is DrainPhase.RUNNING
    # The new object is re-read from the cluster, not invented from the old
    # snapshot; the evicted originals are already gone in the cluster's eyes.
    assert kube.get_node("node-a").unschedulable is True


def test_same_name_replacement_is_deferred_not_evicted(manager, sim):
    round_ = manager.start("node-a")
    manager.run(round_.round_id)

    # Re-create web-0 under the same name while the old object is terminating.
    response = sim.post("/api/sim/pods/shop/web-0/replace", json={"node": "node-a"})
    new_uid = response.json()["pod"]["uid"]
    manager.recheck(round_.round_id)

    replacement = next(p for p in round_.evictable if p.uid == new_uid)
    assert replacement.state is PodState.DEFERRED
    assert "留待续排" in replacement.message

    # A second pass leaves it pending for the next round rather than evicting
    # it on the old decision.
    manager.resume(round_.round_id)
    replacement = next(p for p in round_.evictable if p.uid == new_uid)
    assert replacement.state is PodState.PENDING

    manager.run(round_.round_id)
    replacement = next(p for p in round_.evictable if p.uid == new_uid)
    assert replacement.state in (PodState.TERMINATING, PodState.BLOCKED)


def test_budget_recovery_allows_resume(manager, sim):
    round_ = manager.start("node-a")
    sim.post("/api/sim/budgets/shop/cache-pdb", json={"minAvailable": 2})
    manager.run(round_.round_id)
    assert states(round_)["cache-0"] is PodState.BLOCKED

    sim.post("/api/sim/budgets/shop/cache-pdb", json={"minAvailable": 1})
    manager.resume(round_.round_id)
    manager.run(round_.round_id)

    assert states(round_)["cache-0"] is PodState.TERMINATING


def test_finished_pods_do_not_block_offline(manager, sim):
    round_ = manager.start("node-c")
    manager.run(round_.round_id)
    sim.post("/api/sim/pods/exit-terminating")
    manager.recheck(round_.round_id)
    assert round_.offline is True

    # A completed Job pod stays on the drained node; it needs no eviction and
    # must not keep the node from being reported ready to power off.
    sim.post(
        "/api/sim/pods",
        json={
            "namespace": "batch",
            "name": "nightly-1234",
            "node": "node-c",
            "phase": "Succeeded",
            "ready": False,
        },
    )
    manager.recheck(round_.round_id)

    assert round_.offline is True
    assert round_.phase is DrainPhase.COMPLETE
    finished = next(p for p in round_.exempt if p.name == "nightly-1234")
    assert finished.exempt_reason == "已结束"


def test_rounds_survive_manager_restart(manager, sim, kube, tmp_path):
    """A restart must keep the cordon decision record so the operator can
    resume; pod existence is still re-read from the cluster."""
    from app.drain import DrainManager
    from app.store import RoundStore

    store = RoundStore(tmp_path / "rounds.json")
    manager.store = store
    round_ = manager.start("node-a")
    manager.run(round_.round_id)
    manager.interrupt(round_.round_id)

    restarted = DrainManager(kube, RoundStore(tmp_path / "rounds.json"))
    restored = restarted.get(round_.round_id)
    assert restored is not None
    assert restored.phase is DrainPhase.INTERRUPTED
    assert restored.attempted_names == round_.attempted_names
    assert {pod.uid for pod in restored.evictable} == {
        pod.uid for pod in round_.evictable
    }

    # Resuming after the restart re-reads the cluster rather than trusting the
    # stored snapshot.
    sim.post("/api/sim/pods/exit-terminating")
    restarted.resume(round_.round_id)
    assert {pod.state for pod in restored.evictable} >= {PodState.GONE}
    restarted.recheck(round_.round_id)
    assert restored.offline is True


def test_replacement_scheduled_elsewhere_is_not_part_of_this_round(manager, sim):
    round_ = manager.start("node-a")
    manager.run(round_.round_id)
    old_uid = next(p.uid for p in round_.evictable if p.name == "web-0")

    # The controller recreates web-0 on a different node.
    sim.post("/api/sim/pods/shop/web-0/replace", json={"node": "node-c"})
    manager.recheck(round_.round_id)

    old_row = next(p for p in round_.pods.values() if p.uid == old_uid)
    assert old_row.state is PodState.GONE
    # This round only covers node-a, so the replacement on node-c must not
    # appear in it at all.
    assert {p.node for p in round_.pods.values()} == {"node-a"}
    assert "web-0" not in {p.name for p in round_.evictable if p.state is not PodState.GONE}


def test_concurrent_drains_share_one_budget(manager):
    """Two rounds driven from two threads still cannot exceed web-pdb.

    minAvailable=3 with five web pods leaves room for exactly two evictions.
    """
    import threading

    round_a = manager.start("node-a")
    round_b = manager.start("node-b")
    errors: list[BaseException] = []

    def drive(round_id: str) -> None:
        try:
            manager.run(round_id)
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [
        threading.Thread(target=drive, args=(round_a.round_id,)),
        threading.Thread(target=drive, args=(round_b.round_id,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)
    assert not errors

    admitted = [
        pod
        for round_ in (round_a, round_b)
        for pod in round_.evictable
        if pod.state is PodState.TERMINATING and _labels(round_, pod) == "web"
    ]
    assert len(admitted) <= 2
    blocked = [
        pod
        for round_ in (round_a, round_b)
        for pod in round_.blocked
        if _labels(round_, pod) == "web"
    ]
    assert blocked, "at least one web pod must have been refused"
