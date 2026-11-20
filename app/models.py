"""Domain model shared by the drain manager, the web UI and the simulator."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PodState(str, Enum):
    PENDING = "pending"
    TERMINATING = "terminating"
    GONE = "gone"
    BLOCKED = "blocked"
    DEFERRED = "deferred"

    @property
    def label(self) -> str:
        return {
            PodState.PENDING: "待驱逐",
            PodState.TERMINATING: "待退出",
            PodState.GONE: "已离开",
            PodState.BLOCKED: "受预算阻挡",
            PodState.DEFERRED: "留待续排",
        }[self]

    @property
    def tone(self) -> str:
        return {
            PodState.PENDING: "neutral",
            PodState.TERMINATING: "warn",
            PodState.GONE: "ok",
            PodState.BLOCKED: "danger",
            PodState.DEFERRED: "info",
        }[self]


class DrainPhase(str, Enum):
    RUNNING = "running"
    INTERRUPTED = "interrupted"
    COMPLETE = "complete"

    @property
    def label(self) -> str:
        return {
            DrainPhase.RUNNING: "排空中",
            DrainPhase.INTERRUPTED: "已中断",
            DrainPhase.COMPLETE: "已确认可下线",
        }[self]


@dataclass
class Pod:
    name: str
    namespace: str
    uid: str
    node: str
    phase: str = ""
    terminating: bool = False
    daemonset: bool = False
    static: bool = False
    owner_kind: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    deletion_timestamp: str | None = None

    @property
    def exempt_reason(self) -> str | None:
        """Pods a node drain must leave in place.

        DaemonSet pods mirror the node itself, static/mirror pods are owned by
        the kubelet, and pods that already reached a terminal phase cannot be
        rescheduled anyway.  None of them may be evicted or keep a drained node
        from going offline.
        """
        if self.daemonset:
            return "DaemonSet"
        if self.static:
            return "静态 Pod"
        if self.phase in ("Succeeded", "Failed"):
            return "已结束"
        return None

    @property
    def key(self) -> str:
        return f"{self.namespace}/{self.name}"

    @property
    def is_normal(self) -> bool:
        return self.exempt_reason is None


@dataclass
class PodBudget:
    name: str
    namespace: str
    allowed: int
    healthy: int
    desired: int
    expected: int
    stale: bool = False

    @property
    def summary(self) -> str:
        text = (
            f"允许中断 {self.allowed}，健康 {self.healthy}/{self.desired}，"
            f"共 {self.expected} 个"
        )
        return text + ("（状态待同步）" if self.stale else "")


@dataclass
class NodeInfo:
    name: str
    unschedulable: bool
    ready: bool
    pod_count: int = 0


@dataclass
class PodStatus:
    """Live status of one Pod object (identified by UID) in a drain round."""

    name: str
    namespace: str
    uid: str
    node: str
    state: PodState = PodState.PENDING
    message: str = ""
    exempt_reason: str | None = None
    budgets: list[PodBudget] = field(default_factory=list)
    replacement_of: str | None = None

    @property
    def key(self) -> str:
        return f"{self.namespace}/{self.name}"

    @property
    def display_name(self) -> str:
        return f"{self.namespace}/{self.name}"

    @property
    def short_uid(self) -> str:
        return self.uid[:8]

    @property
    def waiting_exit(self) -> bool:
        """Eviction granted (or deletion started) but the pod object remains."""
        return self.state is PodState.TERMINATING


@dataclass
class Round:
    """One drain operation for one node.

    Only the cordon, the snapshot and the decisions are kept between requests
    (and, when ``DRAIN_STATE_FILE`` is set, between processes).  Pod existence
    is always re-read from the cluster, so neither an interruption nor a
    restart can leave the page reporting stale pods.
    """

    node: str
    round_id: str
    pods: dict[str, PodStatus] = field(default_factory=dict)
    attempted_names: set[str] = field(default_factory=set)
    phase: DrainPhase = DrainPhase.RUNNING
    busy: bool = False
    offline: bool = False
    offline_reason: str = ""
    offline_at: str | None = None
    message: str = ""
    resumed_from: str | None = None
    created_at: str = ""
    updated_at: str = ""

    # -- views used by the templates --------------------------------------
    @property
    def evictable(self) -> list[PodStatus]:
        return sorted(
            (p for p in self.pods.values() if not p.exempt_reason),
            key=lambda p: (p.state is PodState.GONE, p.namespace, p.name),
        )

    @property
    def exempt(self) -> list[PodStatus]:
        return sorted(
            (p for p in self.pods.values() if p.exempt_reason),
            key=lambda p: (p.exempt_reason, p.namespace, p.name),
        )

    def by_state(self, state: PodState) -> list[PodStatus]:
        return [p for p in self.evictable if p.state is state]

    @property
    def blocked(self) -> list[PodStatus]:
        return self.by_state(PodState.BLOCKED)

    @property
    def deferred(self) -> list[PodStatus]:
        return self.by_state(PodState.DEFERRED)

    @property
    def waiting_exit(self) -> list[PodStatus]:
        return self.by_state(PodState.TERMINATING)

    @property
    def budgets(self) -> list[PodBudget]:
        seen: dict[tuple[str, str], PodBudget] = {}
        for status in self.evictable:
            for budget in status.budgets:
                seen.setdefault((budget.namespace, budget.name), budget)
        return sorted(seen.values(), key=lambda b: (b.namespace, b.name))

    @property
    def can_interrupt(self) -> bool:
        return self.phase is DrainPhase.RUNNING and not self.busy

    @property
    def is_offline_ready(self) -> bool:
        return self.phase is DrainPhase.COMPLETE and self.offline

    @property
    def can_resume(self) -> bool:
        # Resume is also the retry path after a budget recovers, so it is
        # offered whenever the round is not busy.
        return not self.busy

    @property
    def can_run(self) -> bool:
        return self.phase is DrainPhase.RUNNING and not self.busy

    @property
    def has_work_left(self) -> bool:
        return bool(self.blocked or self.deferred or self.waiting_exit)

    @property
    def counts(self) -> dict[str, int]:
        return {
            "total": len(self.evictable),
            "pending": len(self.by_state(PodState.PENDING)),
            "waiting": len(self.waiting_exit),
            "gone": len(self.by_state(PodState.GONE)),
            "blocked": len(self.blocked),
            "deferred": len(self.deferred),
            "exempt": len(self.exempt),
        }
