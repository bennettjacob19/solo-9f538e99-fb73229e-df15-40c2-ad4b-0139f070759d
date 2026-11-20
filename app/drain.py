"""Drain orchestration.

Rules, in order:

1. Cordon the node *before* listing anything.  Only once the node reports
   unschedulable are its pods read and offered for eviction.
2. DaemonSet, static/mirror and finished pods are never evicted.
3. Every eviction uses the ``policy/v1`` Eviction subresource with the pod UID
   as a precondition.  A same-named replacement is a different object: if it
   appears mid-round it is not evicted on the old decision; it is marked as
   deferred and picked up by the next round.
4. A PodDisruptionBudget refusal from the API server is not fatal.  The pod is
   flagged as blocked and the round continues with the rest.  Budget
   accounting lives in the API server, so two nodes drained in parallel cannot
   jointly exceed a shared budget.
5. Interrupting keeps the cordon and every eviction already issued.  Resuming
   re-reads the cluster instead of trusting the snapshot.
6. A node is reported as safe to power off only after a fresh check shows it
   is still unschedulable and carries no ordinary pods.
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone

from .kube import (
    EvictionBlocked,
    EvictionConflict,
    KubeClient,
    KubeError,
    pods_matching_budgets,
)
from .models import DrainPhase, PodState, PodStatus, Round
from .store import RoundStore


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DrainError(RuntimeError):
    pass


class DrainManager:
    """Owns the round table and the calls to the cluster.

    A single re-entrant lock guards the round dicts.  Network I/O happens
    outside the lock; templates use ``busy`` to explain a slow request.
    """

    def __init__(self, kube: KubeClient, store: RoundStore | None = None):
        self.kube = kube
        self.store = store
        self._rounds: dict[str, Round] = store.load() if store else {}
        self._lock = threading.RLock()

    def _persist(self) -> None:
        """Save the round table.  Must be called with the lock held."""
        if self.store is not None:
            self.store.save(self._rounds)

    # -- queries -----------------------------------------------------------
    def list_rounds(self) -> list[Round]:
        with self._lock:
            return sorted(self._rounds.values(), key=lambda r: r.created_at, reverse=True)

    def get(self, round_id: str) -> Round | None:
        with self._lock:
            return self._rounds.get(round_id)

    def active_round_for_node(self, node: str) -> Round | None:
        with self._lock:
            for rnd in self._rounds.values():
                if rnd.node == node and rnd.phase is not DrainPhase.COMPLETE:
                    return rnd
            return None

    def nodes(self):
        return self.kube.list_nodes()

    # -- round lifecycle ---------------------------------------------------
    def start(self, node: str) -> Round:
        """Cordon first, then snapshot.

        Cordoning before listing is the point: the pod list shown to the
        operator is guaranteed to come from a node that can no longer receive
        new pods.  If cordon fails, no round exists and nothing was offered.
        """
        with self._lock:
            if self.active_round_for_node(node) is not None:
                raise DrainError(f"节点 {node} 已有进行中的排空轮次")
            self.kube.cordon(node)
            node_info = self.kube.get_node(node)
            if node_info is None:
                raise DrainError(f"节点 {node} 不存在")
            if not node_info.unschedulable:
                raise DrainError(f"节点 {node} 封锁后仍报告可调度，已停止。")
            rnd = Round(
                node=node,
                round_id=uuid.uuid4().hex[:12],
                created_at=_now(),
                updated_at=_now(),
                message="节点已封锁，下面列出本轮要驱逐的 Pod。",
            )
            self._rounds[rnd.round_id] = rnd
            self._persist()
        self.refresh(rnd)
        return rnd

    def resume(self, round_id: str) -> Round:
        """Re-read the cluster and re-arm everything that is still retryable.

        Used both for "继续排空" after an interruption and for retrying after
        a budget recovers.  The cordon stays.  Pods that left stay gone; pods
        that appeared (including same-named replacements) are picked up as
        pending for this new pass.
        """
        rnd = self._require(round_id)
        with self._lock:
            if rnd.busy:
                raise DrainError("本轮正在处理中，请稍候")
            rnd.busy = True
        try:
            node = self.kube.get_node(rnd.node)
            if node is None:
                raise DrainError(f"节点 {rnd.node} 不存在")
            if not node.unschedulable:
                # Nobody may uncordon a node out from under a drain and have us
                # keep evicting onto it.
                self.kube.cordon(rnd.node)
            with self._lock:
                rnd.phase = DrainPhase.RUNNING
                rnd.resumed_from = rnd.updated_at
                rnd.attempted_names.clear()
                for status in rnd.pods.values():
                    if status.state in (PodState.BLOCKED, PodState.DEFERRED):
                        status.state = PodState.PENDING
                        status.message = "已重新读取集群，本轮重新尝试。"
                rnd.message = "已重新读取集群，继续处理本轮剩余 Pod。"
                rnd.updated_at = _now()
                self._persist()
            self.refresh(rnd)
        finally:
            with self._lock:
                rnd.busy = False
        return rnd

    def interrupt(self, round_id: str) -> Round:
        """Stop issuing evictions; keep the cordon and what already left."""
        rnd = self._require(round_id)
        with self._lock:
            if rnd.phase is not DrainPhase.RUNNING:
                return rnd
            rnd.phase = DrainPhase.INTERRUPTED
            rnd.message = (
                "已中断：封锁保留，已发生的驱逐保留。"
                "点“继续排空”会重新读取集群再继续。"
            )
            rnd.updated_at = _now()
            self._persist()
        return rnd

    # -- the main loop -----------------------------------------------------
    def run(self, round_id: str) -> Round:
        """Offer every pending ordinary pod once, then verify the node.

        Blocked and deferred pods never stop the pass: the loop moves on and
        leaves them for a later resume.  An accepted eviction leaves the pod
        in ``TERMINATING`` ("待退出") until a re-read shows the object gone.
        """
        rnd = self._require(round_id)
        with self._lock:
            if rnd.busy:
                raise DrainError("本轮正在处理中，请稍候")
            if rnd.phase is not DrainPhase.RUNNING:
                raise DrainError("本轮已中断，请先点击“继续排空”重新读取集群")
            rnd.busy = True
        try:
            self.refresh(rnd)
            for status in list(rnd.evictable):
                with self._lock:
                    if rnd.phase is not DrainPhase.RUNNING:
                        break
                    if status.state is not PodState.PENDING:
                        continue
                self._evict_one(rnd, status)
            self.refresh(rnd)
            self._settle(rnd)
            self._verify_offline(rnd)
        finally:
            with self._lock:
                rnd.busy = False
                rnd.updated_at = _now()
                self._persist()
        return rnd

    def _evict_one(self, rnd: Round, status: PodStatus) -> None:
        """Evict a single pod and record the outcome on its status row."""
        live = self.kube.get_pod(status.namespace, status.name)
        with self._lock:
            if live is None:
                status.state = PodState.GONE
                status.message = "Pod 已离开节点。"
                return
            if live.uid != status.uid:
                # Same name, different object.  The decision to evict was made
                # for the old UID and never carries over to the replacement.
                # The old row is closed out; refresh() will list the new object
                # separately (as DEFERRED when it landed on this node).
                rnd.attempted_names.add(status.key)
                status.state = PodState.GONE
                status.message = (
                    f"原 Pod 已被同名新对象替换（新 UID {live.uid[:8]}，"
                    f"位于 {live.node}）。"
                )
                rnd.updated_at = _now()
                return
            rnd.attempted_names.add(status.key)
            status.state = PodState.TERMINATING
            status.message = "驱逐请求已发出，等待 Pod 退出。"
            rnd.updated_at = _now()
        try:
            self.kube.evict(live)
        except EvictionBlocked as exc:
            with self._lock:
                status.state = PodState.BLOCKED
                status.message = f"受预算阻挡：{exc}"
        except EvictionConflict as exc:
            with self._lock:
                status.state = PodState.DEFERRED
                status.message = f"未驱逐：{exc}"
        except KubeError as exc:
            with self._lock:
                status.state = PodState.DEFERRED
                status.message = f"驱逐失败：{exc}"
        else:
            with self._lock:
                status.state = PodState.TERMINATING
                status.message = "已获准驱逐，等待 Pod 退出。"
        with self._lock:
            rnd.updated_at = _now()

    def _settle(self, rnd: Round) -> None:
        """Write the round-level message after a pass over the pods."""
        with self._lock:
            if rnd.phase is not DrainPhase.RUNNING:
                return
            counts = rnd.counts
            if counts["blocked"]:
                rnd.message = (
                    f"{counts['blocked']} 个 Pod 受 PodDisruptionBudget 阻挡，"
                    "本轮已继续处理其他 Pod；预算恢复后可点“继续排空”。"
                )
            elif counts["waiting"]:
                rnd.message = (
                    f"{counts['waiting']} 个 Pod 已获准驱逐但尚未退出（待退出）；"
                    "确认它们离开后点“重新检查”。"
                )
            elif counts["deferred"]:
                rnd.message = "有 Pod 留待续排，可点“继续排空”重新读取集群。"
            else:
                rnd.message = "本轮已无待处理的普通 Pod。"
            rnd.updated_at = _now()

    def recheck(self, round_id: str) -> Round:
        """Re-read the cluster and re-evaluate whether the node can go down.

        This never issues an eviction.  It is the action that moves a pod from
        "待退出" to "已离开" and the node to "可下线" once the cluster agrees.
        """
        rnd = self._require(round_id)
        with self._lock:
            if rnd.busy:
                raise DrainError("本轮正在处理中，请稍候")
            rnd.busy = True
        try:
            self.refresh(rnd)
            self._settle(rnd)
            self._verify_offline(rnd)
        finally:
            with self._lock:
                rnd.busy = False
                rnd.updated_at = _now()
                self._persist()
        return rnd

    def observe(self, round_id: str) -> None:
        """Reflect cluster reality when a page is opened.

        Read-only with respect to the cluster.  It keeps the display honest —
        a pod that left, a node that was uncordoned, or a replacement that
        appeared are all visible without pressing a button.  Errors are
        swallowed so a transient API failure does not break the page; the
        stored snapshot is then shown as-is.
        """
        rnd = self.get(round_id)
        if rnd is None:
            return
        with self._lock:
            if rnd.busy:
                return
            rnd.busy = True
        try:
            self.refresh(rnd)
            self._verify_offline(rnd)
        except KubeError:
            pass
        finally:
            with self._lock:
                rnd.busy = False

    # -- re-read -----------------------------------------------------------
    def refresh(self, rnd: Round) -> Round:
        """Reconcile the round snapshot with the live cluster.

        * Pod object no longer present -> ``GONE``.
        * Pod object still present keeps its state; a ``TERMINATING`` pod keeps
          showing "待退出".  Nothing is reverted to ``PENDING`` here, because
          re-issuing an eviction the server already accepted would be wrong.
        * New object (including a same-named replacement, new UID) enters as
          ``PENDING``, or as ``DEFERRED`` when this round already evicted that
          name — "同名替代者留待续排".
        """
        live = self.kube.list_pods_on_node(rnd.node)
        budgets = self.kube.list_budgets()
        by_uid = {pod.uid: pod for pod in live}
        with self._lock:
            for status in rnd.pods.values():
                pod = by_uid.get(status.uid)
                if pod is None:
                    if status.state is not PodState.GONE:
                        if status.state is PodState.TERMINATING:
                            status.message = "已退出节点。"
                        else:
                            status.message = status.message or "Pod 已离开节点。"
                        status.state = PodState.GONE
                    continue
                status.exempt_reason = pod.exempt_reason
                status.budgets = pods_matching_budgets(pod, budgets)
                if pod.terminating and status.state is PodState.PENDING:
                    # Deletion started elsewhere (for example a manual delete).
                    status.state = PodState.TERMINATING
                    status.message = "Pod 正在删除，等待退出。"
            for pod in live:
                if pod.uid in rnd.pods:
                    continue
                replacement = pod.key in rnd.attempted_names
                rnd.pods[pod.uid] = PodStatus(
                    name=pod.name,
                    namespace=pod.namespace,
                    uid=pod.uid,
                    node=pod.node,
                    state=PodState.DEFERRED if replacement else PodState.PENDING,
                    exempt_reason=pod.exempt_reason,
                    budgets=pods_matching_budgets(pod, budgets),
                    message=(
                        "同名替代者（新 UID），留待续排。"
                        if replacement
                        else ""
                    ),
                )
            rnd.updated_at = _now()
        return rnd

    def _verify_offline(self, rnd: Round) -> None:
        """Report the node safe to power off only after an independent check.

        Reads the node and its pods afresh.  The node must be unschedulable
        *now* and no ordinary pod may remain.  While any ordinary pod is still
        present — including one that was granted eviction but has not exited —
        the page must not claim the node is ready.
        """
        node = self.kube.get_node(rnd.node)
        live = self.kube.list_pods_on_node(rnd.node) if node is not None else []
        with self._lock:
            if node is None:
                rnd.offline = False
                rnd.offline_at = None
                rnd.offline_reason = "节点不存在，无法确认。"
                return
            if not node.unschedulable:
                rnd.offline = False
                rnd.offline_at = None
                rnd.offline_reason = "节点当前可调度；未重新封锁前不能下线。"
                if rnd.phase is DrainPhase.COMPLETE:
                    rnd.phase = DrainPhase.RUNNING
                return
            remaining = [pod for pod in live if pod.is_normal]
            if remaining:
                names = "、".join(f"{pod.namespace}/{pod.name}" for pod in remaining[:5])
                more = "" if len(remaining) <= 5 else f" 等 {len(remaining)} 个"
                rnd.offline = False
                rnd.offline_at = None
                rnd.offline_reason = f"节点仍不可调度，但尚有普通 Pod：{names}{more}。"
                if rnd.phase is DrainPhase.COMPLETE:
                    rnd.phase = DrainPhase.RUNNING
                return
            rnd.offline = True
            if not rnd.offline_at:
                rnd.offline_at = _now()
            rnd.offline_reason = "已确认：节点不可调度，且已无普通 Pod。"
            rnd.phase = DrainPhase.COMPLETE

    # -- helpers -----------------------------------------------------------
    def _require(self, round_id: str) -> Round:
        rnd = self.get(round_id)
        if rnd is None:
            raise DrainError(f"排空轮次 {round_id} 不存在")
        return rnd
