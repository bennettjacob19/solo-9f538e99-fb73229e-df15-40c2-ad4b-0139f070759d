"""Optional on-disk persistence of drain rounds.

The cluster is the source of truth for the cordon and for which pods exist.
What the app must not lose across a restart is the *decision record*: which
node was cordoned, which round it belongs to, and which pods were already
offered for eviction.  Without that record an operator who restarts the page
mid-drain would have no way to resume and review what already happened.

Set ``DRAIN_STATE_FILE`` to enable persistence.  Writes are atomic
(temp file + replace) and best effort: a persistence failure never blocks a
drain, because the cluster state is authoritative either way.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .models import DrainPhase, PodBudget, PodState, PodStatus, Round


def round_to_dict(rnd: Round) -> dict:
    return {
        "node": rnd.node,
        "round_id": rnd.round_id,
        "phase": rnd.phase.value,
        "offline": rnd.offline,
        "offline_reason": rnd.offline_reason,
        "offline_at": rnd.offline_at,
        "message": rnd.message,
        "resumed_from": rnd.resumed_from,
        "created_at": rnd.created_at,
        "updated_at": rnd.updated_at,
        "attempted_names": sorted(rnd.attempted_names),
        "pods": [
            {
                "name": pod.name,
                "namespace": pod.namespace,
                "uid": pod.uid,
                "node": pod.node,
                "state": pod.state.value,
                "message": pod.message,
                "exempt_reason": pod.exempt_reason,
                "budgets": [budget.__dict__ for budget in pod.budgets],
            }
            for pod in rnd.pods.values()
        ],
    }


def round_from_dict(data: dict) -> Round:
    pods: dict[str, PodStatus] = {}
    for item in data.get("pods", []):
        pods[item["uid"]] = PodStatus(
            name=item["name"],
            namespace=item["namespace"],
            uid=item["uid"],
            node=item["node"],
            state=PodState(item["state"]),
            message=item.get("message", ""),
            exempt_reason=item.get("exempt_reason"),
            budgets=[PodBudget(**budget) for budget in item.get("budgets", [])],
        )
    return Round(
        node=data["node"],
        round_id=data["round_id"],
        pods=pods,
        attempted_names=set(data.get("attempted_names", [])),
        phase=DrainPhase(data["phase"]),
        offline=bool(data.get("offline", False)),
        offline_reason=data.get("offline_reason", ""),
        offline_at=data.get("offline_at"),
        message=data.get("message", ""),
        resumed_from=data.get("resumed_from"),
        created_at=data.get("created_at", ""),
        updated_at=data.get("updated_at", ""),
    )


class RoundStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)

    @classmethod
    def from_env(cls) -> "RoundStore | None":
        path = os.environ.get("DRAIN_STATE_FILE")
        return cls(path) if path else None

    def load(self) -> dict[str, Round]:
        try:
            raw = self.path.read_text()
        except FileNotFoundError:
            return {}
        except OSError:
            return {}
        try:
            data = json.loads(raw)
        except ValueError:
            return {}
        rounds: dict[str, Round] = {}
        for item in data.get("rounds", []):
            try:
                rnd = round_from_dict(item)
            except (KeyError, ValueError):
                continue
            # A round marked busy was interrupted mid-request; it can never
            # still be busy in this new process.
            rnd.busy = False
            rounds[rnd.round_id] = rnd
        return rounds

    def save(self, rounds: dict[str, Round]) -> None:
        payload = {"version": 1, "rounds": [round_to_dict(r) for r in rounds.values()]}
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".rounds-")
            with os.fdopen(fd, "w") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)
        except OSError:
            # Persistence is a convenience; the cluster remains authoritative.
            pass
