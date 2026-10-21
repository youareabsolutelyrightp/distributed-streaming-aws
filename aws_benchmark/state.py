from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PHASES = (
    "preflight", "release", "foundation", "dataset", "deploy", "readiness",
    "benchmark", "evidence", "analysis", "summary", "cleanup", "audit",
)


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class RunState:
    path: Path
    run_id: str
    phase: str = "preflight"
    completed: list[str] = field(default_factory=list)
    status: str = "active"
    details: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(cls, root: Path, run_id: str) -> "RunState":
        path = root / ".runs" / run_id / "state.json"
        if path.exists():
            raise ValueError(f"Run state already exists: {path}")
        state = cls(path=path, run_id=run_id)
        state.save()
        return state

    @classmethod
    def load(cls, root: Path, run_id: str) -> "RunState":
        path = root / ".runs" / run_id / "state.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            path=path, run_id=value["run_id"], phase=value["phase"],
            completed=list(value["completed"]), status=value["status"],
            details=dict(value.get("details", {})),
        )

    def begin(self, phase: str) -> None:
        if phase not in PHASES:
            raise ValueError(f"Unknown lifecycle phase: {phase}")
        self.phase = phase
        self.details["phase_started_monotonic"] = time.monotonic()
        self.details["updated_at"] = now()
        self.save()
        print(f"PHASE run={self.run_id} name={phase} status=started", flush=True)

    def complete(self, phase: str, **details: Any) -> None:
        if phase not in self.completed:
            self.completed.append(phase)
        self.phase = phase
        self.details.update(details)
        self.details["updated_at"] = now()
        started = self.details.pop("phase_started_monotonic", None)
        elapsed = max(0.0, time.monotonic() - started) if isinstance(started, float) else None
        self.save()
        suffix = f" elapsed={elapsed:.1f}s" if elapsed is not None else ""
        print(f"PHASE run={self.run_id} name={phase} status=complete{suffix}", flush=True)

    def finish(self, status: str, **details: Any) -> None:
        self.status = status
        self.details.update(details)
        self.details["finished_at"] = now()
        self.save()

    def reset_after_cleanup(self) -> None:
        if "cleanup" not in self.completed or self.status == "completed":
            return
        self.completed = [phase for phase in self.completed if phase in {"preflight", "release"}]
        self.phase = self.completed[-1] if self.completed else "preflight"
        self.status = "active"
        self.details = {
            key: self.details[key]
            for key in ("profile_sha256", "release_sha256")
            if key in self.details
        }
        self.details["resumed_after_cleanup_at"] = now()
        self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1, "run_id": self.run_id, "phase": self.phase,
            "completed": self.completed, "status": self.status, "details": self.details,
        }
        descriptor, temporary = tempfile.mkstemp(prefix="state.", dir=self.path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as target:
                json.dump(payload, target, indent=2, sort_keys=True)
                target.write("\n")
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
