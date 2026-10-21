from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Mapping, Sequence


class Commands:
    def run(
        self, arguments: Sequence[str], *, cwd: Path, capture: bool = False,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = dict(os.environ)
        if env:
            environment.update(env)
        return subprocess.run(
            list(arguments), cwd=cwd, env=environment, check=True, text=True,
            capture_output=capture,
        )

    def output(self, arguments: Sequence[str], *, cwd: Path, env: Mapping[str, str] | None = None) -> str:
        return self.run(arguments, cwd=cwd, capture=True, env=env).stdout.strip()
