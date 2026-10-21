from __future__ import annotations

import json
import unittest
from pathlib import Path

from taxi_reference.canonical import canonical_bytes, with_output_id


class CanonicalContractTest(unittest.TestCase):
    def test_shared_golden_vectors(self) -> None:
        path = self._contract_path("canonical-test-vectors.jsonl")
        for line in path.read_text(encoding="utf-8").split("\n"):
            if not line:
                continue
            vector = json.loads(line)
            with self.subTest(vector=vector["name"]):
                self.assertEqual(
                    vector["expected_canonical_line"].encode("utf-8"),
                    canonical_bytes(vector["record"]),
                )
                self.assertEqual(
                    vector["expected_output_id"],
                    with_output_id(vector["record"])["output_id"],
                )

    @staticmethod
    def _contract_path(name: str) -> Path:
        for parent in Path(__file__).resolve().parents:
            candidate = parent / "schemas" / name
            if candidate.is_file():
                return candidate
        mounted = Path("/contracts") / name
        if mounted.is_file():
            return mounted
        raise FileNotFoundError(f"cannot locate shared contract vector {name}")


if __name__ == "__main__":
    unittest.main()
