from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    benchmark_path = Path(__file__).parents[1] / "benchmarks" / "session_token_savings.json"
    benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
    recorded = sum(benchmark["recorded_tokens"])
    replay = len(benchmark["recorded_tokens"]) * benchmark["modeled_budget_tokens_per_job"]
    avoided = recorded - replay
    reduction = avoided / recorded * 100
    print(f"recorded={recorded}")
    print(f"modeled_replay={replay}")
    print(f"avoided={avoided}")
    print(f"reduction={reduction:.1f}%")


if __name__ == "__main__":
    main()
