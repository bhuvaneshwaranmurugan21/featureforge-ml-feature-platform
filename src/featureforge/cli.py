from __future__ import annotations

import argparse
import json
from pathlib import Path

from featureforge.simulator import run_failure_lab


def main() -> None:
    parser = argparse.ArgumentParser(prog="featureforge")
    commands = parser.add_subparsers(dest="command", required=True)
    simulate = commands.add_parser("simulate", help="run the deterministic failure lab")
    simulate.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "simulate":
        result = run_failure_lab()
        rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
        else:
            print(rendered, end="")
        if result["result"] != "PASS":
            raise SystemExit(1)


if __name__ == "__main__":
    main()
