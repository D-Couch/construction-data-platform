"""Small command line interface for the synthetic Milestone 1 demo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .generator import generate
from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(prog="cdp", description="Synthetic construction purchasing/delivery/invoice demo")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="generate synthetic source feeds and a separate expected fixture")
    gen.add_argument("--root", type=Path, default=Path("data/demo"))
    gen.add_argument("--scenario", choices=["golden", "ticket-correction", "duplicate-replay", "invalid-unit"], default="golden")
    gen.add_argument("--seed", type=int, default=1729)
    run = sub.add_parser("run", help="ingest and transform landing files with Spark and Delta")
    run.add_argument("--root", type=Path, default=Path("data/demo"))
    demo = sub.add_parser("demo", help="generate and run a synthetic scenario")
    demo.add_argument("--root", type=Path, default=Path("data/demo"))
    demo.add_argument("--scenario", choices=["golden", "ticket-correction", "duplicate-replay", "invalid-unit"], default="golden")
    args = parser.parse_args()
    if args.command in {"generate", "demo"}:
        manifest = generate(args.root, args.scenario, getattr(args, "seed", 1729))
        print(json.dumps({"generated": True, "synthetic": True, "manifest": manifest}, indent=2))
    if args.command in {"run", "demo"}:
        print(json.dumps(run_pipeline(args.root), indent=2, default=str))


if __name__ == "__main__":
    main()
