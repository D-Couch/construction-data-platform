"""Lakeflow Jobs entrypoint. The target volume contains synthetic files and Delta paths."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from construction_data_platform.generator import generate
from construction_data_platform.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    generate(args.root, "golden")
    # Serverless Jobs use Spark Connect; the workspace-provided session is used
    # directly and the local Spark/Delta builder is reserved for development.
    result = run_pipeline(args.root, spark=SparkSession.builder.getOrCreate())
    print(json.dumps(result, indent=2, default=str))
    if result["status"] != "SUCCESS":
        raise RuntimeError(f"Synthetic demo job did not publish successfully: {result['status']}")


if __name__ == "__main__":
    main()
