"""Measure allocation join cardinality before/after pre-aggregation on synthetic rows."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import tempfile
import time
from pathlib import Path

from pyspark.sql import functions as F

from construction_data_platform.pipeline import create_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lines", type=int, default=10_000)
    parser.add_argument("--allocations-per-delivery", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if min(args.lines, args.allocations_per_delivery, args.repetitions) < 1:
        parser.error("all benchmark dimensions must be positive")
    lines = args.lines
    allocations_per_delivery = args.allocations_per_delivery
    repetitions = args.repetitions
    with tempfile.TemporaryDirectory(prefix="cdp-allocation-bench-") as temporary:
        root = Path(temporary)
        allocation_path = root / "allocations.csv"
        with allocation_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(["line_id", "delivery_id", "allocated_quantity"])
            for line_number in range(lines):
                line_id = f"L{line_number:06d}"
                delivery_id = f"D{line_number:06d}"
                for _ in range(allocations_per_delivery):
                    writer.writerow([line_id, delivery_id, f"{1 / allocations_per_delivery:.6f}"])
        spark = create_spark("construction-data-platform-allocation-benchmark")
        try:
            allocations = spark.read.option("header", True).csv(str(allocation_path)).withColumn(
                "allocated_quantity", F.col("allocated_quantity").cast("decimal(18,6)")
            )
            line_dimension = spark.createDataFrame(
                [(f"L{i:06d}",) for i in range(lines)], ["line_id"]
            )
            delivery_fact = spark.createDataFrame(
                [(f"D{i:06d}", 1.0) for i in range(lines)], ["delivery_id", "accepted_quantity"]
            )

            def before_plan():
                return (
                    allocations.join(line_dimension, "line_id")
                    .join(delivery_fact, "delivery_id")
                    .groupBy("line_id", "delivery_id")
                    .agg(F.sum("allocated_quantity").alias("allocated_quantity"), F.max("accepted_quantity").alias("accepted_quantity"))
                )

            def after_plan():
                return (
                    allocations.groupBy("line_id", "delivery_id")
                    .agg(F.sum("allocated_quantity").alias("allocated_quantity"))
                    .join(line_dimension, "line_id")
                    .join(delivery_fact, "delivery_id")
                    .groupBy("line_id", "delivery_id")
                    .agg(F.sum("allocated_quantity").alias("allocated_quantity"), F.max("accepted_quantity").alias("accepted_quantity"))
                )

            def summarize(plan):
                row = plan().agg(
                    F.count("*").alias("rows"),
                    F.sum("allocated_quantity").alias("allocated_quantity"),
                    F.sum("accepted_quantity").alias("accepted_quantity"),
                ).first()
                return {
                    "rows": row["rows"],
                    "allocated_quantity": str(row["allocated_quantity"]),
                    "accepted_quantity": str(row["accepted_quantity"]),
                }

            first_execution_ms = {}
            outputs = {}
            for label, plan in (("before", before_plan), ("after", after_plan)):
                started = time.perf_counter()
                outputs[label] = summarize(plan)
                first_execution_ms[label] = round((time.perf_counter() - started) * 1000, 2)

            allocations = allocations.cache()
            line_dimension = line_dimension.cache()
            delivery_fact = delivery_fact.cache()
            allocations.count()
            line_dimension.count()
            delivery_fact.count()
            timings = {"before": [], "after": []}
            warm_order = []
            for repetition in range(repetitions):
                ordered_plans = (("before", before_plan), ("after", after_plan))
                if repetition % 2:
                    ordered_plans = tuple(reversed(ordered_plans))
                warm_order.append([label for label, _ in ordered_plans])
                for label, plan in ordered_plans:
                    started = time.perf_counter()
                    summarize(plan)
                    timings[label].append(round((time.perf_counter() - started) * 1000, 2))
            assert outputs["before"] == outputs["after"]
            output_path = root / "optimized-result"
            after_plan().write.mode("overwrite").parquet(str(output_path))
            output_bytes = sum(path.stat().st_size for path in output_path.rglob("*") if path.is_file())
            print(json.dumps({
                "synthetic": True,
                "benchmark": "allocation join pre-aggregation",
                "platform": "local Spark + Delta project environment; no Delta writes in timed section",
                "spark_version": spark.version,
                "spark_master": spark.sparkContext.master,
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                "lines": lines,
                "allocations_per_delivery": allocations_per_delivery,
                "allocation_rows": lines * allocations_per_delivery,
                "join_rows_before_preaggregation": lines * allocations_per_delivery,
                "join_rows_after_preaggregation": lines,
                "join_input_reduction_factor": allocations_per_delivery,
                "allocation_input_bytes": allocation_path.stat().st_size,
                "output_bytes_after_plan": output_bytes,
                "repetitions": repetitions,
                "outputs_equal": outputs["before"] == outputs["after"],
                "output": outputs["after"],
                "first_execution_elapsed_ms": first_execution_ms,
                "first_execution_order": ["before", "after"],
                "elapsed_ms": timings,
                "warmed_repetition_order": warm_order,
                "median_elapsed_ms": {key: round(statistics.median(value), 2) for key, value in timings.items()},
                "note": "Synthetic local microbenchmark; timings are resource proxies, not Databricks or production performance claims.",
            }, indent=2))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
