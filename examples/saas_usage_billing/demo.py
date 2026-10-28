"""Independent synthetic SaaS usage-to-invoice example using the same Spark/Delta pattern."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import functions as F

from construction_data_platform.pipeline import create_spark


def write_inputs(root: Path) -> None:
    landing = root / "landing"
    landing.mkdir(parents=True, exist_ok=True)
    usage = [
        {"event_id": "evt-001", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "120", "source_updated_at": (datetime.now(UTC) - timedelta(days=5)).isoformat()},
        {"event_id": "evt-002", "customer_id": "ACME-SYNTH", "period": "2026-10", "units": "80", "source_updated_at": (datetime.now(UTC) - timedelta(days=5)).isoformat()},
    ]
    invoices = [{"invoice_id": "inv-9001", "customer_id": "ACME-SYNTH", "period": "2026-10", "billed_units": "190", "unit_price": "2.50", "currency": "USD", "source_updated_at": datetime.now(UTC).isoformat()}]
    for name, rows in (("usage", usage), ("invoices", invoices)):
        with (landing / f"{name}_batch_001.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def run(root: Path) -> dict[str, object]:
    root = root.resolve()
    spark = create_spark("saas-usage-billing-demo")
    try:
        input_frames = {}
        freshness = []
        for feed in ("usage", "invoices"):
            paths = sorted((root / "landing").glob(f"{feed}_*.csv"))
            frames = [spark.read.option("header", True).option("inferSchema", False).csv(str(path)) for path in paths]
            frame = frames[0] if frames else None
            for extra in frames[1:]:
                frame = frame.unionByName(extra)
            if frame is not None:
                frame = frame.dropDuplicates(["event_id"] if feed == "usage" else ["invoice_id"])
                (root / "tables" / "bronze" / feed).mkdir(parents=True, exist_ok=True)
                frame.write.format("delta").mode("overwrite").save(str(root / "tables" / "bronze" / feed))
                input_frames[feed] = frame
                latest = frame.agg(F.max("source_updated_at")).first()[0]
                age_hours = (datetime.now(UTC) - datetime.fromisoformat(latest)).total_seconds() / 3600
                freshness.append({"feed": feed, "status": "STALE_OVER_72H" if age_hours > 72 else "RECEIVED", "files": len(paths), "rows": frame.count(), "latest_update": latest})
            else:
                freshness.append({"feed": feed, "status": "MISSING_EXPECTED_FILE", "files": 0, "rows": 0, "latest_update": None})
        usage_totals = input_frames["usage"].groupBy("customer_id", "period").agg(F.sum(F.col("units").cast("long")).alias("observed_units"))
        invoice = input_frames["invoices"].select("invoice_id", "customer_id", "period", F.col("billed_units").cast("long").alias("billed_units"), F.col("unit_price").cast("decimal(12,2)").alias("unit_price"), "currency")
        reconciliation = invoice.join(usage_totals, ["customer_id", "period"], "full_outer").withColumn("unit_difference", F.col("billed_units") - F.col("observed_units")).withColumn("expected_amount", (F.col("observed_units") * F.col("unit_price")).cast("decimal(14,2)")).withColumn("exception", F.when(F.col("billed_units").isNull(), "MISSING_INVOICE").when(F.col("observed_units").isNull(), "MISSING_USAGE").when(F.col("unit_difference") != 0, "BILLED_USAGE_MISMATCH"))
        result = [row.asDict(recursive=True) for row in reconciliation.collect()]
        generation = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        output = root / "tables" / "gold" / "generations" / generation
        output.mkdir(parents=True, exist_ok=True)
        reconciliation.write.format("delta").mode("overwrite").save(str(output / "usage_invoice_reconciliation"))
        pointer = {"generation": generation, "published_at_utc": datetime.now(UTC).isoformat(), "synthetic": True}
        pointer_path = root / "tables" / "gold" / "current_generation.json"
        pointer_path.parent.mkdir(parents=True, exist_ok=True)
        temp = pointer_path.with_suffix(".tmp")
        temp.write_text(json.dumps(pointer, indent=2) + "\n", encoding="utf-8")
        temp.replace(pointer_path)
        report = {"synthetic": True, "scenario": "saas-usage-to-invoice", "status": "SUCCESS", "published": True, **pointer, "publication_pointer": str(pointer_path), "source_freshness": freshness, "reconciliation": result}
        (root / "evidence").mkdir(parents=True, exist_ok=True)
        (root / "evidence" / "run.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        return report
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the independent synthetic SaaS usage-to-invoice example")
    parser.add_argument("--root", type=Path, default=Path("data/saas-demo"))
    args = parser.parse_args()
    write_inputs(args.root)
    print(json.dumps(run(args.root), indent=2, default=str))


if __name__ == "__main__":
    main()
