from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from construction_data_platform.generator import generate
from construction_data_platform.pipeline import run_pipeline


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from construction_data_platform.pipeline import create_spark

    session = create_spark("construction-data-platform-tests")
    yield session
    session.stop()


def test_clean_replay_and_interrupted_publication(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "clean"
    generate(root, "golden")
    first = run_pipeline(root, spark)
    oracle = json.loads((root / "expected" / "golden_oracle.json").read_text())["expected"]
    assert str(first["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    assert float(first["daily_delivered"][0]["accepted_net_quantity"]) == float(oracle["accepted_metric_tonnes_by_project_material_date"][0]["accepted_net_quantity"])
    assert first["product_rows"]["invoice_exceptions"] == oracle["invoice_exception_count"]
    pointer_path = root / "tables" / "gold" / "current_generation.json"
    prior_generation = json.loads(pointer_path.read_text())["generation"]

    replay = run_pipeline(root, spark)
    assert str(replay["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    assert replay["duplicate_business_envelopes_suppressed"] >= 6

    renamed = root / "landing" / "scale_tickets_duplicate_new_name.csv"
    shutil.copy2(root / "landing" / "scale_tickets_batch_001.csv", renamed)
    duplicate_file = run_pipeline(root, spark)
    assert str(duplicate_file["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    assert duplicate_file["duplicate_business_envelopes_suppressed"] >= 1

    failed = run_pipeline(root, spark, fail_after_product="daily_delivered_quantities")
    assert failed["published"] is False
    assert json.loads(pointer_path.read_text())["generation"] == duplicate_file["generation"]
    assert prior_generation != failed["generation"]


def test_ticket_correction_replaces_quantity(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "correction"
    generate(root, "golden")
    before = run_pipeline(root, spark)
    before_generation = before["generation"]
    correction = root / "landing" / "scale_tickets_batch_002.csv"
    generate(root, "ticket-correction")
    after = run_pipeline(root, spark)
    assert str(after["daily_delivered"][0]["accepted_net_quantity"]) == "9.000000"
    assert after["product_rows"]["invoice_exceptions"] == 1
    pointer = json.loads((root / "tables" / "gold" / "current_generation.json").read_text())
    assert pointer["generation"] == after["generation"] != before_generation
    prior = spark.read.format("delta").load(str(root / "tables" / "gold" / "generations" / before_generation / "daily_delivered_quantities")).first()
    assert str(prior["accepted_net_quantity"]) == "10.000000"
    assert correction.exists()


def test_unknown_unit_is_quarantined_and_valid_rows_continue(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "invalid"
    generate(root, "invalid-unit")
    feed = root / "landing" / "scale_tickets_batch_001.csv"
    with feed.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0].update(weight_unit="kg", gross_weight="12000", tare_weight="2000")
    with feed.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    result = run_pipeline(root, spark)
    assert str(result["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    assert result["quarantine_rows"] == 1
    quarantined = spark.read.format("delta").load(str(root / "tables" / "silver" / "quarantine"))
    assert quarantined.first()["quarantine_reason"] == "UNKNOWN_WEIGHT_UNIT"


def test_invoice_waits_for_late_purchase_order_then_resolves(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "late-po"
    generate(root, "golden")
    po_file = root / "landing" / "purchase_order_lines_batch_001.csv"
    held_po = tmp_path / "purchase_order_lines_batch_001.csv"
    shutil.move(po_file, held_po)
    pending = run_pipeline(root, spark)
    assert pending["product_rows"]["invoice_exceptions"] == 1
    assert pending["invoice_exceptions"][0]["exception_reason"] == "PENDING_SUPPORTING_DELIVERY"
    shutil.move(held_po, po_file)
    resolved = run_pipeline(root, spark)
    assert resolved["daily_delivered"][0]["accepted_net_quantity"] == 10
    assert resolved["product_rows"]["invoice_exceptions"] == 0
