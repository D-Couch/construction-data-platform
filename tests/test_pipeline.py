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

    recovered = run_pipeline(root, spark)
    assert recovered["published"] is True
    assert recovered["generation"] != failed["generation"]
    assert str(recovered["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    assert json.loads(pointer_path.read_text())["generation"] == recovered["generation"]


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


def test_ticket_cancellation_removes_delivery_and_flags_invoice(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "cancelled-ticket"
    generate(root, "golden")
    ticket_file = root / "landing" / "scale_tickets_batch_001.csv"
    with ticket_file.open(newline="", encoding="utf-8") as stream:
        tickets = list(csv.DictReader(stream))
    tickets[0]["status"] = "cancelled"
    tickets[0]["source_revision"] = "2"
    tickets[0]["source_updated_at"] = "2026-10-21T08:00:00Z"
    with ticket_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(tickets[0]))
        writer.writeheader()
        writer.writerows(tickets)

    result = run_pipeline(root, spark)
    assert result["daily_delivered"] == []
    assert result["product_rows"]["invoice_exceptions"] == 1
    assert result["invoice_exceptions"][0]["exception_reason"] == "CANCELLED_DELIVERY"


def test_linked_credit_is_signed_once_and_does_not_need_delivery_allocation(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "linked-credit"
    generate(root, "golden")
    line_file = root / "landing" / "invoice_lines_batch_001.csv"
    with line_file.open(newline="", encoding="utf-8") as stream:
        lines = list(csv.DictReader(stream))
    credit = dict(
        lines[0],
        source_record_id="SUP-1:INV-CREDIT-1:CR1",
        invoice_id="INV-CREDIT-1",
        invoice_line_id="CR1",
        original_invoice_line_ref="SUP-1:INV-5001:L1",
        document_type="credit",
        billed_quantity="2.000",
        line_amount="25.00",
    )
    with line_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(lines[0]))
        writer.writeheader()
        writer.writerows([*lines, credit])

    invoice_file = root / "landing" / "invoices_batch_001.csv"
    with invoice_file.open(newline="", encoding="utf-8") as stream:
        invoices = list(csv.DictReader(stream))
    invoices.append(dict(invoices[0], source_record_id="SUP-1:INV-CREDIT-1", invoice_id="INV-CREDIT-1", invoice_number="5001-C", normalized_invoice_number="5001-C"))
    with invoice_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(invoices[0]))
        writer.writeheader()
        writer.writerows(invoices)

    result = run_pipeline(root, spark)
    assert result["product_rows"]["invoice_exceptions"] == 0
    net = spark.read.format("delta").load(str(root / "tables" / "gold" / "generations" / result["generation"] / "invoice_net_amounts")).first()
    assert str(net["net_invoice_amount"]) == "100.00"


def test_unlinked_credit_is_exception(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "unlinked-credit"
    generate(root, "golden")
    line_file = root / "landing" / "invoice_lines_batch_001.csv"
    with line_file.open(newline="", encoding="utf-8") as stream:
        lines = list(csv.DictReader(stream))
    credit = dict(lines[0], source_record_id="SUP-1:INV-CREDIT-1:CR1", invoice_id="INV-CREDIT-1", invoice_line_id="CR1", original_invoice_line_ref="", document_type="credit")
    lines.append(credit)
    with line_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(lines[0]))
        writer.writeheader()
        writer.writerows(lines)
    invoice_file = root / "landing" / "invoices_batch_001.csv"
    with invoice_file.open(newline="", encoding="utf-8") as stream:
        invoices = list(csv.DictReader(stream))
    invoices.append(dict(invoices[0], source_record_id="SUP-1:INV-CREDIT-1", invoice_id="INV-CREDIT-1", invoice_number="5001-C", normalized_invoice_number="5001-C"))
    with invoice_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(invoices[0]))
        writer.writeheader()
        writer.writerows(invoices)

    result = run_pipeline(root, spark)
    assert result["product_rows"]["invoice_exceptions"] == 1
    assert result["invoice_exceptions"][0]["exception_reason"] == "CREDIT_MISSING_ORIGINAL_REFERENCE"


def test_out_of_order_revision_repair_and_additive_schema_field(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "revision-repair"
    generate(root, "golden")
    ticket_file = root / "landing" / "scale_tickets_batch_001.csv"
    with ticket_file.open(newline="", encoding="utf-8") as stream:
        tickets = list(csv.DictReader(stream))
    old = dict(tickets[0], source_revision="1", gross_weight="500", tare_weight="700", extra_source_note="retained")
    latest = dict(tickets[0], source_revision="2", source_updated_at="2026-10-21T08:00:00Z", gross_weight="11", tare_weight="2", extra_source_note="retained")
    with ticket_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(old))
        writer.writeheader()
        writer.writerows([latest, old])

    result = run_pipeline(root, spark)
    assert str(result["daily_delivered"][0]["accepted_net_quantity"]) == "9.000000"
    assert result["quarantine_rows"] == 0
    history = spark.read.format("delta").load(str(root / "tables" / "silver" / "scale_tickets_history"))
    assert history.count() == 2
    assert "extra_source_note" in history.columns


def test_unsupported_schema_version_is_quarantined_without_publication_loss(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "schema-version"
    generate(root, "golden")
    first = run_pipeline(root, spark)
    ticket_file = root / "landing" / "scale_tickets_batch_001.csv"
    with ticket_file.open(newline="", encoding="utf-8") as stream:
        tickets = list(csv.DictReader(stream))
    tickets[0]["schema_version"] = "2.0"
    tickets[0]["source_revision"] = "2"
    tickets[0]["source_updated_at"] = "2026-10-21T08:00:00Z"
    with ticket_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(tickets[0]))
        writer.writeheader()
        writer.writerows(tickets)

    result = run_pipeline(root, spark)
    assert result["quarantine_rows"] == 1
    assert str(result["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    quarantine = spark.read.format("delta").load(str(root / "tables" / "silver" / "quarantine"))
    assert quarantine.first()["quarantine_reason"] == "UNSUPPORTED_SCHEMA_VERSION"
    assert result["generation"] != first["generation"]


def test_missing_expected_feed_is_visible_in_published_freshness(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "missing-feed"
    generate(root, "golden")
    (root / "landing" / "invoices_batch_001.csv").unlink()
    po_file = root / "landing" / "purchase_order_lines_batch_001.csv"
    with po_file.open(newline="", encoding="utf-8") as stream:
        purchase_orders = list(csv.DictReader(stream))
    purchase_orders[0]["source_updated_at"] = "2026-10-15T12:00:00Z"
    with po_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(purchase_orders[0]))
        writer.writeheader()
        writer.writerows(purchase_orders)
    result = run_pipeline(root, spark)
    freshness = spark.read.format("delta").load(str(root / "tables/gold/generations" / result["generation"] / "source_freshness"))
    invoice_feed = freshness.filter("source_feed = 'invoices'").first()
    po_feed = freshness.filter("source_feed = 'purchase_order_lines'").first()
    assert invoice_feed["freshness_status"] == "MISSING_EXPECTED_FILE"
    assert invoice_feed["files_received_this_run"] == 0
    assert po_feed["freshness_status"] == "STALE_SOURCE_UPDATE_OVER_72H"


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
    repaired = dict(rows[0], source_revision="2", source_updated_at="2026-10-21T08:00:00Z", weight_unit="metric_tonne", gross_weight="12", tare_weight="2")
    with feed.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows([rows[0], repaired])
    invalid_file = root / "landing" / "scale_tickets_invalid.csv"
    with invalid_file.open(newline="", encoding="utf-8") as stream:
        invalid_rows = list(csv.DictReader(stream))
    invalid_repair = dict(invalid_rows[0], source_revision="2", source_updated_at="2026-10-21T08:00:00Z", weight_unit="metric_tonne", gross_weight="2", tare_weight="0")
    with invalid_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(invalid_rows[0]))
        writer.writeheader()
        writer.writerows([invalid_rows[0], invalid_repair])
    reprocessed = run_pipeline(root, spark)
    assert reprocessed["quarantine_rows"] == 0
    assert str(reprocessed["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    raw = spark.read.format("delta").load(str(root / "tables" / "bronze" / "scale_tickets"))
    assert raw.count() >= 2


def test_invoice_waits_for_late_purchase_order_then_resolves(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "late-po"
    generate(root, "late-po")
    pending = run_pipeline(root, spark)
    assert pending["product_rows"]["invoice_exceptions"] == 1
    assert pending["invoice_exceptions"][0]["exception_reason"] == "PENDING_SUPPORTING_DELIVERY"
    deferred_po = root / "deferred" / "purchase_order_lines_late_arrival.csv"
    shutil.move(deferred_po, root / "landing" / "purchase_order_lines_late_arrival.csv")
    resolved = run_pipeline(root, spark)
    assert resolved["daily_delivered"][0]["accepted_net_quantity"] == 10
    assert resolved["product_rows"]["invoice_exceptions"] == 0


def test_late_receipt_updates_historical_delivery_date(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "late-receipt"
    generate(root, "late-receipt")

    before_receipt = run_pipeline(root, spark)
    assert before_receipt["daily_delivered"] == []
    assert before_receipt["invoice_exceptions"][0]["exception_reason"] == "PENDING_SUPPORTING_DELIVERY"

    deferred_receipt = root / "deferred" / "site_receipts_late_arrival.csv"
    shutil.move(deferred_receipt, root / "landing" / "site_receipts_late_arrival.csv")
    after_receipt = run_pipeline(root, spark)
    assert str(after_receipt["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    assert after_receipt["daily_delivered"][0]["business_date"].isoformat() == "2026-10-20"
    assert after_receipt["product_rows"]["invoice_exceptions"] == 0


def test_line_amount_mismatch_is_an_invoice_exception(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "bad-amount"
    generate(root, "golden")
    line_file = root / "landing" / "invoice_lines_batch_001.csv"
    with line_file.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0]["line_amount"] = "124.98"
    with line_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    result = run_pipeline(root, spark)
    assert result["product_rows"]["invoice_exceptions"] == 1
    assert result["invoice_exceptions"][0]["exception_reason"] == "LINE_AMOUNT_MISMATCH"


def test_short_ton_conversion_is_decimal_and_deterministic(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "short-ton"
    generate(root, "golden")
    ticket_file = root / "landing" / "scale_tickets_batch_001.csv"
    with ticket_file.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows[0].update(weight_unit="us_short_ton", gross_weight="13.023113", tare_weight="2.000000")
    with ticket_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    result = run_pipeline(root, spark)
    assert str(result["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"


def test_invoice_price_uses_the_po_revision_effective_at_delivery(tmp_path: Path, spark: SparkSession) -> None:
    mismatch_root = tmp_path / "price-mismatch"
    generate(mismatch_root, "golden")
    po_file = mismatch_root / "landing" / "purchase_order_lines_batch_001.csv"
    with po_file.open(newline="", encoding="utf-8") as stream:
        po_rows = list(csv.DictReader(stream))
    po_rows[0]["unit_price"] = "13.00"
    with po_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(po_rows[0]))
        writer.writeheader()
        writer.writerows(po_rows)
    mismatch = run_pipeline(mismatch_root, spark)
    assert mismatch["product_rows"]["invoice_exceptions"] == 1
    assert mismatch["invoice_exceptions"][0]["exception_reason"] == "UNIT_PRICE_MISMATCH"

    amendment_root = tmp_path / "future-price-amendment"
    generate(amendment_root, "golden")
    po_file = amendment_root / "landing" / "purchase_order_lines_batch_001.csv"
    with po_file.open(newline="", encoding="utf-8") as stream:
        po_rows = list(csv.DictReader(stream))
    amendment = dict(
        po_rows[0],
        source_revision="2",
        source_updated_at="2026-10-21T08:00:00Z",
        effective_at="2026-10-20T16:00:00Z",
        unit_price="15.00",
    )
    with po_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(po_rows[0]))
        writer.writeheader()
        writer.writerows([*po_rows, amendment])
    amended = run_pipeline(amendment_root, spark)
    assert amended["product_rows"]["invoice_exceptions"] == 0
    delivery = spark.read.format("delta").load(str(amendment_root / "tables" / "silver" / "delivery_status")).first()
    assert str(delivery["po_unit_price"]) == "12.500000"


def test_partial_allocations_do_not_multiply_delivery_quantities(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "partial-allocations"
    generate(root, "golden")

    ticket_file = root / "landing" / "scale_tickets_batch_001.csv"
    with ticket_file.open(newline="", encoding="utf-8") as stream:
        tickets = list(csv.DictReader(stream))
    tickets[0].update(gross_weight="7.000", tare_weight="2.000")
    tickets.append(dict(
        tickets[0],
        source_record_id="SCALE-B:T-9002",
        scale_system_id="SCALE-B",
        ticket_id="T-9002",
        delivery_id="D-9002",
    ))
    with ticket_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(tickets[0]))
        writer.writeheader()
        writer.writerows(tickets)

    receipt_file = root / "landing" / "site_receipts_batch_001.csv"
    with receipt_file.open(newline="", encoding="utf-8") as stream:
        receipts = list(csv.DictReader(stream))
    receipts.append(dict(receipts[0], receipt_id="RCPT-9002", source_record_id="RCPT-9002", delivery_id="D-9002"))
    with receipt_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(receipts[0]))
        writer.writeheader()
        writer.writerows(receipts)

    allocation_file = root / "landing" / "invoice_delivery_allocations_batch_001.csv"
    with allocation_file.open(newline="", encoding="utf-8") as stream:
        allocations = list(csv.DictReader(stream))
    allocations[0]["allocated_quantity"] = "4.000"
    allocations.extend([
        dict(allocations[0], allocation_id="A2", source_record_id="SUP-1:INV-5001:L1:A2", delivery_id="D-9002", allocated_quantity="5.000"),
        dict(allocations[0], allocation_id="A3", source_record_id="SUP-1:INV-5001:L1:A3", delivery_id="D-9001", allocated_quantity="1.000"),
    ])
    with allocation_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(allocations[0]))
        writer.writeheader()
        writer.writerows(allocations)

    result = run_pipeline(root, spark)
    assert str(result["daily_delivered"][0]["accepted_net_quantity"]) == "10.000000"
    assert result["product_rows"]["invoice_exceptions"] == 0


def test_invoice_quantity_and_price_units_normalize_before_amount_check(tmp_path: Path, spark: SparkSession) -> None:
    root = tmp_path / "invoice-kg"
    generate(root, "golden")
    line_file = root / "landing" / "invoice_lines_batch_001.csv"
    with line_file.open(newline="", encoding="utf-8") as stream:
        lines = list(csv.DictReader(stream))
    lines[0].update(billed_quantity="10000.000", quantity_unit="kg", price_unit="metric_tonne")
    with line_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(lines[0]))
        writer.writeheader()
        writer.writerows(lines)

    result = run_pipeline(root, spark)
    assert result["product_rows"]["invoice_exceptions"] == 0
    reconciliation = spark.read.format("delta").load(str(root / "tables" / "silver" / "invoice_reconciliation")).first()
    assert str(reconciliation["billed_quantity_metric_tonnes"]) == "10.000000"
    assert str(reconciliation["invoice_price_metric_tonne"]) == "12.500000"


def test_quantity_price_and_amount_tolerance_boundaries_are_inclusive(tmp_path: Path, spark: SparkSession) -> None:
    at_boundary = tmp_path / "tolerance-boundary"
    generate(at_boundary, "golden")
    line_file = at_boundary / "landing" / "invoice_lines_batch_001.csv"
    with line_file.open(newline="", encoding="utf-8") as stream:
        lines = list(csv.DictReader(stream))
    lines[0].update(billed_quantity="10.050", unit_price="12.51", line_amount="125.74")
    with line_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(lines[0]))
        writer.writeheader()
        writer.writerows(lines)
    allocation_file = at_boundary / "landing" / "invoice_delivery_allocations_batch_001.csv"
    with allocation_file.open(newline="", encoding="utf-8") as stream:
        allocations = list(csv.DictReader(stream))
    allocations[0]["allocated_quantity"] = "10.050"
    with allocation_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(allocations[0]))
        writer.writeheader()
        writer.writerows(allocations)
    accepted = run_pipeline(at_boundary, spark)
    assert accepted["product_rows"]["invoice_exceptions"] == 0

    above_boundary = tmp_path / "above-tolerance-boundary"
    generate(above_boundary, "golden")
    line_file = above_boundary / "landing" / "invoice_lines_batch_001.csv"
    with line_file.open(newline="", encoding="utf-8") as stream:
        lines = list(csv.DictReader(stream))
    lines[0].update(billed_quantity="10.051", unit_price="12.51", line_amount="125.74")
    with line_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(lines[0]))
        writer.writeheader()
        writer.writerows(lines)
    allocation_file = above_boundary / "landing" / "invoice_delivery_allocations_batch_001.csv"
    with allocation_file.open(newline="", encoding="utf-8") as stream:
        allocations = list(csv.DictReader(stream))
    allocations[0]["allocated_quantity"] = "10.051"
    with allocation_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(allocations[0]))
        writer.writeheader()
        writer.writerows(allocations)
    rejected = run_pipeline(above_boundary, spark)
    assert rejected["product_rows"]["invoice_exceptions"] == 1
    assert rejected["invoice_exceptions"][0]["exception_reason"] == "QUANTITY_MISMATCH"


def test_invalid_timestamp_and_unsupported_invoice_unit_are_quarantined(tmp_path: Path, spark: SparkSession) -> None:
    invalid_time_root = tmp_path / "invalid-ticket-time"
    generate(invalid_time_root, "golden")
    ticket_file = invalid_time_root / "landing" / "scale_tickets_batch_001.csv"
    with ticket_file.open(newline="", encoding="utf-8") as stream:
        tickets = list(csv.DictReader(stream))
    tickets[0]["weighed_at"] = "not-a-timestamp"
    with ticket_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(tickets[0]))
        writer.writeheader()
        writer.writerows(tickets)
    invalid_time = run_pipeline(invalid_time_root, spark)
    assert invalid_time["quarantine_rows"] == 1
    assert invalid_time["daily_delivered"] == []
    quarantine = spark.read.format("delta").load(str(invalid_time_root / "tables" / "silver" / "quarantine"))
    assert quarantine.first()["quarantine_reason"] == "INVALID_EVENT_TIMESTAMP"

    invalid_unit_root = tmp_path / "invalid-invoice-unit"
    generate(invalid_unit_root, "golden")
    line_file = invalid_unit_root / "landing" / "invoice_lines_batch_001.csv"
    with line_file.open(newline="", encoding="utf-8") as stream:
        lines = list(csv.DictReader(stream))
    lines[0]["quantity_unit"] = "cubic_yard"
    with line_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(lines[0]))
        writer.writeheader()
        writer.writerows(lines)
    invalid_unit = run_pipeline(invalid_unit_root, spark)
    assert invalid_unit["quarantine_rows"] == 1
    quarantine = spark.read.format("delta").load(str(invalid_unit_root / "tables" / "silver" / "quarantine"))
    assert quarantine.first()["quarantine_reason"] == "UNKNOWN_INVOICE_QUANTITY_UNIT"
