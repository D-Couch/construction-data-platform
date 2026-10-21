"""Create deterministic synthetic source files; manifests and truth stay outside landing."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

SCHEMA_VERSION = "1.0"
SEED = 1729


def _write_csv(path: Path, rows: list[dict[str, str]]) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    payload = path.read_bytes()
    return {"path": str(path), "rows": len(rows), "sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}


def _envelope(record_id: str, revision: str, event_time: str, **fields: str) -> dict[str, str]:
    return {
        "source_system": fields.pop("source_system", "synthetic"),
        "company_id": "NORTHSTAR-CIVIL",
        "schema_version": SCHEMA_VERSION,
        "source_record_id": record_id,
        "source_revision": revision,
        "source_updated_at": fields.pop("source_updated_at", event_time),
        "event_time": event_time,
        **fields,
    }


def generate(root: Path, scenario: str = "golden", seed: int = SEED) -> dict[str, object]:
    """Write feeds to root/landing and scenario truth only to root/expected."""
    root = root.resolve()
    landing = root / "landing"
    expected = root / "expected"  # Never consumed by the pipeline.
    for folder in (landing, expected):
        folder.mkdir(parents=True, exist_ok=True)
    for old in landing.glob("*.csv"):
        old.unlink()

    po_time = "2026-10-19T12:00:00Z"
    delivery_time = "2026-10-20T14:30:00Z"
    po = [_envelope("PO-1001-L1", "1", po_time, po_id="PO-1001", po_line_id="1", project_id="P-001", supplier_id="SUP-1", material_id="AGG-1", ordered_quantity="30.000", quantity_unit="metric_tonne", unit_price="12.50", price_unit="metric_tonne", currency="USD", effective_at=po_time, status="open")]
    tickets = [_envelope("SCALE-A:T-9001", "1", delivery_time, scale_system_id="SCALE-A", ticket_id="T-9001", delivery_id="D-9001", po_id="PO-1001", po_line_id="1", material_id="AGG-1", gross_weight="12.000", tare_weight="2.000", weight_unit="metric_tonne", weighed_at=delivery_time, status="valid")]
    receipts = [_envelope("RCPT-9001", "1", delivery_time, receipt_id="RCPT-9001", delivery_id="D-9001", project_id="P-001", received_at=delivery_time, acceptance_status="accepted", accepted_fraction="1")]
    invoices = [_envelope("SUP-1:INV-5001", "1", "2026-10-20T18:00:00Z", supplier_id="SUP-1", invoice_id="INV-5001", invoice_date="2026-10-20", currency="USD", status="posted", invoice_number="5001", normalized_invoice_number="5001")]
    invoice_lines = [_envelope("SUP-1:INV-5001:L1", "1", "2026-10-20T18:00:00Z", supplier_id="SUP-1", invoice_id="INV-5001", invoice_line_id="L1", po_id="PO-1001", po_line_id="1", material_id="AGG-1", billed_quantity="10.000", quantity_unit="metric_tonne", unit_price="12.50", price_unit="metric_tonne", line_amount="125.00", document_type="invoice", original_invoice_line_ref="")]
    allocations = [_envelope("SUP-1:INV-5001:L1:A1", "1", "2026-10-20T18:00:00Z", supplier_id="SUP-1", invoice_id="INV-5001", invoice_line_id="L1", allocation_id="A1", delivery_id="D-9001", allocated_quantity="10.000", quantity_unit="metric_tonne")]

    feeds = {"purchase_order_lines": po, "scale_tickets": tickets, "site_receipts": receipts, "invoices": invoices, "invoice_lines": invoice_lines, "invoice_delivery_allocations": allocations}
    files = []
    for name, rows in feeds.items():
        files.append(_write_csv(landing / f"{name}_batch_001.csv", rows))

    if scenario in {"ticket-correction", "late-reference"}:
        revision = dict(tickets[0], source_revision="2", source_record_id="SCALE-A:T-9001", source_updated_at="2026-10-21T08:00:00Z", gross_weight="11.000", tare_weight="2.000", weighed_at="2026-10-20T14:30:00Z")
        _write_csv(landing / "scale_tickets_batch_002.csv", [revision])
    if scenario == "duplicate-replay":
        # New filename, identical business envelope/payload to exercise retry suppression.
        _write_csv(landing / "scale_tickets_retry_renamed.csv", tickets)
    if scenario == "invalid-unit":
        bad = dict(tickets[0], source_record_id="SCALE-A:T-BAD", ticket_id="T-BAD", delivery_id="D-BAD", weight_unit="ton")
        _write_csv(landing / "scale_tickets_invalid.csv", [bad])

    now = datetime.now(UTC).isoformat()
    manifest = {"synthetic": True, "project": "construction-data-platform", "description": "A Databricks data platform that connects construction purchasing, deliveries, and invoices to produce reliable reporting and identify discrepancies.", "scenario": scenario, "seed": seed, "contract_version": SCHEMA_VERSION, "generated_at_utc": now, "arrival_schedule": {"initial_feeds": "available at run start", "correction": "included only in correction scenarios"}, "files": files}
    (expected / "scenario_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    oracle_path = Path(__file__).resolve().parents[2] / "fixtures" / "golden_oracle.json"
    (expected / "golden_oracle.json").write_text(oracle_path.read_text(encoding="utf-8"), encoding="utf-8")
    return manifest
