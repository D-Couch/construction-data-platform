# Source contracts

These are fictional, explicit interfaces to implement and version. Choose precise types/nullability in code and export a data dictionary. Source prefixes/company identifiers are part of business keys. Use string identifiers, UTC timestamps, decimal weights/money, and enumerated units/statuses. Business date is derived using the project's configured timezone, initially America/Chicago.

## Common envelope
Every source record has source_system, company_id, schema_version, source_record_id, source_revision, source_updated_at, and event_time. At ingestion add source_file, file_content_hash, payload_hash, ingested_at, and ingestion_batch_id. Corrections use a new revision with the same source business key. Retries may carry identical content in differently named files. Equal key/revision with different payload is a conflict, never an arbitrary last-write win. Record deletes/cancellations explicitly.

## Purchasing (batch exports)
projects: one project/version, with project_id, timezone and status.
suppliers/materials: one entity/version with canonical IDs, source-ID mapping, and material base unit.
purchase_order_lines: one full line image/revision keyed by company_id, po_id, po_line_id; project_id, supplier_id, material_id, ordered_quantity, quantity_unit, unit_price, price_unit, currency, effective_at, status. Initial delivery records reference one PO line. Line revisions do not erase history. Missing records in an extract do not imply deletion unless the extract contract explicitly says complete snapshot.

## Scale system (incremental event files)
scale_tickets: full ticket images/revisions keyed by company_id, scale_system_id, ticket_id; delivery_id, po_id/line, material_id, gross_weight, tare_weight, weight_unit, weighed_at, status. Net quantity is derived. Require gross >= tare >= 0. A corrected ticket replaces the prior current ticket, not a second delivery. One current ticket per delivery in the initial domain; violations become exceptions.

## Field receiving (batch/offline submissions)
site_receipts: one receipt/revision keyed by company_id, receipt_id; delivery_id, project_id, received_at, acceptance_status (accepted/rejected/pending), optional accepted_fraction in [0,1], source_revision. For the first milestone use accepted_fraction 1 or 0; fractional acceptance is later. One current authoritative receipt per delivery; conflicting receipts are quarantined/flagged. Do not sum retry submissions. Received_at can predate ingestion by several days.

## Accounts payable (batch exports)
invoices: one invoice/revision keyed by company_id, supplier_id, invoice_id; invoice_date, currency, status, business invoice number and normalized duplicate-detection attributes.
invoice_lines: one full line/revision keyed by invoice key plus invoice_line_id; po_id/line, billed_quantity, quantity_unit, unit_price, price_unit, line_amount, document_type, optional original_invoice_line_ref. Exclude tax/freight in initial scope. Currency initially USD only.
invoice_delivery_allocations: explicit versioned bridge keyed by invoice-line key plus allocation_id; delivery_id and allocated_quantity/unit. Support several deliveries per invoice line and partial billing across several lines without multiplying delivery facts. Validate allocation totals and cumulative quantity against receipts. Missing allocations are unresolved, not a reason to guess matches.

## Optional dispatch extension
dispatch_events: append events with stable event_id, delivery_id, truck_id, event_type, occurred_at. Event sequence may arrive out of order. Derive turnaround metrics only after defining event pairing, incomplete trips, and timezone behavior. Do not collect real driver/location information.

## Processing rules
1. Validate envelopes/schema; preserve raw evidence, quarantine malformed payloads with reason and provenance.
2. Deduplicate technical retries, order revisions deterministically, retain valid history, and expose current records.
3. Validate relationships without permanently discarding late-linked records. Use a retryable unresolved state distinct from invalid input.
4. Compute facts at declared grains before joins/aggregations. Use allocations for invoice-to-delivery relationships.
5. Record run outcomes and quality checks. Publish reporting generations only when hard invariants pass.
6. Support repair/reprocessing with the same contracts. Never use ground-truth fixture labels or injected-error flags as pipeline features or exception rules.

Synthetic generator must produce separate source feeds and arrival schedules, with a seed and scenario manifest kept outside the ingestion paths. Tiny golden fixtures must have independently specified expected answers; do not derive the oracle by running the production transformation function again.
