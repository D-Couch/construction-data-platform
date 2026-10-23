# Source contracts, version 1.0

The implementation consumes six separate CSV exports from `landing/`, all labeled synthetic. Every row carries the common envelope in `docs/SOURCE_CONTRACTS.md`. Source columns currently used are:

| Feed | Business key | Milestone 1 fields |
|---|---|---|
| purchase_order_lines | company_id, po_id, po_line_id | project, supplier, material, ordered quantity/unit, price/unit/currency, effective time, status |
| scale_tickets | company_id, scale_system_id, ticket_id | delivery, PO line, material, gross/tare/unit, weighed time, status |
| site_receipts | company_id, receipt_id | delivery, project, received time, acceptance status/fraction |
| invoices | company_id, supplier_id, invoice_id | invoice date, currency, status, invoice number |
| invoice_lines | invoice key + invoice_line_id | PO line, billed quantity/unit, unit price/unit, amount, document type |
| invoice_delivery_allocations | invoice line key + allocation_id | delivery, allocated quantity/unit |

The local Spark/Delta implementation preserves supplied fields and file/record provenance in raw Delta tables. Invalid ticket weights/timestamps, receipt timestamps/status/fractions, and invoice/allocation units or numeric values are quarantined with reason codes. Equal business key/revision with different payload fails deterministically rather than choosing a winner. Reconciliation converts supported weight units to metric tonnes, checks allocation totals against billed and received quantities, validates invoice line amounts to cents using half-up rounding, and compares invoice prices to the PO revision effective at ticket weigh time. Delayed PO and receipt files resolve on replay. Additive CSV fields are preserved; consumed records declaring an unsupported schema version are quarantined while supported historical versions remain current. Ticket cancellation removes accepted current quantity and flags allocated invoice lines. Positive-magnitude credits require a reference to an existing original invoice line, are excluded from delivery allocation matching, and are negated once in `gold/invoice_net_amounts`. Quarantine repair requires a corrected higher revision or corrected source replay; no in-place raw mutation is supported. Volume units, supplier/material dimension mappings, duplicate commercial invoice warnings, and automatic quarantine repair tooling remain out of scope.
