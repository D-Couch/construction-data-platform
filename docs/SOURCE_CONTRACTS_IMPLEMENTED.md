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

Milestone 1 preserves all supplied fields and provenance in raw Delta tables. Unknown scale weight units and invalid gross/tare ordering are quarantined. Equal business key/revision with different payload is a hard run failure. CSV schema compatibility policy is strict for consumed fields; additive source fields are preserved. Volume units, credits, fractional acceptance, supplier/material dimension mappings, duplicate commercial invoice warnings, PO effective-price selection across amendments, and repair workflows remain later work.
