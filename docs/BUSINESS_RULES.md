# Demonstration business rules

These are configurable project assumptions, not Michels policies. Implement and test them before adding scope.

## Quantity and time
- Base material quantity: metric tonne (1,000 kg). Support kg and US short ton (2,000 lb); 1 lb = 0.45359237 kg. Store original unit/value and normalized decimal quantity. Unknown or ambiguous units must be quarantined, not assumed.
- No volume-to-weight conversion without an explicit approved density contract; initially exclude volume units.
- Daily delivered quantity counts the latest valid ticket net weight for a delivery with a latest accepted site receipt. Rejected/pending/unresolved deliveries are separately reported and excluded from accepted totals.
- Use project-local received date for daily receipt reporting. Late receipt arrival updates that historical date in a new published generation.
- Distinguish source business effective time from platform knowledge/ingestion time. Keep published generation snapshots for 'what we reported then' comparisons. SCD validity alone does not prove when the platform learned a fact.

## Reconciliation
- Invoice lines must reference a valid supplier/PO line/material and explicit delivery allocation(s). Validate supplier/project/material consistency. Unsupported currencies are exceptions.
- Compare quantity on the same allocated basis: do not compare a partially billed line against the entire delivery or entire PO.
- Demonstration quantity tolerance: max(0.01 metric tonne, 0.5% of allocated receipt quantity), configurable/versioned. Flag differences strictly greater than tolerance. Test threshold boundaries. Zero reference quantity requires an explicit branch.
- Compare unit prices in normalized quantity units against the PO price effective for the delivery, unless a documented amended-price rule applies. Initial price tolerance is USD 0.01 per base unit. Version the rule and test revisions; never silently apply today's PO price to all history.
- Validate line amount = billed quantity x unit price in its original pricing unit, rounded to cents using a documented rounding convention (initially ROUND_HALF_UP); amount tolerance USD 0.01. Use decimals, not binary floating-point money.
- Duplicate file/event processing is suppressed; suspected duplicate commercial invoices with distinct IDs are flagged, never silently deleted. Use supplier plus normalized invoice number as the initial deterministic warning key; document false-positive limitations.
- Credits must link to an original line. Model credits as signed adjustments exactly once with explicit document type and positive source magnitudes; do not double-negate. Do not treat a credit as a new material delivery. The local implementation uses the canonical supplier/invoice/line reference and reports credits as negative amounts in `invoice_net_amounts`.
- Cancelling a ticket removes it from current accepted totals and opens affected reconciliation exceptions; historical publication evidence remains available.

## Completeness and publication
- Initial daily demo reporting cutoff: 09:00 America/Chicago for the previous business date. This is a simulated target, not a service commitment. Generator manifests act as source delivery contracts, not correctness labels.
- Local source freshness reports a missing expected feed and flags latest `source_updated_at` older than 72 hours. This is a versioned synthetic monitoring threshold, separate from event-time lateness and not a source SLA.
- Check expected file/batch arrival and source event horizons separately. Latest ingestion time alone cannot establish business completeness.
- Publish complete or explicitly provisional data with source-specific freshness/completeness flags. A failed validation must not replace the last-good generation with an apparently healthy report.
- Separate hard failures (duplicate current keys, allocation multiplication, invalid publication) from row quarantine and warn-only freshness breaches. Every check has an owner role, severity, action, and evidence.
- Reporting readers use a published generation identifier shared by all products. Implement an atomic pointer/publication record or another tested equivalent; individual Delta table commits are insufficient for multi-table consistency.
- An invoice arriving before a receipt is pending evidence, not confirmed overbilling. Resolve it automatically after valid supporting data arrives. Preserve the exception status transition history.
