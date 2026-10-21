# Acceptance scenarios

Codex must implement executable assertions and save sanitized run evidence, not merely copy this checklist into a README. Run tiny golden tests locally and repeat core integration cases in Databricks when access permits. Keep source scenario truth out of the ingestion inputs.

| Scenario | Expected behavior |
| --- | --- |
| Clean purchase, 10-tonne accepted delivery, exact invoice | Delivered quantity 10; no reconciliation discrepancy |
| Same export rerun; duplicate event in a newly named file | Business counts/amounts unchanged; retry/dedup accounting visible |
| Ticket revision from 10 to 9 tonnes | Current delivery becomes 9, never 19; original invoice becomes a quantity exception; prior publication remains inspectable |
| Older revision arrives after newer revision | Current state remains newer; valid history retained |
| Same business key/revision, conflicting content | Visible conflict with deterministic handling; no nondeterministic winner |
| Receipt arrives two days late | Original receipt-date total is corrected; pending invoice resolves where appropriate |
| PO arrives after ticket | Unresolved reference becomes linked without manual raw-data edits |
| Mixed kg, metric tonne, US short ton | Known decimal conversions match independent expected values |
| Ambiguous ton or unsupported cubic-yard input | Quarantined with clear reason; no guessed conversion |
| Gross weight below tare; invalid timestamp | Invalid row isolated with provenance; valid rows continue under documented policy |
| Partial billing; several deliveries per invoice line | Allocations conserve quantities; no join fan-out; only supported amount reconciles |
| Two invoice IDs share a supplier invoice number | Suspected duplicate warning; both raw records preserved |
| Invoice precedes receipt | Pending evidence rather than confirmed mismatch; transition auditable |
| PO price amendment | Correct effective price selected; prior deliveries not silently repriced |
| Ticket cancellation / linked credit | Current quantity and net billed value corrected once; history retained |
| Additive field vs incompatible schema change | Defined compatible evolution vs quarantine/failure behavior; no silent reinterpretation |
| Process stops between reporting-table writes | Consumers see last complete generation; restart converges to the correct new generation |
| Rebuild a historical date and rerun it | Same results as clean rebuild; unrelated dates and history preserved |
| Lost checkpoint / replay old files | Recovery policy prevents duplicate business effects or refuses unsafe operation with a documented repair path |
| Source misses scheduled arrival | Freshness/completeness breach visible; last-good data time shown |

Milestone 1 must execute clean flow, duplicate replay, ticket correction, late/missing-reference handling, unit/amount validation, and interrupted publication tests. Later milestones extend the full matrix.

## Evidence requirements
- Test command, environment/dependency versions, seed, row counts and fixture version.
- Expected and actual normalized quantities, money, exception counts and states.
- Before/after current and historical outputs for corrections and reruns.
- Run identifiers, source provenance, quarantine reasons, publication generation.
- Local versus Databricks checks explicitly separated; unexecuted tests labeled unverified.
- Performance measurements are accepted only with correctness invariants passing on the same data.
