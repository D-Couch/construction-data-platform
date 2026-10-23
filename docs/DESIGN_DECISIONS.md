# Design decisions

All sample values and operational records in this project are synthetic. These decisions describe the bounded demonstration implementation, not contractor policy.

| Decision | Reason | Limitation |
|---|---|---|
| Keep append-only source evidence in raw Delta and select current records by business key/revision | Replay, corrections, and investigations need preserved source versions and provenance | Local pipeline scans and recomputes retained history; no streaming checkpoint or incremental merge path |
| Preserve the simpler allocation join until a stable measured benefit is established | In the local 10,000-line / 160,000-allocation profile, pre-aggregation reduced join input 16-fold but warmed median elapsed time changed from 190.40 ms to 188.08 ms, too small to establish a gain | Databricks and broader profiles are not available; first-pass timings were ordered and are not comparable cold-run measurements |
| Store reporting products in generation-specific Delta paths and switch one pointer only after all writes | Readers must see a complete set of products | Atomic file replacement is only locally exercised; Databricks volume/object-store semantics are unverified |
| Keep invoice credits as positive source magnitudes with an explicit original-line reference, then negate once in the derived net amount | Preserve source document semantics and prevent double-negation | Initial link format is supplier/invoice/line identifiers; tax, freight, and complex credit pricing are out of scope |
| Keep source schema evolution strict for consumed fields and preserve additive fields | Prevent silent reinterpretation while allowing harmless additions | Automated schema registry and explicit cross-version mapping are not implemented |
| Do not configure automatic publication-history pruning | A prior published result must remain reproducible independently of Delta time travel | Local storage can grow; an explicit archival/retention policy is needed before larger runs |
