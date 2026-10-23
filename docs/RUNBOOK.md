# Recovery runbook

All examples use synthetic local fixtures. Local commands validate the packaged Spark/Delta implementation; they do not exercise Databricks or an upstream source connection.

## Upstream outage or stale source

1. Check the latest run report in `<root>/evidence/runs/` and compare source files against the expected arrival schedule. A missing file is a freshness gap, not a zero-row success.
2. Keep consumers on the generation named by `<root>/tables/gold/current_generation.json`. A failed run leaves that pointer unchanged.
3. Obtain the missed source export through its approved channel, preserve its original file, and place it in `<root>/landing/` with the contracted feed prefix.
4. Run `make demo` only for the golden fixture; for an existing repair root run `.venv/bin/python -m construction_data_platform.cli run --root <root>`.
5. Confirm the run succeeded, expected records appear with source provenance, the invoice exceptions transition as expected, and the pointer now names the new generation. Keep the previous generation for audit comparison.

## Schema incompatibility

Additive CSV columns are retained in raw evidence and do not change the consumed contract. Missing or renamed required fields and unsupported schema versions must not be silently interpreted. Preserve the offending file, inspect its header against `docs/SOURCE_CONTRACTS.md`, and keep the last published generation active. Correct or map the source using a reviewed versioned contract, then replay the file. Do not edit raw Delta rows by hand.

## Lost checkpoint or replay

This local implementation does not rely on a streaming checkpoint: it scans the feed allowlist and deduplicates business key/revision/payload from persisted raw Delta history. Replaying identical files, including renamed retries, is safe and should not change current business totals. If raw history is missing or corrupted, stop publication and restore the retained raw evidence to a fresh repair root before processing. Do not delete the raw tables or reuse a root with partial unrelated contents.

## Quarantine repair

Read `tables/silver/quarantine` and record the reason code plus source file and business key. Preserve the original file. Correct the source in a new versioned export or add the missing required evidence; do not mutate quarantine or inject a repair flag. Replay the source. A valid higher revision supersedes the invalid revision in current state while both remain in raw evidence. Verify the quarantine state and publication generation after repair.

## Partial publication failure

If a run fails after one product is staged, readers continue to resolve the last-good generation through `current_generation.json`. Inspect the failed run report and staged generation, fix the cause, and rerun from the same root. The next successful run recomputes from immutable raw inputs and atomically replaces the local pointer. Never direct consumers to a staged generation. Databricks object-store pointer behavior has not been verified.

## Bad load or ticket cancellation

Stop downstream use of a suspect new generation by keeping the prior pointer. Preserve input evidence, identify the business key/revision, and issue a corrected source revision. A ticket revision with status `cancelled` removes its delivery from current accepted quantities and creates a `CANCELLED_DELIVERY` invoice exception when allocated. Reprocess and compare current results with the retained prior generation before making the new pointer current.

## Backfill and historical reconstruction

The current local rebuild is bounded by the source files retained in one project root, but recomputes the full current state rather than providing an incremental date-range API. Published generation Delta directories are retained independently of Delta time travel; no automatic pruning is configured. To rebuild, replay all retained source files into a separate clean root, compare outputs and provenance, and retain both root and generation evidence. A date-range-only rebuild is not implemented and must not be claimed.

## Rollback

For local output, atomically restore `current_generation.json` from the retained pointer contents for the prior generation only after confirming that all products for that generation still exist. Keep the failed generation for diagnosis. This local rollback procedure is not a verified Databricks rollback mechanism.
