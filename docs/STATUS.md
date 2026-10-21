# Project status

## Milestone 1 — local baseline complete

The deterministic generator writes six independently versioned synthetic source feeds and keeps deferred late-arriving files outside the pipeline landing area. The Spark/Delta pipeline preserves source provenance, deduplicates business envelopes across replay and renamed files, validates and quarantines invalid ticket/receipt/invoice/allocation records, resolves effective purchase order revisions, and writes raw, validated, quarantine, and reporting Delta data. Reporting includes daily accepted delivered quantities, invoice reconciliation exceptions, and a run report. A generation pointer publishes a complete local output only after product writes succeed; failed generations remain unpublished and can be retried. The independent oracle is a test artifact under `fixtures/`, never a pipeline input.

The latest local synthetic demo run is recorded in [`evidence/milestone1-local-demo.json`](../evidence/milestone1-local-demo.json). It ingested six feeds × one source row and published **P-001 / AGG-1 / 2026-10-20 = 10.000000 metric tonnes**, with **USD 125.00** reconciled to the effective purchase-order price, **zero invoice exceptions**, and **zero quarantine rows**. Replay suppressed six duplicate business envelopes. The generator, demo output, and report are explicitly labeled synthetic.

## Checks actually run

- `make test` — **12 passed in 652.34 seconds** using local PySpark 4.0.1 and Delta Lake 4.0.0.
- `make demo` — completed successfully using real local Spark and Delta execution.
- `.venv/bin/ruff check src tests` — passed.
- `.venv/bin/python -m compileall -q src tests` — passed.
- Parsed `databricks.yml` as YAML and asserted the bounded job concurrency — passed.
- `.tools/databricks/databricks bundle validate -t dev` — configuration was read, then validation stopped because profile `construction-data-dev` has no configured authentication. No remote validation, deployment, or job run occurred.
- `git diff --check` — passed.

The 12 Spark acceptance tests cover clean oracle reconciliation; same-name and renamed-file duplicate replay; failed publication and retry; ticket correction with prior generation retained; invalid ticket quarantine; late PO and receipt arrivals; amount and price discrepancies; short-ton and invoice-kg unit normalization; future-effective PO price amendments; repeated/partial invoice allocations without delivery quantity multiplication; tolerance boundaries; and invalid timestamps/units.

## Environment and reproduction

The observed execution host is an x86_64 Fedora Linux 44 container with 32 CPUs and 31 GiB available memory; Python is 3.14.8. The project uses isolated `.venv` dependencies, project-scoped Temurin Java 17.0.20.1 at `.tools/java`, PySpark 4.0.1, Delta Lake 4.0.0, and Databricks CLI 1.21.0. The demo/tests were executed locally against Spark/Delta, not in a Databricks workspace. No Databricks CLI profile or workspace is configured.

```bash
./scripts/bootstrap-local.sh   # project-scoped Java 17 and Python dependencies
make test                      # local Spark + Delta acceptance suite
make demo                      # rebuild data/demo and publish synthetic golden output
.venv/bin/ruff check src tests
.venv/bin/python -m compileall -q src tests
```

The first Spark start may resolve the Delta Lake JVM package through the existing Maven configuration and cache it under ignored project state. `make demo` prints the synthetic manifest and sanitized run report. The independent oracle can be compared with published values in the acceptance tests; it is not loaded by the pipeline.

## Databricks readiness and blocker

The bundle declares a project-owned schema and managed volume and a bounded serverless Job (one concurrent run, one task retry, 3600-second timeout). Its serverless environment uses version 6 and the entrypoint uses the workspace-provided Spark session. The bundle format and YAML are locally parseable, but workspace compatibility is not verified. The local pipeline stores Delta by path and publishes through a local JSON pointer; Unity Catalog object registration, volume permissions, serverless execution behavior, and object-store pointer semantics have not been remotely tested.

When a workspace is available, authenticate locally without sharing credentials in chat, then validate:

```bash
.tools/databricks/databricks auth login --host https://<workspace-url> --profile construction-data-dev
.tools/databricks/databricks bundle validate -t dev
```

Review the isolated dev resources and workspace quotas before deploying/running. Free Edition is serverless-only and quota-limited; confirm it supports the declared Unity Catalog schema, managed volume, and serverless Job in the selected workspace. Local checks are not Databricks verification.

## Remaining work

Milestone 1 local acceptance is complete. Remote Databricks validation and an isolated dev run await workspace setup. Later production hardening includes durable Auto Loader/checkpoints, a persisted run ledger and repair workflow, schema evolution policy, freshness monitoring, and source-specific cancellation/credit semantics. This small synthetic fixture demonstrates correctness only; it makes no live-source, lineage, access-control, performance, or production-scale claim.
