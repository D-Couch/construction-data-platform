# Project status

## Milestone 1 — local baseline

Implemented a deterministic synthetic demonstration with six separate CSV feeds, source provenance, a separate golden oracle, real PySpark/Delta bronze and silver paths, quarantine, current/history records, daily accepted delivery totals, invoice exceptions, run JSON, and a generation pointer. The consumer contract reads all reporting products from the generation named in `tables/gold/current_generation.json`. The generator labels outputs synthetic and never writes its oracle into `landing/`.

**Observed golden result (Spark 4.0.1 / Delta 4.0.0):** six feeds × one source row; P-001 / AGG-1 / 2026-10-20 = **10.000000 metric tonnes**; invoice line = USD 125.00; **0 invoice exceptions; 0 quarantines**. The independently specified expected result is tracked in `fixtures/golden_oracle.json`; sanitized run evidence is in `evidence/milestone1-local-demo.json`. The latest published Delta files are under `data/demo/tables/gold/generations/`.

**Executed checks:** `make test` — 4 passed in 277.74 seconds. The tests exercised exact clean reconciliation against an independently written oracle, same-file replay, identical payload under a new filename, ticket revision (10 → 9 tonnes, one quantity exception, old generation remains readable), interruption before pointer replacement, kg conversion, unsupported-unit quarantine, and late PO arrival (pending support → resolved). `make demo` completed successfully. `.venv/bin/ruff check src tests`, `python -m compileall`, and YAML parsing passed. The Databricks CLI 1.21.0 bundle validator parsed the bundle and then stopped at authentication because profile `construction-data-dev` is not configured.

## Environment and reproduction

Inspected host: Fedora Linux 44 container on x86_64, 32 GiB RAM, 32 CPUs, Python 3.14.8. The system had no Java, Spark CLI, Databricks CLI or `~/.databrickscfg`; its `.git` directory is empty and read-only. Project dependencies are isolated in `.venv`; Temurin Java 17.0.20.1 is project-scoped in `.tools/java`. The actual Spark/Delta versions were selected and exercised locally. No Databricks runtime or workspace has been verified.

```bash
./scripts/bootstrap-local.sh   # Downloads project-scoped Java 17 and installs the package/dev dependencies.
make test                      # Local Spark + Delta acceptance tests.
make demo                      # Rebuild data/demo and publish one synthetic golden generation.
.venv/bin/ruff check src tests
```

The first Spark run resolves Delta JVM dependencies from Maven Central into `.cache/ivy`. `make demo` was observed to publish one daily row and zero exceptions; the executable run report includes IDs, row counts, timestamps, quantity and pointer path.

## Databricks deployment and blockers

`databricks.yml` uses the current Declarative Automation Bundles format, a bounded serverless Lakeflow Job, a project-owned schema and managed volume, and separate logical dev/prod schema targets. Configuration was YAML-parsed; `databricks bundle validate` was attempted with the downloaded official CLI and stopped with `cannot configure default credentials` for the declared `construction-data-dev` profile. No deployment or Databricks job run occurred. Local Delta tests are not Databricks verification.

Required next workspace step: sign in to or create an eligible Databricks workspace, then configure the named profile without sharing secrets in chat:

```bash
.tools/databricks/databricks auth login --host https://<workspace-url> --profile construction-data-dev
.tools/databricks/databricks bundle validate -t dev
```

After validation, review the isolated schema/volume/job changes and explicitly deploy/run the dev target. Free Edition is serverless-only and quota-limited; verify actual Unity Catalog, volume, and serverless Job support in the selected workspace first. No workspace resources were created here.

## Scope and remaining work

- The Databricks entrypoint and bundle are prepared but not remotely validated. Local tests write Delta by path and use an atomic local JSON pointer; object-store pointer semantics have not been tested.
- This tiny golden profile is correctness evidence, not a performance or production-scale claim. Source feeds are CSV fixtures, not live connectors or CDC.
- Initial source validation focuses on weight values/units and conflicting revision payloads. Invalid timestamp/envelope handling, malformed-row quarantine across all feeds, receipt conflict management, price-effective amendments, currency/supplier consistency, duplicate commercial invoice warnings, schema evolution, credits/cancellations, persisted run-ledger tables, and backfill repair workflows remain incomplete or later milestones.
- The bundle output stores Delta data under the project Volume path; registration as Unity Catalog managed tables, Auto Loader with durable checkpoints, source freshness monitors, and remote SQL reporting remain unverified/next work.
- Git identity is configured. The supplied `.git` directory is empty and read-only, so this workspace uses the ignored `.git-data/` directory through `scripts/git-project.sh` for local Git commands. This permits a local commit without changing the protected `.git` path.

## Next task

Configure a Databricks workspace profile, run CLI bundle validation, verify required Free Edition/workspace capabilities, then deploy and execute only the isolated dev bundle and compare its output with `data/demo/expected/golden_oracle.json`.
