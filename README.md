# construction-data-platform

A Databricks data platform that connects construction purchasing, deliveries, and invoices to produce reliable reporting and identify discrepancies.

This portfolio project uses **synthetic operational data only**. It is not connected to Michels or any real contractor. Milestone 1 is an installable Python package with real local PySpark and Delta tables. The versioned Databricks bundle defines a bounded serverless Lakeflow Job; workspace validation and execution are pending because no workspace authentication is configured in this environment.

## Run the local demo

Requires Python 3.11 or newer, Java 17, and network access to Maven Central the first time Delta dependencies are resolved. In this environment the Java 17 runtime is kept in `.tools/java` and Python dependencies in `.venv`.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
# If Java is not installed on the host, provision a project-scoped Java 17 runtime as described in docs/STATUS.md.
make demo
```

Generation creates files under `data/demo/landing/`; the independent oracle and scenario manifest are stored separately under `data/demo/expected/` and are never pipeline inputs. Spark writes raw, validated and reporting Delta data under `data/demo/tables/`. Consumers resolve all reporting products through `tables/gold/current_generation.json`. `data/demo/evidence/runs/` contains sanitized synthetic run reports.

Other commands:

```bash
.venv/bin/python -m construction_data_platform.cli generate --root data/correction --scenario ticket-correction
.venv/bin/python -m construction_data_platform.cli run --root data/correction
.venv/bin/python -m construction_data_platform.cli generate --root data/late-receipt --scenario late-receipt
.venv/bin/python -m construction_data_platform.cli run --root data/late-receipt
mv data/late-receipt/deferred/site_receipts_late_arrival.csv data/late-receipt/landing/
.venv/bin/python -m construction_data_platform.cli run --root data/late-receipt
make test
make lint
```

See [docs/STATUS.md](docs/STATUS.md) for exact environment setup, executed checks, outcomes and limitations.

## Reports and recovery

The latest synthetic run report is available with `.venv/bin/python -m construction_data_platform.cli report --root data/demo`. Export a self-contained snapshot of the current published run with `.venv/bin/python scripts/render-report.py --root data/demo`; the report leads with run outcome, the consumer publication pointer, arrived/missing/stale sources, and changes from the prior successful generation. See [`reports/synthetic-dashboard.html`](reports/synthetic-dashboard.html). Run `.venv/bin/python scripts/demo-recovery.py` (or `make recovery-demo`) to demonstrate a ticket correction, interruption between product writes, retention of the previous publication, and successful restart.

## Transfer example: SaaS usage and billing

Run `make saas-demo` for a separate synthetic example that reconciles metered software usage against a customer invoice. It uses local Spark and Delta to demonstrate a stale feed and invoice mismatch, a renamed-file replay with duplicate suppression, a corrected event revision, and an interrupted publication that leaves the last-good generation active. The final correction restores agreement. Each command writes to a new timestamped folder beneath `data/saas-demo/`; its `evidence/walkthrough.json` records the steps and generations. Its source schema and business rules are independent of the construction example; it demonstrates the same integration and pipeline operations pattern in another industry. Data is fictional and outputs are not production scale.

See the [recovery runbook](docs/RUNBOOK.md), [governance limits](docs/GOVERNANCE.md), [architecture](docs/ARCHITECTURE.md), and [design decisions](docs/DESIGN_DECISIONS.md).

Reproduce the local allocation experiment with `BENCHMARK_ARGS='--allocations-per-delivery 16' make benchmark`; its sanitized output is in [`evidence/milestone4-allocation-benchmark.json`](evidence/milestone4-allocation-benchmark.json). It records an input-reduction result but does not demonstrate a meaningful runtime gain.
