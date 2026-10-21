# construction-data-platform

A Databricks data platform that connects construction purchasing, deliveries, and invoices to produce reliable reporting and identify discrepancies.

This portfolio project uses **synthetic operational data only**. It is not connected to Michels or any real contractor. Milestone 1 is an installable Python package with real local PySpark and Delta tables. Databricks execution is pending because no workspace authentication profile is configured in this environment.

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
.venv/bin/pytest
.venv/bin/ruff check src tests
```

See [docs/STATUS.md](docs/STATUS.md) for exact environment setup, executed checks, outcomes and limitations.

This workspace exposes the original empty `.git` directory as read-only. Use `scripts/git-project.sh` for Git commands here; it keeps repository metadata in the ignored `.git-data/` directory.
