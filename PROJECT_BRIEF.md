# construction-data-platform

A Databricks data platform that connects construction purchasing, deliveries, and invoices to produce reliable reporting and identify discrepancies.

## Purpose and business setting
Build a realistic, bounded data engineering assignment for a fictional infrastructure contractor. Purchasing, dispatch, scale houses, field receiving, and accounts payable send independent records on different schedules. Operations needs delivered quantities; accounts payable needs explainable invoice exceptions. Late records, duplicate exports, corrected tickets, and inconsistent identifiers undermine trust.

This is inspired by infrastructure-contractor enterprise data engineering responsibilities. It does not represent Michels systems, proprietary data, or a deployed enterprise service. Synthetic operational sources enable exact correctness tests. Demonstrating platform engineering matters more than adding an AI feature.

## Consumers and products
- Project operations: daily accepted net quantity by project, material, and business date, with completeness indicators.
- Accounts payable: invoice-line exceptions, quantity/price differences, unresolved references, and evidence links. Never initiate payments.
- Data engineering: run status, source freshness, quarantine reasons, reconciliation totals, recovery commands, and lineage evidence.

Start with aggregate deliveries measured by weight, USD, and a small fictional contractor. Exclude tax, freight surcharges, complex payment terms, employee payroll, GPS tracking, and forecasting from the first release. Dispatch analytics is an extension after purchasing/delivery/invoice reconciliation works.

## Architecture
Independent source exports/events -> landing -> raw Delta -> validated current/history Delta -> reporting Delta -> SQL reports/dashboard.

Databricks is the target execution platform. Use PySpark transformations and SQL with Delta tables. Prefer Unity Catalog managed tables and a volume for landing files, with explicit project-owned schemas and paths. Keep bronze/raw, silver/validated, and gold/reporting naming consistent and explain these terms in plain language.

Local development: an installable Python package, compatible local PySpark/Delta/Java dependencies, pytest, Ruff, fixtures, and deterministic integration runs. Avoid mandatory notebooks for business logic. A small notebook may invoke packaged code for demonstration.

Workspace execution: use Lakeflow Jobs for task orchestration, and Auto Loader/Structured Streaming for incremental file ingestion where supported. Choose one transformation/orchestration implementation rather than duplicating everything in both declarative and imperative pipelines. A supported bounded AvailableNow run is preferable to an unattended stream for a personal demo. File-arrival simulation is not a live dispatch connector or true source CDC. Document the source transport, trigger, and refresh latency accurately.

Deployment: use current Databricks Declarative Automation Bundles (formerly Asset Bundles), checking current CLI/resource schemas. Separate logical dev and prod targets with no hard-coded workspace IDs or credentials. Dev resources can be executed when access permits; a prod target in code is not evidence of a real production environment.

Do not require Kafka, Terraform-managed cloud infrastructure, Kubernetes, or a separate orchestration platform. Add only if a concrete later requirement justifies them. Free Edition restrictions must be checked; document unsupported features instead of promising them.

## Data correctness and reliability
Source contracts and business assumptions live in the companion docs. Preserve event time, source update time, ingestion time, and the source version independently. Preserve immutable raw evidence. Keep technical retries distinct from new business revisions. Support records whose referenced purchase order arrives later. Corrections, cancellations, and credit adjustments must change current totals without destroying history.

A Delta transaction is atomic for its table, not automatically across the entire DAG. Track run/batch identifiers and publish coherent reporting generations only after validation. Consumers must retain the last complete generation during failures. Define the publication/read contract explicitly and test interruption between table writes. Do not claim end-to-end exactly-once behavior based solely on a streaming checkpoint.

Business-key/revision deduplication must survive checkpoint changes and bounded historical rebuilds. Define watermark/lateness policy only after proving correction handling: a fixed watermark must not silently discard valid old revisions. Out-of-horizon events need a visible repair queue/backfill path.

## Data scale
Start with a tiny deterministic fixture and a modest demonstration profile, for example 10 projects and tens of thousands of operational records. Add configurable larger profiles within available quotas. Capture actual row counts, input bytes, output bytes, compute configuration, and elapsed times. Do not equate synthetic millions of rows with proven enterprise scale.

## Milestone 1 — complete local baseline
Implement the core four domains: purchasing, scale tickets, site receipts, invoices/allocations. Keep dispatch optional.
- Package and CLI with generate, ingest, transform, reconcile, validate, and demo/replay operations, or an equally clear small interface.
- Versioned source contracts, deterministic generator, independent expected-result fixture, and source provenance.
- Real local Spark/Delta raw, validated, and reporting tables; quarantine with reason codes.
- Daily quantities and invoice exceptions with record-level evidence and documented units/grains.
- Run ledger, reconciliation checks, a coherent publication generation, and a readable report/query output. Dashboard polish is later.
- Passing tests for clean input, duplicate replays, corrections, missing references, units, and monetary precision. Demonstrate interruption/restart does not publish half-updated reporting data.
- Actual output evidence and exact commands in README and docs/STATUS.md. Label data synthetic.
Local completion must explicitly state Databricks execution is pending if no workspace was available.

## Milestone 2 — verified Databricks execution
- Compatible deployment bundle, isolated dev schemas/volume, parameterized tasks and dependencies, timeouts and bounded retries.
- Incremental file ingestion with durable checkpoint/schema paths and batch source extracts.
- Run the core scenario on actual Databricks; compare output with the same expected results used locally. Record runtime, configuration, run identifiers, and sanitized evidence.
- Operational tables for freshness, failures, quarantine, and reconciliation; alert condition evaluation. Test a real configured notification channel only when explicitly authorized; otherwise show/test alert payloads and state delivery is unconfigured.
- Initial SQL reports or workspace dashboard with last-good publication time and incomplete-source indicators.
- Mark workspace-required checks unverified when blocked. Never silently substitute a local demo.

## Milestone 3 — production failure scenarios
- Out-of-order revisions, late arrivals, schema additions/breaking changes, invoices before receipts, credits, cancellations, and partial delivery allocations.
- Demonstrate restart after failure, safe replay, bounded backfill, quarantine repair/reprocessing, and stale-source status.
- Preserve reproducible historical published totals with explicit retention. Do not rely solely on temporary Delta time-travel retention for an indefinite audit requirement.
- Data contracts/checks in CI, integration tests in Databricks where authenticated, and deployment separation. Do not require workspace credentials for basic pull-request tests.
- Governance matrix: operations reader, restricted finance reader, engineer/writer; test available controls using approved principals. Do not claim enforcement from a policy document alone. Document native lineage actually observed and supplement with source-file/record provenance.
- Runbooks for upstream outage, schema incompatibility, checkpoint loss, bad load, and partial publication failure. Include detection, consumer impact, repair, validation, and rollback.

## Milestone 4 — measured tuning and portfolio delivery
- Measure one realistic bottleneck before/after a justified change: incremental recomputation, join strategy, file sizing, or table layout. Do not mechanically partition small tables or use unsupported optimizations.
- Hold data and correctness checks constant. Record warm/cold behavior, repetitions, platform settings, row counts and bytes. Quote monetary savings only from actual available billing data; otherwise report runtime/resource proxies.
- Complete dashboard, architecture figure, data dictionary, design decisions, sanitized real-run screenshots, and a short incident/recovery demonstration.
- Clearly document synthetic sources, tested scale, unsupported workspace features, and remaining production gaps. Keep CI/CD scripts and actual remote deployments distinct in status.

## Execution and scope
Finish one end-to-end milestone before expanding. Install missing project dependencies; do not stop at import compilation. If Java/Spark/Delta cannot execute, report the exact environment blocker and do not mark Spark integration passed. If account setup is needed, complete all independent local work and request only the necessary workspace/authentication step. No paid infrastructure or remote GitHub publishing is part of the initial task.

## Official documentation starting points
Verify current pages and workspace compatibility when implementation begins:
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- https://docs.databricks.com/aws/en/dev-tools/bundles/
- https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
- https://docs.databricks.com/aws/en/jobs/
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/
These are reference entry points, not instructions to provision AWS or assume a particular paid cloud account.
