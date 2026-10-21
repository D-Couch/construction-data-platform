# construction-data-platform

Read PROJECT_BRIEF.md, docs/SOURCE_CONTRACTS.md, docs/BUSINESS_RULES.md, docs/ACCEPTANCE_TESTS.md, and docs/STATUS.md before implementation. Use the agreed name and description verbatim in project-facing metadata.

## Working agreements
- Implement working milestones; begin with Milestone 1. Do not stop at a plan, directory scaffold, notebook, or dashboard mock.
- Make routine design decisions independently. Install project dependencies in an isolated environment, inspect the actual host, and continue independent work through external blockers.
- Databricks, PySpark, SQL, and Delta Lake are core requirements. Do not replace the implementation with a pandas-only simulation. Use small local Spark/Delta tests plus actual Databricks integration where access permits.
- Verify current official Databricks docs and workspace capabilities before writing platform-specific configuration. Use supported versioned deployment definitions and bounded compute. Do not create paid infrastructure or alter unrelated workspace resources.
- Preserve source revisions, raw provenance, table grains, and explicit contracts. Use deterministic ordering and keys. Prevent duplicate business effects when files, batches, or tasks are replayed.
- Never join line-level facts in a way that multiplies quantities or amounts. Use declared allocations and test aggregate conservation. Use decimal money/quantity semantics and explicit units.
- No secrets in code, reports, screenshots, fixtures, or logs. Use supported authentication and secret references. Do not ask for credentials in chat.
- Synthetic operational data must be labeled synthetic everywhere results are shown. Known-truth test artifacts must not be available as pipeline inputs. No invented benchmark, deployment, lineage, access-control, or production-scale claims.
- Test invalid inputs, revisions, late data, reruns, partial failures, and backfills. Execute relevant tests and smoke checks; distinguish local checks from remotely verified behavior.
- Use ordinary local Git checkpoints if identity is configured. Never guess Git author identity; continue implementation if commits are blocked. Do not create/publish a remote or push without user instruction.
- Keep docs/STATUS.md current with commands, evidence, limitations, blockers, and the next task. Keep this file concise; add actual setup/check commands once implemented.
