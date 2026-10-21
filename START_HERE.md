# construction-data-platform — Codex handoff

A Databricks data platform that connects construction purchasing, deliveries, and invoices to produce reliable reporting and identify discrepancies.

## Start
1. Extract the construction-data-platform folder from the ZIP into your chosen local projects directory.
2. Open/add that folder as a new Project in Codex. Keep AGENTS.md at the project root.
3. Paste the entire contents of KICKOFF_PROMPT.txt into the first coding task.
4. Codex should inspect the host, implement a small end-to-end pipeline, execute its checks, and update docs/STATUS.md. This handoff contains specifications, not an implemented platform or claimed results.
5. If workspace access is missing, let Codex finish local work and prepare deployment. Supply the workspace URL and authenticate through the supported CLI/browser flow when needed; never paste access tokens into chat or committed files.

## Contents
- AGENTS.md: persistent implementation instructions.
- KICKOFF_PROMPT.txt: the initial task.
- PROJECT_BRIEF.md: business problem, architecture, scope, and milestones.
- docs/SOURCE_CONTRACTS.md: source boundaries, grains, revisions, and relationships.
- docs/BUSINESS_RULES.md: explicit demonstration assumptions and reconciliation rules.
- docs/ACCEPTANCE_TESTS.md: executable scenarios Codex must implement.
- docs/STATUS.md: initial status and continuation record.

## Databricks
Use your existing authorized workspace or a suitable Free Edition account. Codex must verify current feature support and quotas before selecting compute or deployment resources. It must not sign you up for paid services. A local Spark/Delta run is valuable but does not count as verified Databricks deployment.

## Later task
After reviewing Milestone 1, paste:
"Read AGENTS.md, PROJECT_BRIEF.md, and docs/STATUS.md. Inspect the current code and Git state. Implement the next incomplete milestone end to end, preserving tested source contracts and business semantics. Run the required checks, update status, and report measured results and remaining blockers."

GitHub owner: D-Couch. Intended repository name: construction-data-platform. Repository creation, visibility choice, and publication are later user-directed steps; no remote changes are made by this package.
