# Governance and access verification

The following is an intended access matrix for a future Unity Catalog deployment. No workspace principals or grants are configured or tested in this repository, so this is a design proposal and not evidence of enforced access controls.

| Role | Intended access | Restrictions | Verification |
|---|---|---|---|
| Operations reader | Read published daily quantity and completeness reports | No raw invoice line amounts or write access | Workspace principal test pending |
| Finance reader | Read invoice exceptions, allocations, amounts, and source evidence | No payment actions or source mutation | Workspace principal test pending |
| Data engineer | Read raw/validated and operational status; write project pipeline tables | Project-owned schema/volume only; no unrelated resources | Workspace principal test pending |

Before a Databricks deployment, map these roles to approved workspace groups, apply least-privilege Unity Catalog grants to the project schema and volume, then test positive and negative read/write cases using approved principals. Do not infer security from this document or bundle names.

## Lineage evidence

No native Databricks lineage has been observed because no authenticated workspace run is available. Local evidence carries source feed, source file, content hash, source record ID/revision, payload hash, ingestion time, batch ID, and published generation. These fields provide application-level traceability; they are not a substitute claim for platform-native lineage.
