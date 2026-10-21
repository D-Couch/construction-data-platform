# Milestone 1 data dictionary

All values are synthetic. Source records retain a common envelope (`source_system`, `company_id`, `schema_version`, `source_record_id`, `source_revision`, `source_updated_at`, `event_time`) plus `source_file`, `file_content_hash`, `payload_hash`, `ingested_at`, and `ingestion_batch_id`.

| Layer/table | Grain | Important measures/fields | Storage |
|---|---|---|---|
| bronze/{feed} | source row and observed file | immutable CSV fields and provenance | Delta |
| silver/{feed}_history | business key + revision + payload | all non-conflicting revisions | Delta |
| silver/{feed}_current | one highest valid revision per key | current source image | Delta |
| silver/quarantine | rejected source record | reason code and source provenance | Delta |
| silver/delivery_status | current ticket + receipt + PO line | current state and accepted tonnes | Delta |
| gold/daily_delivered_quantities | project, material, project-local receipt date | accepted net metric tonnes | Delta, generation scoped |
| gold/invoice_exceptions | invoice line | allocation/receipt quantity difference or amount/reference reason | Delta, generation scoped |
| gold/current_generation.json | one pointer | shared generation ID and publish time | atomic local file replace |

Weights retain original source values and units. Accepted delivery reporting uses decimal metric tonnes (1,000 kg; US short ton is 907.18474 kg). Money is USD decimal and line amount validation rounds half-up to cents via Spark `round` for positive initial invoice values. Quantity tolerance is max(0.01 tonne, 0.5% of allocated accepted quantity). These are demo rules, not contractor policy.
