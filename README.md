# AML Data Hub (Scaffold v1)

Standalone ingestion and data management app for AML transaction monitoring data.

## What this includes

- Landing area ingestion from CSV files (`data/landing`)
- Data quality rules (`informative` and `reject` severities)
- Referential integrity checks before bridge/fact inserts
- Job statistics capture (runtime, files, records read/loaded/rejected)
- Star schema baseline (natural keys only)
- SCD Type-2 style processing for dimensions using composite key: `natural_key + valid_from`
- Basic UI report page for job stats and DQ outcomes
- Alembic-backed schema migrations with revision history

## Schema (initial)

Dimensions:
- `dh_dim_household`
- `dh_dim_customer`
- `dh_dim_associated_party`
- `dh_dim_account`
- `dh_dim_branch`
- `dh_dim_branch`
- `dh_dim_sub_account`
- `dh_dim_country`
- `dh_dim_currency`
- `dh_dim_ofac_sdn`
- `dh_dim_panama_node`
- `dh_dim_counterparty_account`
- `dh_dim_transaction_type`

Bridges:
- `dh_bridge_household_customer`
- `dh_bridge_customer_account`
- `dh_bridge_customer_associated_party`
- `dh_bridge_panama_relationship`

Fact:
- `dh_fact_cash`

Operations / metadata:
- `dh_job_runs`
- `dh_job_file_stats`
- `dh_dq_rules`
- `dh_dq_results`

## Input file contract (per-table)

Each input CSV must be table-prefixed and loaded from a separate file.

Examples:
- `dh_dim_customer_20260417_120000.csv`
- `dh_bridge_customer_account_20260417_120000.csv`
- `dh_fact_cash_20260417_120000.csv`

Supported prefixes:
- `dh_dim_household`
- `dh_dim_customer`
- `dh_dim_associated_party`
- `dh_dim_account`
- `dh_dim_sub_account`
- `dh_dim_country`
- `dh_dim_currency`
- `dh_dim_ofac_sdn`
- `dh_dim_panama_node`
- `dh_dim_counterparty_account`
- `dh_dim_transaction_type`
- `dh_bridge_household_customer`
- `dh_bridge_customer_account`
- `dh_bridge_customer_associated_party`
- `dh_bridge_panama_relationship`
- `dh_fact_cash`

Unsupported filenames are moved to rejected and logged as DQ issues.

## Load order and integrity

Pipeline load order is dependency-safe:
1. Dimensions
2. Bridges
3. Fact (`dh_fact_cash`)

Before inserting bridge/fact rows, the pipeline checks referenced keys exist in current dimension rows.
Failed checks are rejected and logged in `dh_dq_results` with `rule_name=referential_integrity`.

## Quick start

1. Create a virtualenv and install requirements.
2. Copy `.env.example` to `.env` and adjust if needed.
3. Run schema migrations:
   - `./scripts/db_migrate.sh local`
4. Load sample files:
   - `./scripts/load_sample_data.sh`
5. Run a job:
   - `./scripts/run_job.sh`
6. Start UI/API server:
   - `./scripts/start_server.sh`
7. Open:
   - `http://localhost:8100`

## Migration script

Use the migration helper:

- `./scripts/db_migrate.sh [local|remote|database-url] [auto|upgrade|stamp|repair]`

Examples:

- `./scripts/db_migrate.sh local`
- `./scripts/db_migrate.sh remote`
- `./scripts/db_migrate.sh remote upgrade`
- `./scripts/db_migrate.sh sqlite:////tmp/data_hub.db stamp`
- `./scripts/db_migrate.sh local repair`


## Dimension attr_json schemas

Dimension attribute structure is defined in JSON files under:
- `config/dim_schemas/`

One schema file per dimension table (for example `dh_dim_customer.json`).

Supported schema keys:
- `required`: list of required attribute names
- `properties`: per-field constraints
- `additionalProperties`: `true` or `false`

Supported property constraints:
- `type`: `string`, `number`, `integer`
- `maxLength` (string)
- `pattern` (string regex)
- `enum` (allowed values)
- `minimum`, `maximum` (number/integer)
- `dq`: field-level DQ rules object

Supported `dq` checks:
- `not_null`: true/false
- `regex`: regex pattern
- `lookup_name`: validates against active values in `dh_lov_values`
- `dimension_lookup`: validates against an existing dimension table value
  format: `{ "table": "dh_dim_country", "field": "country_code_2" }`

Pipeline behavior:
- During dimension loads, non-key CSV columns are mapped into `attr_json` attributes.
- Attributes are validated against the table schema before insert/update.
- Violations are rejected and logged to `dh_dq_results` with `rule_name=dimension_schema_validation`.

Schema lint command:
- `./scripts/check_dim_schemas.sh`

## LOV config

List-of-values are managed from CSV at:
- `config/lov_values.csv`

Sync LOVs into the database:
- `./scripts/sync_lov_values.sh`
- `./scripts/sync_lov_values.sh --deactivate-missing`
- `./scripts/sync_lov_values.sh --csv /path/to/lov_values.csv`

The pipeline uses these for schema `dq.lookup_name` checks.

Strict preflight guard:
- `./scripts/check_dim_schemas.sh` now fails if any schema `dq.lookup_name` is missing from `config/lov_values.csv`.
- Job runtime also performs this preflight and fails fast before processing input files when lookup references are out of sync.

## DQ rules config

DQ rules are managed from CSV at:
- `config/dq_rules.csv`

Sync rules into the database:
- `./scripts/sync_dq_rules.sh`
- `./scripts/sync_dq_rules.sh --deactivate-missing`
- `./scripts/sync_dq_rules.sh --csv /path/to/dq_rules.csv`

Behavior:
- `rule_name` is the stable key.
- Existing rules with the same `rule_name` are updated.
- New rules are inserted.
- Rules missing from CSV are only deactivated when `--deactivate-missing` is used.

## Monitoring APIs

No-auth JSON endpoints:
- `GET /api/dq/rules` (supports `limit`, `active_only`, `entity_name`)
- `GET /api/jobs/batch-results` (supports `limit`)
- `GET /api/dq/violations` (supports `limit`)
- `GET /api/graph/summary` (supports `include_surrogates`, `include_ofac_matches`, `include_txn_flow`)
- `GET /api/graph/elements` (supports `include_surrogates`, `include_ofac_matches`, `include_txn_flow`)
- `GET /api/graph/customer/{customer_key}` (supports `hops`, `max_nodes`, `max_edges`, plus graph include flags)
- `GET /api/graph/customer/{customer_key}/summary` (supports `hops`, plus graph include flags)

Related endpoints:
- `GET /api/jobs/runs`
- `GET /api/dq/results`
- `POST /api/jobs/run`

Lightweight API browser UI:
- `GET /api-browser`

Graph snapshot export:
- `python3 scripts/build_graph_snapshot.py`
- Optional flags:
  - `--output /path/to/graph_snapshot.json`
  - `--no-include-surrogates`
  - `--no-include-ofac-matches`
  - `--no-include-txn-flow`

## Scheduling

Use `scripts/run_job.sh` in cron or an external scheduler.


## OFAC SDN Reference Data

Use this script to refresh OFAC SDN sample reference data from OFAC's published CSV:

- `./scripts/update_ofac_sdn_sample.sh`

This updates:
- `data/sample/dh_dim_ofac_sdn_sample.csv`

Pipeline support:
- SDN rows are loaded into `dh_dim_ofac_sdn` as SCD Type 2 rows keyed by `sdn_uid`.


## ISO Reference Data

Use these scripts to refresh ISO-backed reference sample files:

- `./scripts/update_country_sample.sh` updates `data/sample/dh_dim_country_sample.csv`
- `./scripts/update_currency_sample.sh` updates `data/sample/dh_dim_currency_sample.csv`

Combined refresh (country + currency + OFAC SDN + Panama Papers):
- `./scripts/update_iso_reference_samples.sh`


## Panama Papers Reference Data

Use this script to refresh Panama Papers nodes and relationships from ICIJ's Offshore Leaks dataset:

- `./scripts/update_panama_papers_samples.sh`

This updates:
- `data/sample/dh_dim_panama_node_sample.csv`
- `data/sample/dh_bridge_panama_relationship_sample.csv`

Pipeline support:
- Nodes are loaded into `dh_dim_panama_node` as SCD Type 2 rows keyed by `node_id + node_type`.
- Relationships are loaded into `dh_bridge_panama_relationship` with SCD history keyed by `start_node_id + end_node_id + rel_type`.
