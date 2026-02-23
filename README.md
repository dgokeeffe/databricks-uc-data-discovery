# Unity Catalog data discovery dashboard

An AI/BI (Lakeview) dashboard that combines `system.access.table_lineage`, `system.information_schema`, and `system.access.audit` to give you a comprehensive view of all data assets across your Databricks account.

**30 datasets, 6 pages, 62 widgets** - covering lineage, data health, governance, documentation coverage, and audit trails.

## Why table_lineage as the foundation?

Unlike `information_schema` (which is privilege-filtered and workspace-scoped), `table_lineage` captures every read/write event across your entire account - making it the best source of truth for understanding what data exists. This dashboard layers on `information_schema` for metadata enrichment and `audit` logs for governance visibility.

## Pages

### 1. Catalog overview

High-level inventory of your data estate:

- Total catalogs, schemas, tables, users, and workspaces (counter widgets)
- Most read tables in the last 90 days
- Catalog and schema inventory with table counts
- Entity metadata breakdown - shows exactly what's touching your data (AI/BI dashboards, DLT pipelines, interactive notebooks, jobs, SQL queries) using the `entity_metadata` struct

### 2. Lineage explorer

Deep dive into data lineage relationships:

- Full lineage table with source/target mapping
- Upstream and downstream table relationships
- Entity type to source/target type heatmaps
- Table access patterns by user, entity type, and date
- Column-level usage analysis
- Catalog usage per entity and per team

### 3. Data health and freshness

Identify stale, unused, and orphaned datasets:

- Data freshness distribution (Fresh/Recent/Aging/Stale)
- Data producers vs consumers breakdown
- Table freshness detail with days since last write/read
- User activity showing who produces vs consumes data

### 4. Cross-workspace

Understand shared data assets and data mesh patterns:

- Tables accessed from multiple workspaces
- Cross-workspace table details with workspace names

### 5. Governance and documentation

Documentation and tag coverage from `information_schema`:

- Table and column documentation percentages
- Tag coverage by catalog (tagged vs untagged tables)
- Table format distribution (Delta, Parquet, CSV, etc.)
- Table growth over time by catalog
- Table ownership with per-owner documentation rates
- Full documentation coverage detail table (sorted worst-first)

### 6. Audit and access logs

Unity Catalog governance events from `system.access.audit`:

- Summary counters: total events, unique users, permission changes, creates, deletes, failures
- UC governance activity over time (createTable, deleteTable, updatePermissions, etc.)
- Permission change log with who changed what and when
- Repeated denied access attempts (users hitting access errors 3+ times)
- Most active governance users ranked by event type

## System tables used

| System table | Purpose |
|---|---|
| `system.access.table_lineage` | Core data - all read/write events across the account |
| `system.access.column_lineage` | Column-level lineage for deep dive analysis |
| `system.access.workspaces_latest` | Workspace names and URLs (replaces manual workspace reference tables) |
| `system.access.audit` | UC governance events - permission changes, object lifecycle, denied access |
| `system.billing.usage` | Cost attribution for job-to-catalog mapping |
| `system.lakeflow.jobs` | Job metadata for lineage enrichment |
| `system.information_schema.tables` | Table metadata - owner, type, format, comments, creation date |
| `system.information_schema.columns` | Column metadata and documentation coverage |
| `system.information_schema.table_tags` | Tag coverage analysis |

## Key features

- **Account-level scope**: Sees all tables across all workspaces, not just the current one
- **No manual setup**: Uses `system.access.workspaces_latest` instead of requiring a custom workspace reference table
- **entity_metadata parsing**: Breaks down access by AI/BI dashboard, DLT pipeline, notebook, job, and SQL query
- **Documentation scoring**: Shows what % of tables and columns have comments, sorted worst-first for action
- **Tag coverage**: Identifies catalogs with low tag adoption
- **Audit trail**: Permission changes, denied access patterns, and governance user activity
- **Data freshness tiers**: Categorizes tables as Fresh/Recent/Aging/Stale based on last write
- **Cross-workspace analysis**: Identifies tables shared across workspace boundaries

## Prerequisites

- Unity Catalog enabled on your account
- System tables enabled (see [Enable system tables](https://docs.databricks.com/aws/en/admin/system-tables/#enable))
- `SELECT` permission on the system tables listed above
- A SQL warehouse to run the dashboard queries

## How to import

1. Download `uc-data-discovery.lvdash.json` from this repo
2. In your Databricks workspace, go to **Dashboards**
3. Click the kebab menu and select **Import dashboard from file**
4. Select the downloaded JSON file
5. Attach a SQL warehouse and run

## Notes

- The `job_to_catalog` dataset references two custom UDFs (`job_type_from_sku` and `team_name_from_tags`). You can either create these in your workspace or simplify that dataset by removing those columns.
- Lineage data has a [retention period](https://docs.databricks.com/aws/en/admin/system-tables/lineage) - older events may not be available.
- `system.access.workspaces_latest` is in Public Preview. It only includes currently active workspaces (cancelled workspaces are removed).
- `information_schema` views are privilege-filtered - you only see objects you have UC permissions to access.
- `system.access.audit` is in Public Preview.

## License

MIT
