# Databricks notebook source
# MAGIC %md
# MAGIC # UC table storage snapshot
# MAGIC
# MAGIC Iterates over all Delta tables visible in Unity Catalog, runs
# MAGIC `ANALYZE TABLE ... COMPUTE STORAGE METRICS` on each, and writes results
# MAGIC to a configurable snapshot table.
# MAGIC
# MAGIC **Requires DBR 18.0+** (serverless satisfies this).

# COMMAND ----------

dbutils.widgets.text("target_table", "main.observability.table_storage_snapshot")
dbutils.widgets.text("catalog_filter", "")

target_table = dbutils.widgets.get("target_table")
catalog_filter = dbutils.widgets.get("catalog_filter").strip()

print(f"Target table: {target_table}")
print(f"Catalog filter: {catalog_filter or '(all visible catalogs)'}")

# COMMAND ----------

from datetime import datetime, date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DateType,
    TimestampType,
)

snapshot_schema = StructType(
    [
        StructField("snapshot_date", DateType(), False),
        StructField("table_catalog", StringType(), False),
        StructField("table_schema", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("table_full_name", StringType(), False),
        StructField("table_type", StringType(), True),
        StructField("data_source_format", StringType(), True),
        StructField("total_bytes", LongType(), True),
        StructField("total_num_files", LongType(), True),
        StructField("active_bytes", LongType(), True),
        StructField("num_active_files", LongType(), True),
        StructField("vacuumable_bytes", LongType(), True),
        StructField("num_vacuumable_files", LongType(), True),
        StructField("time_travel_bytes", LongType(), True),
        StructField("num_time_travel_files", LongType(), True),
        StructField("error", StringType(), True),
        StructField("snapshot_timestamp", TimestampType(), False),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover tables

# COMMAND ----------

# Build the catalog filter clause
if catalog_filter:
    catalogs = [c.strip() for c in catalog_filter.split(",") if c.strip()]
    catalog_in = ", ".join(f"'{c}'" for c in catalogs)
    catalog_clause = f"AND t.table_catalog IN ({catalog_in})"
else:
    catalog_clause = ""

tables_query = f"""
SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    t.table_type,
    t.data_source_format
FROM system.information_schema.tables t
WHERE t.table_schema NOT IN ('information_schema')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  {catalog_clause}
ORDER BY t.table_catalog, t.table_schema, t.table_name
"""

tables_df = spark.sql(tables_query)
tables_list = tables_df.collect()
total_tables = len(tables_list)
print(f"Found {total_tables} tables to analyze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze each table

# COMMAND ----------

today = date.today()
results = []
succeeded = 0
failed = 0

for i, row in enumerate(tables_list):
    full_name = f"`{row.table_catalog}`.`{row.table_schema}`.`{row.table_name}`"
    now = datetime.utcnow()

    if (i + 1) % 50 == 0 or i == 0:
        print(f"Processing {i + 1}/{total_tables}: {full_name}")

    try:
        metrics = spark.sql(
            f"ANALYZE TABLE {full_name} COMPUTE STORAGE METRICS"
        ).collect()[0]

        results.append(
            (
                today,
                row.table_catalog,
                row.table_schema,
                row.table_name,
                f"{row.table_catalog}.{row.table_schema}.{row.table_name}",
                row.table_type,
                row.data_source_format,
                metrics.total_bytes,
                metrics.total_num_files,
                metrics.active_bytes,
                metrics.num_active_files,
                metrics.vacuumable_bytes,
                metrics.num_vacuumable_files,
                metrics.time_travel_bytes,
                metrics.num_time_travel_files,
                None,  # error
                now,
            )
        )
        succeeded += 1

    except Exception as e:
        error_msg = str(e)[:1000]  # Truncate very long error messages
        results.append(
            (
                today,
                row.table_catalog,
                row.table_schema,
                row.table_name,
                f"{row.table_catalog}.{row.table_schema}.{row.table_name}",
                row.table_type,
                row.data_source_format,
                None,  # total_bytes
                None,  # total_num_files
                None,  # active_bytes
                None,  # num_active_files
                None,  # vacuumable_bytes
                None,  # num_vacuumable_files
                None,  # time_travel_bytes
                None,  # num_time_travel_files
                error_msg,
                now,
            )
        )
        failed += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write results

# COMMAND ----------

print(f"\nDone. Succeeded: {succeeded}, Failed: {failed}, Total: {total_tables}")

if results:
    results_df = spark.createDataFrame(results, schema=snapshot_schema)

    # Ensure target catalog/schema exist (best effort)
    parts = target_table.split(".")
    if len(parts) == 3:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{parts[0]}`")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{parts[0]}`.`{parts[1]}`")

    results_df.write.mode("append").saveAsTable(target_table)
    print(f"Wrote {len(results)} rows to {target_table}")
else:
    print("No tables found - nothing to write.")
