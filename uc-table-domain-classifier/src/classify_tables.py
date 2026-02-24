# Databricks notebook source
# MAGIC %md
# MAGIC # UC table domain classifier
# MAGIC
# MAGIC Uses `ai_classify()` to label every table in Unity Catalog with a
# MAGIC **data domain** and **quality tier** based on its fully-qualified name.
# MAGIC Results are MERGEd into a Delta table for dashboard consumption.
# MAGIC
# MAGIC **Requires DBR 15.4+** (Foundation Model API must be enabled).

# COMMAND ----------

dbutils.widgets.text("target_table", "main.observability.table_domain_classifications")
dbutils.widgets.text("catalog_filter", "")

target_table = dbutils.widgets.get("target_table")
catalog_filter = dbutils.widgets.get("catalog_filter").strip()

print(f"Target table: {target_table}")
print(f"Catalog filter: {catalog_filter or '(all visible catalogs)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover tables

# COMMAND ----------

if catalog_filter:
    catalogs = [c.strip() for c in catalog_filter.split(",") if c.strip()]
    catalog_in = ", ".join(f"'{c}'" for c in catalogs)
    catalog_clause = f"AND t.table_catalog IN ({catalog_in})"
else:
    catalog_clause = ""

tables_df = spark.sql(f"""
SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) AS table_full_name,
    t.table_type,
    t.table_owner,
    t.created,
    t.last_altered
FROM system.information_schema.tables t
WHERE t.table_schema NOT IN ('information_schema')
  {catalog_clause}
ORDER BY t.table_catalog, t.table_schema, t.table_name
""")

total_tables = tables_df.count()
print(f"Found {total_tables} tables to classify")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classify with ai_classify()

# COMMAND ----------

classified_df = tables_df.selectExpr(
    "table_catalog",
    "table_schema",
    "table_name",
    "table_full_name",
    "table_type",
    "table_owner",
    "created",
    "last_altered",
    """ai_classify(
        table_full_name,
        ARRAY('finance', 'marketing', 'hr_people', 'iot_telemetry', 'customer',
              'product', 'engineering_infra', 'analytics', 'compliance_security', 'other')
    ) AS domain""",
    """ai_classify(
        table_full_name,
        ARRAY('bronze_raw', 'silver_staging', 'gold_curated',
              'archive_backup', 'system_internal', 'unknown')
    ) AS quality_tier""",
    "current_timestamp() AS classified_at",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure target table exists

# COMMAND ----------

parts = target_table.split(".")
if len(parts) == 3:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{parts[0]}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{parts[0]}`.`{parts[1]}`")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    table_catalog STRING,
    table_schema STRING,
    table_name STRING,
    table_full_name STRING,
    table_type STRING,
    table_owner STRING,
    created TIMESTAMP,
    last_altered TIMESTAMP,
    domain STRING,
    quality_tier STRING,
    classified_at TIMESTAMP
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE results

# COMMAND ----------

classified_df.createOrReplaceTempView("new_classifications")

spark.sql(f"""
MERGE INTO {target_table} AS tgt
USING new_classifications AS src
ON tgt.table_full_name = src.table_full_name
WHEN MATCHED THEN UPDATE SET
    tgt.table_catalog = src.table_catalog,
    tgt.table_schema = src.table_schema,
    tgt.table_name = src.table_name,
    tgt.table_type = src.table_type,
    tgt.table_owner = src.table_owner,
    tgt.created = src.created,
    tgt.last_altered = src.last_altered,
    tgt.domain = src.domain,
    tgt.quality_tier = src.quality_tier,
    tgt.classified_at = src.classified_at
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete rows for dropped tables

# COMMAND ----------

spark.sql(f"""
DELETE FROM {target_table}
WHERE table_full_name NOT IN (
    SELECT table_full_name FROM new_classifications
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = spark.sql(f"""
SELECT
    COUNT(*) AS total_classified,
    COUNT(DISTINCT domain) AS distinct_domains,
    COUNT(DISTINCT quality_tier) AS distinct_tiers
FROM {target_table}
""").collect()[0]

print(f"Classification complete:")
print(f"  Total tables classified: {summary.total_classified}")
print(f"  Distinct domains: {summary.distinct_domains}")
print(f"  Distinct quality tiers: {summary.distinct_tiers}")

domain_counts = spark.sql(f"SELECT domain, COUNT(*) AS cnt FROM {target_table} GROUP BY domain ORDER BY cnt DESC")
print("\nDomain distribution:")
domain_counts.show(truncate=False)

tier_counts = spark.sql(f"SELECT quality_tier, COUNT(*) AS cnt FROM {target_table} GROUP BY quality_tier ORDER BY cnt DESC")
print("Quality tier distribution:")
tier_counts.show(truncate=False)
