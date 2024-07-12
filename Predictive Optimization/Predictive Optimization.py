# Databricks notebook source
## Create widgets for user inputs
dbutils.widgets.text("metastore_name", "", "Metastore Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM system.storage.predictive_optimization_operations_history;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT metastore_name FROM system.storage.predictive_optimization_operations_history;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT catalog_name FROM system.storage.predictive_optimization_operations_history;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT table_name FROM system.storage.predictive_optimization_operations_history;

# COMMAND ----------

# DBTITLE 1,How many estimated DBUs has predictive optimization used in the last 30 days?
# MAGIC %sql
# MAGIC SELECT SUM(usage_quantity)
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE
# MAGIC      usage_unit = "ESTIMATED_DBU"
# MAGIC      AND  timestampdiff(day, start_time, Now()) < 30

# COMMAND ----------

# DBTITLE 1,On which tables did predictive optimization spend the most in the last 30 days (estimated cost)?
# MAGIC %sql
# MAGIC SELECT
# MAGIC      metastore_name,
# MAGIC      catalog_name,
# MAGIC      schema_name,
# MAGIC      table_name,
# MAGIC      SUM(usage_quantity) as totalDbus
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE
# MAGIC     usage_unit = "ESTIMATED_DBU"
# MAGIC     AND timestampdiff(day, start_time, Now()) < 30
# MAGIC GROUP BY ALL
# MAGIC ORDER BY totalDbus DESC

# COMMAND ----------

# DBTITLE 1,On which tables is predictive optimization performing the most operations?
# MAGIC %sql
# MAGIC SELECT
# MAGIC      metastore_name,
# MAGIC      catalog_name,
# MAGIC      schema_name,
# MAGIC      table_name,
# MAGIC      operation_type,
# MAGIC      COUNT(DISTINCT operation_id) as operations
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC GROUP BY ALL
# MAGIC ORDER BY operations DESC
# MAGIC

# COMMAND ----------

# DBTITLE 1,For a given catalog, how many total bytes have been compacted?
# MAGIC %sql
# MAGIC
# MAGIC -- For a given catalog, how many total bytes have been compacted?
# MAGIC
# MAGIC SELECT
# MAGIC      schema_name,
# MAGIC      table_name,
# MAGIC      SUM(operation_metrics["amount_of_data_compacted_bytes"]) as bytesCompacted,
# MAGIC      ROUND(SUM(operation_metrics["amount_of_data_compacted_bytes"]) / (1024 * 1024 * 1024), 1) as gbCompacted
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE
# MAGIC     metastore_name = '${metastore_name}'
# MAGIC     AND catalog_name = '${catalog_name}'
# MAGIC     AND operation_type = "COMPACTION"
# MAGIC GROUP BY ALL
# MAGIC ORDER BY bytesCompacted DESC

# COMMAND ----------

# DBTITLE 1,What tables had the most bytes compacted?
# MAGIC %sql
# MAGIC
# MAGIC -- What tables had the most bytes and gigabytes compacted?
# MAGIC
# MAGIC SELECT
# MAGIC     catalog_name,
# MAGIC      schema_name,
# MAGIC      table_name,
# MAGIC      SUM(operation_metrics["amount_of_data_compacted_bytes"]) as bytesCompacted,
# MAGIC      ROUND(SUM(operation_metrics["amount_of_data_compacted_bytes"]) / (1024 * 1024 * 1024), 1) as gbCompacted
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE operation_type = "COMPACTION"
# MAGIC and operation_metrics["amount_of_data_compacted_bytes"] > 0
# MAGIC GROUP BY ALL
# MAGIC ORDER BY bytesCompacted DESC
# MAGIC

# COMMAND ----------

# DBTITLE 1,What tables had the most bytes vacuumed?
# MAGIC %sql
# MAGIC -- What tables had the most bytes and gigabytes vacuumed?
# MAGIC
# MAGIC SELECT
# MAGIC      metastore_name,
# MAGIC      catalog_name,
# MAGIC      schema_name,
# MAGIC      table_name,
# MAGIC      SUM(operation_metrics["amount_of_data_deleted_bytes"]) as bytesVacuumed,
# MAGIC      ROUND(SUM(operation_metrics["amount_of_data_deleted_bytes"]) / (1024 * 1024 * 1024), 1) as gbVacuumed
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE operation_type = "VACUUM"
# MAGIC GROUP BY ALL
# MAGIC ORDER BY bytesVacuumed DESC

# COMMAND ----------

# DBTITLE 1,What is the success rate for operations run by predictive optimizations?
# MAGIC %sql
# MAGIC WITH operation_counts AS (
# MAGIC      SELECT
# MAGIC            COUNT(DISTINCT (CASE WHEN operation_status = "SUCCESSFUL" THEN operation_id END)) as successes,
# MAGIC            COUNT(DISTINCT operation_id) as total_operations
# MAGIC     FROM system.storage.predictive_optimization_operations_history
# MAGIC  )
# MAGIC SELECT successes / total_operations as success_rate
# MAGIC FROM operation_counts
# MAGIC
