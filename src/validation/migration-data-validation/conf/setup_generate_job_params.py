# Databricks notebook source
# MAGIC %run ./constants

# COMMAND ----------

spark.sql(f"""
with dr_src as (
select 
  concat('"', concat_ws(':', workflow_name, table_family), '"') as workflow_table_family,
  collect_set(workflow_name) as workflow_name
from {VALIDATION_MAPPING_TABLE}
group by workflow_name, table_family
)
select string_agg(workflow_table_family, ','),workflow_name
 from dr_src
 group by workflow_name
""").display()