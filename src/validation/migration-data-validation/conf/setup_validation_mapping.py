# Databricks notebook source
# MAGIC %run ./constants

# COMMAND ----------

MAX_ENTRY_ID = int(spark.sql(f"select coalesce(max(entry_id),0) from {VALIDATION_MAPPING_TABLE}").first()[0])
print(f"Setting up Validation Mapping Table: {VALIDATION_MAPPING_TABLE}")
spark.sql(
    f"""
MERGE INTO {VALIDATION_MAPPING_TABLE} AS target
USING (
  select
    -- monotonically_increasing_id()+1+{MAX_ENTRY_ID} as entry_id,
    row_number() over(order by concat_ws('_',target_catalog,target_schema,target_table)) + {MAX_ENTRY_ID} as entry_id, 
    concat('wf_validation_',group_name) as workflow_name,
    concat_ws('_',target_catalog,target_schema,target_table) as table_family,
    'databricks' as src_connection_name,
    concat_ws('.','source_system',source_schema, source_table) as src_table,
    'databricks' as tgt_connection_name,
    concat_ws('.',target_catalog,target_schema,target_table) as tgt_table,
    '{{}}' as col_mapping,
    case when (primary_key is not null or primary_key <> ''  ) and (partition_column is not null or partition_column <> '') then 
      replace(concat_ws('|', primary_key, partition_column),',','|')
    when (primary_key is not null or primary_key <> ''  ) then
      replace(primary_key, ',','|')
    end as tgt_primary_keys,
    null as date_bucket,
    '_aud_batch_load_id' as mismatch_exclude_fields,
    '_aud_batch_load_id' as exclude_fields_from_summary,
    null as filter,
    '[{{"filter_name": "N/A", "filter": "N/A", "capture_mismatches": true}}]' as addtnl_filters,
    null as src_sql_override,
    null as tgt_sql_override,
    null as src_data_load_filter,
    null as tgt_data_load_filter,
    '{VALIDATION_DB}' as validation_data_db,
    null as retain_tables_list,
    true as quick_validation,
    case when is_active = 'Y' then true else false end as validation_is_active
  from {INGESTION_CONFIG_TABLE}
) AS source
ON target.src_table = source.src_table AND target.tgt_table = source.tgt_table
WHEN MATCHED THEN UPDATE SET
  target.workflow_name = source.workflow_name,
  target.table_family = source.table_family,
  target.src_connection_name = source.src_connection_name,
  target.src_table = source.src_table,
  target.tgt_connection_name = source.tgt_connection_name,
  target.tgt_table = source.tgt_table,
  target.col_mapping = source.col_mapping,
  target.tgt_primary_keys = source.tgt_primary_keys,
  target.date_bucket = source.date_bucket,
  target.mismatch_exclude_fields = source.mismatch_exclude_fields,
  target.exclude_fields_from_summary = source.exclude_fields_from_summary,
  target.filter = source.filter,
  target.addtnl_filters = source.addtnl_filters,
  target.src_sql_override = source.src_sql_override,
  target.tgt_sql_override = source.tgt_sql_override,
  target.src_data_load_filter = source.src_data_load_filter,
  target.tgt_data_load_filter = source.tgt_data_load_filter,
  target.validation_data_db = source.validation_data_db,
  target.retain_tables_list = source.retain_tables_list,
  target.quick_validation = source.quick_validation,
  target.validation_is_active = source.validation_is_active
WHEN NOT MATCHED THEN INSERT *
"""
).display()