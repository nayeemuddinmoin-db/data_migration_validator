# Databricks notebook source
# MAGIC %run ./constants

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {VALIDATION_MAPPING_TABLE} (
    entry_id BIGINT,
    workflow_name STRING,
    table_family STRING,
    src_connection_name STRING,
    src_table STRING,
    tgt_connection_name STRING,
    tgt_table STRING,
    col_mapping STRING,
    tgt_primary_keys STRING,
    date_bucket STRING,
    mismatch_exclude_fields STRING,
    exclude_fields_from_summary STRING,
    filter STRING,
    addtnl_filters STRING,
    src_sql_override STRING, 
    tgt_sql_override STRING,
    src_data_load_filter STRING,
    tgt_data_load_filter STRING,
    validation_data_db STRING,
    retain_tables_list STRING,
    quick_validation BOOLEAN,
    validation_is_active BOOLEAN)
    ''')
except Exception as e:
   print(str(e).split("\n")[0])

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {TABLE_CONFIG_TABLE} (
    connection_name STRING,
    warehouse STRING,
    jdbc_options STRING,
    cast_to_string BOOLEAN)
  ''')
except Exception as e:
   print(str(e).split("\n")[0])

# COMMAND ----------

try:
  spark.sql(f"INSERT OVERWRITE {TABLE_CONFIG_TABLE} VALUES ('databricks','databricks',null,true)")
except Exception as e:
   print(str(e).split("\n")[0])

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {WINDOWED_VALIDATION_METRICS}
  (
    run_timestamp TIMESTAMP,
    table_type STRING,
    last_update_date_field STRING,
    last_updated_date DATE,
    num_records BIGINT
  )
  PARTITIONED BY (iteration_name STRING, workflow_name STRING, table_family STRING)
  ''')
except Exception as e:
   print(str(e).split("\n")[0])

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {UPDATE_TSTMP_TIMELINE}
  (
    run_timestamp TIMESTAMP,
    type STRING,
    date_bucket STRING,
    last_update_tstmp TIMESTAMP
  )
  PARTITIONED BY (iteration_name STRING, workflow_name STRING, table_family STRING)
  ''')
except Exception as e:
  print(str(e).split("\n")[0])

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {PRIMARY_KEY_VALIDATION} (
    run_timestamp timestamp,
    table_name STRING,
    total_record_count BIGINT,
    distinct_key_count BIGINT
  ) PARTITIONED BY (iteration_name STRING, workflow_name STRING, table_family STRING)
  ''')
except Exception as e:
  print(str(e).split("\n")[0])

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {DATA_TYPE_COMPATIBILITY_MATRIX} (
    warehouse STRING,
    data_type STRING,
    has_precision BOOLEAN,
    databricks_compatibility_regex STRING,
    netezza_compatibility_regex STRING,
    snowflake_compatibility_regex STRING,
    oracle_compatibility_regex STRING,
    mssql_compatibility_regex STRING,
    teradata_compatibility_regex STRING,
    hive_compatibility_regex STRING
    )
  ''')
except Exception as e:
  print(str(e).split("\n")[0])

# COMMAND ----------

import os
spark.read.option("header","true")\
    .option("multiLine", "true")\
    .option("escape","\"")\
    .option("inferSchema", "true")\
    .csv("file://"+os.path.abspath("./data_type_compatibility_matrix.csv"))\
    .write.mode("overwrite").saveAsTable(f'''{DATA_TYPE_COMPATIBILITY_MATRIX}''')

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {VALIDATION_LOG_TABLE} (
    src_warehouse STRING,
    src_table STRING,
    tgt_warehouse STRING,
    tgt_table STRING,
    validation_run_status STRING,
    validation_run_start_time TIMESTAMP,
    validation_run_end_time TIMESTAMP,
    streamlit_user_name STRING,
    streamlit_user_email STRING,
    exception STRING
  )PARTITIONED BY (iteration_name STRING, workflow_name STRING, table_family STRING)
    ''')
except Exception as e:
  print(str(e).split("\n")[0])

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {VALIDATION_SUMMARY_TABLE} (
    src_warehouse STRING,
    src_table STRING,
    tgt_warehouse STRING,
    tgt_table STRING,
    final_validation_status STRING,
    primary_key_compliance_status STRING,
    ordinal_position_compliance_status STRING,
    col_name_compare_status STRING,
    data_type_compare_status STRING,
    comment_compare_status STRING,
    data_type_compatibility_status STRING,
    mismatches_status STRING,
    mismatches_after_exclusion_status STRING,
    extras_status STRING,
    col_mapping STRING,
    tgt_primary_keys STRING,
    exclude_fields_from_summary STRING,
    date_bucket STRING,
    mismatch_exclude_fields STRING,
    src_sql_override STRING, 
    tgt_sql_override STRING,
    src_data_load_filter STRING,
    tgt_data_load_filter STRING,
    validation_data_db STRING,
    streamlit_user_name STRING,
    streamlit_user_email STRING,
    favorite BOOLEAN,
    metrics STRUCT<
      src_records: BIGINT,
      tgt_records: BIGINT,
      src_delta_table_size_bytes: BIGINT,
      tgt_delta_table_size_bytes: BIGINT,
      src_extras: BIGINT,
      tgt_extras: BIGINT,
      mismatches: BIGINT,
      matches: BIGINT
      >,
    hash_metrics STRUCT<
      src_records: BIGINT,
      tgt_records: BIGINT,
      src_delta_table_size_bytes: BIGINT,
      tgt_delta_table_size_bytes: BIGINT,
      src_extras: BIGINT,
      tgt_extras: BIGINT,
      mismatches: BIGINT,
      matches: BIGINT
      >,
    quick_validation BOOLEAN
  )PARTITIONED BY (iteration_name STRING, workflow_name STRING, table_family STRING)
    ''')
except Exception as e:
  print(str(e).split("\n")[0])

# COMMAND ----------

try:
  spark.sql(f'''
  CREATE TABLE {TABLE_HASH_ANOMALIES} (
  comparison_type STRING,
  p_keys STRING,
  src_row_hash STRING,
  tgt_row_hash STRING,
  run_timestamp TIMESTAMP
)
PARTITIONED BY (iteration_name STRING, workflow_name STRING, table_family STRING)
''')
except Exception as e:
  print(str(e).split("\n")[0])