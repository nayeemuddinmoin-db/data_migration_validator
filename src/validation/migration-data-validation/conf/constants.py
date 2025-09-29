# Databricks notebook source
# DBTITLE 1,Description
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# COMMAND ----------

APP_SPN = "a4840b46-a1c1-4e3d-b9fc-6b725ab4b94a"
JOB_RUN_AS_USER = "rahul.singha@databricks.com" # App ID in case of SPN "5087f00f-2031-47a1-88a4-32844bcd9cbb" 
APP_CLUSTER_ID = "0927-122932-wl2072gn" # APP CLUSTER ID
JOB_CLUSTER_ID = None
JOB_ID = 919069293546107  # Validation Workflow
EXTERNAL_LOCATIONS = None

# COMMAND ----------

PARALLELISM = 30
# INGESTION_METADATA_TABLE = "cat_ril_nayeem_01.rildb01.source_file_metadata"
INGESTION_OPS_CATALOG = "ts42_demo"
INGESTION_OPS_SCHEMA = "ts42_demo.migration_operations"

INGESTION_METADATA_TABLE = f"{INGESTION_OPS_SCHEMA}.serving_ingestion_metadata"
INGESTION_AUDIT_TABLE = f"{INGESTION_OPS_SCHEMA}.serving_ingestion_audit"
INGESTION_CONFIG_TABLE = f"{INGESTION_OPS_SCHEMA}.serving_ingestion_config"
INGESTION_SRC_TABLE_PARTITION_MAPPING= f"{INGESTION_OPS_SCHEMA}.source_table_partition_mapping"

# COMMAND ----------

VALIDATION_OPS_CATALOG = "cat_ril_nayeem_03"

VALIDATION_DB = "cat_ril_nayeem_03.dmvdb"
VALIDATION_SYSTEM_DB = "cat_ril_nayeem_03.dmvdb_system"
VALIDATION_METRICS_DB = "cat_ril_nayeem_03.dmvdb_system_metrics"
# VALIDATION_EXAMPLE_DB = "cat_ril_nayeem_02.dmvdbpath_system_example"

# COMMAND ----------

#### Below constants will get auto setup based on above values

# COMMAND ----------

VALIDATION_MAPPING_TABLE = f"{VALIDATION_SYSTEM_DB}.validation_mapping"
TABLE_CONFIG_TABLE = f"{VALIDATION_SYSTEM_DB}.tables_config"
DATA_TYPE_COMPATIBILITY_MATRIX = f"{VALIDATION_SYSTEM_DB}.db_data_type_compatibility_matrix"
VALIDATION_LOG_TABLE = f"{VALIDATION_SYSTEM_DB}.validation_log_table"
VALIDATION_SUMMARY_TABLE = f"{VALIDATION_METRICS_DB}.validation_summary_table"
DATABRICKS_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.databricks_schema_store"

SNOWFLAKE_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.snowflake_schema_store"
NETEZZA_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.netezza_schema_store"
ORACLE_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.oracle_schema_store"
MSSQL_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.mssql_schema_store"
TERADATA_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.teradata_schema_store"
HIVE_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.hive_schema_store"

MISMATCH_METRICS = f"{VALIDATION_METRICS_DB}.mismatch_metrics"
PRIMARY_KEY_VALIDATION = f"{VALIDATION_METRICS_DB}.primary_key_validation"
WINDOWED_VALIDATION_METRICS = f"{VALIDATION_METRICS_DB}.windowed_validation_metrics"
UPDATE_TSTMP_TIMELINE = f"{VALIDATION_METRICS_DB}.update_tstmp_timeline"
TABLE_HASH_ANOMALIES = f"{VALIDATION_METRICS_DB}.table_hash_anomalies"




