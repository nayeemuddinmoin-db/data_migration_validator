##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

CLUSTER_ID = "0923-090922-8zm5hdks"
JOB_ID = 631829696505080
DATABRICKS_INSTANCE = "https://adb-984752964297111.11.azuredatabricks.net"

# ACCESS_TOKEN_SCOPE = ""
# ACCESS_TOKEN_KEY = ""

VALIDATION_SYSTEM_DB = "cat_ril_nayeem_02.dmvdbpath_system"
VALIDATION_METRICS_DB = "cat_ril_nayeem_02.dmvdbpath_system_metrics"
VALIDATION_EXAMPLE_DB = "cat_ril_nayeem_02.dmvdbpath_system_example"

VALIDATION_MAPPING_TABLE = f"{VALIDATION_SYSTEM_DB}.validation_mapping"
TABLE_CONFIG_TABLE = f"{VALIDATION_SYSTEM_DB}.tables_config"
DATA_TYPE_COMPATIBILITY_MATRIX = f"{VALIDATION_SYSTEM_DB}.db_data_type_compatibility_matrix"
VALIDATION_LOG_TABLE = f"{VALIDATION_SYSTEM_DB}.validation_log_table_KUSH_TEST" 
VALIDATION_SUMMARY_TABLE = f"{VALIDATION_METRICS_DB}.validation_summary_table_KUSH_TEST"
SNOWFLAKE_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.snowflake_schema_store"
NETEZZA_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.netezza_schema_store"
DATABRICKS_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.databricks_schema_store"
ORACLE_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.oracle_schema_store"
MSSQL_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.mssql_schema_store"
TERADATA_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.teradata_schema_store"
HIVE_SCHEMA_STORE = f"{VALIDATION_METRICS_DB}.hive_schema_store"
MISMATCH_METRICS = f"{VALIDATION_METRICS_DB}.mismatch_metrics"
PRIMARY_KEY_VALIDATION = f"{VALIDATION_METRICS_DB}.primary_key_validation"
WINDOWED_VALIDATION_METRICS = f"{VALIDATION_METRICS_DB}.windowed_validation_metrics"
UPDATE_TSTMP_TIMELINE = f"{VALIDATION_METRICS_DB}.update_tstmp_timeline"
TABLE_HASH_ANOMALIES = f"{VALIDATION_METRICS_DB}.table_hash_anomalies"