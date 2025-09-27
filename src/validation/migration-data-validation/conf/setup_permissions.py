# Databricks notebook source
# MAGIC %md
# MAGIC ### 1.Constants Setup

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Grant can_restart permission to run_as user or spn on cluster for the job

# COMMAND ----------

if JOB_CLUSTER_ID is not None:

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.compute import (
        ClusterAccessControlRequest,
        ClusterPermissionLevel
    )
    
    w = WorkspaceClient()
    
    w.clusters.set_permissions(
        cluster_id=JOB_CLUSTER_ID,
        access_control_list=[
            ClusterAccessControlRequest(
                user_name=JOB_RUN_AS_USER,
                permission_level=ClusterPermissionLevel.CAN_RESTART
            )
        ]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Grant can_run permission to run_as user or spn on the code directory

# COMMAND ----------

# from databricks.sdk.service.workspace import WorkspaceAccessControlRequest

# FOLDER_PATH = "/Workspace/Users/nayeemuddin.moinuddin@databricks.com/.bundle/data_migration_validator_app"
# USER_EMAIL = "kushagra.parashar@databricks.com"

# w.workspace.set_permissions(
#     path=FOLDER_PATH,
#     access_control_list=[
#         WorkspaceAccessControlRequest(
#             user_name=USER_EMAIL,
#             permission_level="CAN_RUN"
#         )
#     ]
# )
print("IF USER DOES NOT HAVE THIS PERMISSION WE MIGHT HAVE TO DO IT MANUALLY...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.Grant use catalog, use schema on ingestion ops catalog and dmv catalog

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG, USE SCHEMA ON CATALOG `{INGESTION_OPS_CATALOG}` TO `{JOB_RUN_AS_USER}`")
spark.sql(f"GRANT USE CATALOG, USE SCHEMA ON CATALOG `{VALIDATION_OPS_CATALOG}` TO `{JOB_RUN_AS_USER}`")
spark.sql(f"GRANT USE CATALOG, USE SCHEMA ON CATALOG `{VALIDATION_OPS_CATALOG}` TO `{APP_SPN}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Grant select on ingestion ops

# COMMAND ----------

spark.sql(f"GRANT SELECT ON SCHEMA {INGESTION_OPS_SCHEMA} TO `{JOB_RUN_AS_USER}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.Grant select/modify on dmv system and metrics schema

# COMMAND ----------

spark.sql(f"GRANT SELECT,MODIFY,CREATE TABLE ON SCHEMA {VALIDATION_DB} TO `{JOB_RUN_AS_USER}`")
spark.sql(f"GRANT READ VOLUME ON SCHEMA {VALIDATION_DB} TO `{APP_SPN}`")
spark.sql(f"GRANT SELECT ON SCHEMA {VALIDATION_SYSTEM_DB} TO `{JOB_RUN_AS_USER}`")
spark.sql(f"GRANT SELECT,MODIFY,CREATE TABLE ON SCHEMA {VALIDATION_METRICS_DB} TO `{JOB_RUN_AS_USER}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.Grant modify on dmv validation log table

# COMMAND ----------

spark.sql(f"GRANT MODIFY ON TABLE {VALIDATION_LOG_TABLE} TO `{JOB_RUN_AS_USER}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.Grant read files on external locations where source files are available

# COMMAND ----------

###
# %sql
# GRANT SELECT ON ANY FILE TO `5087f00f-2031-47a1-88a4-32844bcd9cbb`;


# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.Grant select on target tables

# COMMAND ----------

target_table_list = spark.sql(f"select distinct target_catalog,target_schema from {INGESTION_CONFIG_TABLE}").collect()

# COMMAND ----------

for table in target_table_list:
    print(f"{table.target_catalog}.{table.target_schema}")
    spark.sql(f"GRANT USE CATALOG ON CATALOG {table.target_catalog} TO `{JOB_RUN_AS_USER}`")
    spark.sql(f"GRANT USE SCHEMA,SELECT ON SCHEMA {table.target_catalog}.{table.target_schema} TO `{JOB_RUN_AS_USER}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.Grant select/modify on DMV validation mapping and config table to App SPN

# COMMAND ----------

spark.sql(f"GRANT SELECT,MODIFY ON TABLE {VALIDATION_MAPPING_TABLE} TO `{APP_SPN}`")
spark.sql(f"GRANT SELECT,MODIFY ON TABLE {TABLE_CONFIG_TABLE} TO `{APP_SPN}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.Grant select on DMV Schema to App SPN

# COMMAND ----------

spark.sql(f"GRANT SELECT ON SCHEMA {VALIDATION_DB} TO `{APP_SPN}`")
spark.sql(f"GRANT SELECT ON SCHEMA {VALIDATION_SYSTEM_DB} TO `{APP_SPN}`")
spark.sql(f"GRANT SELECT ON SCHEMA {VALIDATION_METRICS_DB} TO `{APP_SPN}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.Grant can_restart permission to App SPN on App Cluster

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    ClusterAccessControlRequest,
    ClusterPermissionLevel
)

w = WorkspaceClient()

w.clusters.set_permissions(
    cluster_id=APP_CLUSTER_ID,
    access_control_list=[
        ClusterAccessControlRequest(
            user_name=APP_SPN,
            permission_level=ClusterPermissionLevel.CAN_RESTART
        )
    ]
)