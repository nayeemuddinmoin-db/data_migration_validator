# Databricks notebook source
# # Upgrade Databricks SDK to the latest version and restart Python to see updated packages
# %pip install --upgrade databricks-sdk==0.49.0
# %restart_python

# COMMAND ----------

# MAGIC %pip install /Volumes/cat_ril_nayeem_03/dmvdb/lib/databricks_sdk-0.67.0-py3-none-any.whl --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

import os
workspace_path = os.getcwd()
root_path = workspace_path.replace("conf","")
notebook_path = root_path + "/migration-data-validation"
notebook_path

# COMMAND ----------

from databricks.sdk.service.jobs import JobSettings as Job
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

wf_dmv_validator_runner_nayeem = Job.from_dict(
    {
        "name": VALIDATION_WORKFLOW_NAME,
        "max_concurrent_runs": 10,
        "tasks": [
            {
                "task_key": "tsk-dmv-validator-run",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "00-triggered_from_workflow": "",
                        "01-iteration_name_suffix": "",
                        "02-workflow_name": "",
                        "03-workflow:table_family": "",
                        "04-user_name": "",
                        "05-user_email": "",
                    },
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "data_mig_validator_job_cluster",
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "data_mig_validator_job_cluster",
                "new_cluster": {
                    "spark_version": "16.4.x-scala2.12",
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1,
                    },
                    "node_type_id": "Standard_E16ads_v5",
                    "driver_node_type_id": "Standard_E16ads_v5",
                    "custom_tags": {
                        "BusinessContact": "xyz@ril.com",
                        "LOB": "Connectivity",
                        "ProjectName": "DBX",
                    },
                    "enable_elastic_disk": True,
                    "data_security_mode": "USER_ISOLATION",
                    "runtime_engine": "STANDARD",
                    "autoscale": {
                        "min_workers": 1,
                        "max_workers": 4,
                    },
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "run_as": {
            "user_name": JOB_RUN_AS_USER,
        }
    }
)

# COMMAND ----------

dmv_job = w.jobs.create(**wf_dmv_validator_runner_nayeem.as_shallow_dict())

# COMMAND ----------

dmv_job

# COMMAND ----------

from databricks.sdk.service.jobs import JobAccessControlRequest,JobPermissionLevel
curr_user = spark.sql("select current_user").first()[0]
w.jobs.set_permissions(
    job_id=dmv_job.job_id,
    access_control_list=[
        JobAccessControlRequest(
            user_name=JOB_RUN_AS_USER,
            permission_level=JobPermissionLevel.CAN_MANAGE_RUN
        ),
        JobAccessControlRequest(
            service_principal_name=APP_SPN,
            permission_level=JobPermissionLevel.CAN_MANAGE_RUN
        ),
        JobAccessControlRequest(
            user_name=curr_user,
            permission_level=JobPermissionLevel.IS_OWNER
        ),
    ]
)