# Databricks notebook source
# MAGIC %run ./constants

# COMMAND ----------

dbutils.notebook.run("./setup_ddls", 600)

# COMMAND ----------

dbutils.notebook.run("./setup_validation_mapping", 600)

# COMMAND ----------

dbutils.notebook.run("./setup_generate_job_params", 600)

# COMMAND ----------

dbutils.notebook.run("./setup_permissions", 600)