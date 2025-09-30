# Databricks notebook source
# MAGIC %run ./constants

# COMMAND ----------

import os

with open("requirements.template.txt") as f:
    content = f.read().replace("__VOLUME_PATH__", APP_LIB_VOLUME_PATH)

with open("../databricks_apps/streamlit-data-migration-validator/requirements.txt", "w") as f:
    f.write(content)