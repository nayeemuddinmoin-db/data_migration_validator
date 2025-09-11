# Databricks notebook source
# DBTITLE 1,Description
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# COMMAND ----------

from pyspark.sql.functions import *


def readSnowflake(query, database, table, schema, jdbc_options):

    # jdbc_options = json.loads(jdbc_options)

    url = jdbc_options["url"]
    username = dbutils.secrets.get(
        scope=jdbc_options["user"]["secret_scope"], key=jdbc_options["user"]["key"]
    )
    private_key = dbutils.secrets.get(
        scope=jdbc_options["private_key"]["secret_scope"],
        key=jdbc_options["private_key"]["key"],
    )
    warehouse = jdbc_options["warehouse"]
    sfRole = jdbc_options["sfRole"]

    sfOptions = {
        "sfUrl": url,
        "sfUser": username,
        "pem_private_key": private_key,
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": warehouse,
        "autopushdown": "on",
        "sfRole": sfRole,
    }

    snowflake_table = (
        spark.read.format("snowflake")
        .options(**sfOptions)
        .option("query", query)
        .load()
    )
    return snowflake_table

# COMMAND ----------

import traceback
from pyspark.sql.functions import lit, to_timestamp


def captureSnowflakeSchema(table, jdbc_options):
    try:
        database, schema, table_name = table.split(".")
    except ValueError:
        print("Provide the Fully Qualified Names for the Snowflake Table")
        traceback.print_exc()
    print(f"Capturing Snowflake Schema for the table: {table}")
    query = f"select * from {database}.information_schema.columns where lower(TABLE_NAME)=lower('{table_name}')"
    information_schema = readSnowflake(query, database, table, schema, jdbc_options)
    
    return information_schema

# COMMAND ----------

def captureSnowflakeTable(table, jdbc_options, data_load_filter, src_cast_to_string):
    try:
        database, schema, table_name = table.split(".")
    except ValueError:
        print("Provide the Fully Qualified Names for the Snowflake Table")
        traceback.print_exc()
    print(f"Capturing Snowflake Contents for the table: {table}")
    load_filter = data_load_filter if (not data_load_filter is None) else "1=1"
    query = f"""(Select * from {table} where {load_filter}) a"""
    df = readSnowflake(query, database, table, schema, jdbc_options)
    to_str = df.columns
    if src_cast_to_string:
        #Convert all fields to String
        for col in to_str:
            df = df.select([df[c].cast(StringType()).alias(c) for c in to_str])
    return df
