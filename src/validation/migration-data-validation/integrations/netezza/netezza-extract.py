# Databricks notebook source
# DBTITLE 1,Description
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# COMMAND ----------

def readNetezza(query, db_name, table, jdbc_options):

    # jdbc_options = json.loads(jdbc_options)
    hostname = jdbc_options["hostname"]
    url = f"jdbc:netezza://{hostname}/{db_name}"
    user = dbutils.secrets.get(
        scope=jdbc_options["user"]["secret_scope"], key=jdbc_options["user"]["key"]
    )
    password = dbutils.secrets.get(
        scope=jdbc_options["password"]["secret_scope"],
        key=jdbc_options["password"]["key"],
    )

    reader_options = {
        "url": url,
        "dbtable": query,
        "user": user,
        "password": password,
        "fetchsize": "10000",
        "numPartitions": "9",
    }

    df = (
        spark.read.format("jdbc")
        .option("driver", "org.netezza.Driver")
        .options(**reader_options)
        .load()
    )
    return df

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp


def captureNetezzaSchema(table, jdbc_options):
    table_name = table.split(".")[-1]
    db_name = table.split(".")[0]
    print(f"Capturing Netezza Schema for the table: {table}")
    query = f"""(Select * from _v_relation_column where name ='{table_name}') a"""
    df = readNetezza(query, db_name, table, jdbc_options)
    
    return df

# COMMAND ----------

def captureNetezzaTable(table, jdbc_options, data_load_filter, src_cast_to_string):
    load_filter = data_load_filter if (not data_load_filter is None) else "1=1"
    query = f"""(Select * from {table} where {load_filter}) a"""
    db_name = table.split(".")[0]
    df = readNetezza(query, db_name, table, jdbc_options)
    to_str = df.columns
    if src_cast_to_string:
        #Convert all fields to String
        for col in to_str:
            df = df.select([df[c].cast(StringType()).alias(c) for c in to_str])
    return df
