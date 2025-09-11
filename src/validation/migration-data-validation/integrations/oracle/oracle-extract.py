# Databricks notebook source
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# COMMAND ----------

def readOracle(query, schema_name, table, jdbc_options):

    # jdbc_options = json.loads(jdbc_options)
    hostname = jdbc_options["hostname"]
    port = jdbc_options["port"]
    service_name = jdbc_options["service_name"]
    url = f"jdbc:oracle:thin:@//{hostname}:{port}/{service_name}"
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
        "numPartitions": "100",
    }

    df = (
        spark.read.format("jdbc")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .options(**reader_options)
        .load()
    )
    return df

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp


def captureOracleSchema(table, jdbc_options):
    table_name = table.split(".")[-1]
    schema_name = table.split(".")[0]
    print(f"Capturing Oracle Schema for the table: {table}")
    query = f"""(SELECT OWNER, a.TABLE_NAME, a.COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD,DATA_TYPE_OWNER, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID, DEFAULT_LENGTH, DATA_DEFAULT, b.COMMENTS
            FROM all_tab_columns a LEFT OUTER JOIN USER_COL_COMMENTS b ON a.TABLE_NAME = b.TABLE_NAME AND a.COLUMN_NAME = b.COLUMN_NAME
            WHERE a.table_name = '{table_name}') a"""
    df = readOracle(query, schema_name, table, jdbc_options)
    print(f'{table_name}: {df.show()}')

    return df

# COMMAND ----------

def captureOracleTable(table, jdbc_options, data_load_filter, src_cast_to_string):
    load_filter = data_load_filter if (not data_load_filter is None) else "1=1"
    query = f"""(Select * from {table} where {load_filter}) a"""
    db_name = table.split(".")[0]
    df = readOracle(query, db_name, table, jdbc_options)
    to_str = df.columns
    if src_cast_to_string:
        # Convert all fields to String
        for col in to_str:
            df = df.select([df[c].cast(StringType()).alias(c) for c in to_str])
    return df
