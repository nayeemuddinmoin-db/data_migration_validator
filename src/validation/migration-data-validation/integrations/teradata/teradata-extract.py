# Databricks notebook source
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# COMMAND ----------

def readTeradata(query, db_name, table, jdbc_options):

    # jdbc_options = json.loads(jdbc_options)
    hostname = jdbc_options["hostname"]
    port = jdbc_options["port"]
    url = f"jdbc:teradata://{hostname}/DATABASE={db_name},DBS_PORT={port}"
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
        .option("driver", "com.teradata.jdbc.TeraDriver")
        .options(**reader_options)
        .load()
    )
    return df

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp


def captureTeradataSchema(table, jdbc_options):
    table_name = table.split(".")[-1]
    db_name = table.split(".")[0]
    print(f"Capturing Teradata Schema for the table: {table}")
    query = f"""(SELECT
      DatabaseName,
      TableName,
      ColumnName,
      ColumnId,
      ROW_NUMBER() OVER (order by ColumnId) AS ColumnOrder,
      CASE
        WHEN ColumnType = '++' THEN 'TD_ANYTYPE'
        WHEN ColumnType = 'A1' THEN 'ARRAY'
        WHEN ColumnType = 'AN' THEN 'ARRAY'
        WHEN ColumnType = 'AT' THEN 'TIME'
        WHEN ColumnType = 'BF' THEN 'BYTE'
        WHEN ColumnType = 'BO' THEN 'BLOB'
        WHEN ColumnType = 'BV' THEN 'VARBYTE'
        WHEN ColumnType = 'CF' THEN 'CHAR'
        WHEN ColumnType = 'CO' THEN 'CLOB'
        WHEN ColumnType = 'CV' THEN 'VARCHAR'
        WHEN ColumnType = 'D' THEN 'DECIMAL'
        WHEN ColumnType = 'DA' THEN 'DATE'
        WHEN ColumnType = 'DH' THEN 'INTERVAL DAY TO HOUR'
        WHEN ColumnType = 'DM' THEN 'INTERVAL DAY TO MINUTE'
        WHEN ColumnType = 'DS' THEN 'INTERVAL DAY TO SECOND'
        WHEN ColumnType = 'DT' THEN 'DATASET'
        WHEN ColumnType = 'DY' THEN 'INTERVAL DAY'
        WHEN ColumnType = 'F' THEN 'FLOAT'
        WHEN ColumnType = 'HM' THEN 'INTERVAL HOUR TO MINUTE'
        WHEN ColumnType = 'HR' THEN 'INTERVAL HOUR'
        WHEN ColumnType = 'HS' THEN 'INTERVAL HOUR TO SECOND'
        WHEN ColumnType = 'I1' THEN 'BYTEINT'
        WHEN ColumnType = 'I2' THEN 'SMALLINT'
        WHEN ColumnType = 'I8' THEN 'BIGINT'
        WHEN ColumnType = 'I' THEN 'INTEGER'
        WHEN ColumnType = 'JN' THEN 'JSON'
        WHEN ColumnType = 'MI' THEN 'INTERVAL MINUTE'
        WHEN ColumnType = 'MO' THEN 'INTERVAL MONTH'
        WHEN ColumnType = 'MS' THEN 'INTERVAL MINUTE TO SECOND'
        WHEN ColumnType = 'N' THEN 'NUMBER'
        WHEN ColumnType = 'PD' THEN 'PERIOD(DATE)'
        WHEN ColumnType = 'PM' THEN 'PERIOD(TIMESTAMP WITH TIME ZONE)'
        WHEN ColumnType = 'PS' THEN 'PERIOD(TIMESTAMP)'
        WHEN ColumnType = 'PT' THEN 'PERIOD(TIME)'
        WHEN ColumnType = 'PZ' THEN 'PERIOD(TIME WITH TIME ZONE)'
        WHEN ColumnType = 'SC' THEN 'INTERVAL SECOND'
        WHEN ColumnType = 'SZ' THEN 'TIMESTAMP WITH TIME ZONE'
        WHEN ColumnType = 'TS' THEN 'TIMESTAMP'
        WHEN ColumnType = 'TZ' THEN 'TIME WITH TIME ZONE'
        WHEN ColumnType = 'UT' THEN 'UDT'
        WHEN ColumnType = 'XM' THEN 'XML'
        WHEN ColumnType = 'YM' THEN 'INTERVAL YEAR TO MONTH'
        WHEN ColumnType = 'YR' THEN 'INTERVAL YEAR'
      END AS Data_Type,
      ColumnType,
      --Fix to handle Teradata's Char/Varchar Length difference between the Information Schema and the Desc results.
      CASE
        WHEN CharType = 1 THEN ColumnLength -- LATIN CHAR/VARCHAR
        WHEN CharType = 2 THEN ColumnLength / 2 -- UNICODE CHAR/VARCHAR
        ELSE ColumnLength
        END AS ColumnLength,
      DecimalTotalDigits,
      DecimalFractionalDigits,
      Nullable,
      CommentString
    FROM
      DBC.ColumnsV
      where DatabaseName = '{db_name}' and TableName ='{table_name}') a"""
    df = readTeradata(query, db_name, table, jdbc_options)
    print(f'{table_name}: {df.show()}')

    return df

# COMMAND ----------

def captureTeradataTable(table, jdbc_options, data_load_filter, src_cast_to_string):
    load_filter = data_load_filter if (not data_load_filter is None) else "1=1"
    query = f"""(Select * from {table} where {load_filter}) a"""
    db_name = table.split(".")[0]
    df = readTeradata(query, db_name, table, jdbc_options)
    to_str = df.columns
    print(to_str)
    if src_cast_to_string:
        #Convert all fields to String
        for col in to_str:
            df = df.select([df[c].cast(StringType()).alias(c) for c in to_str])
    return df
