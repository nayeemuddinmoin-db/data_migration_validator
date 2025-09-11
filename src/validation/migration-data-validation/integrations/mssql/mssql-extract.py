# Databricks notebook source
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################
 

# COMMAND ----------

import json
def readMSSql(query, db_name, table, jdbc_options):
 
    # jdbc_options = json.loads(jdbc_options)
    hostname = jdbc_options["hostname"]
    catalog = jdbc_options["database"]
    additional_options = jdbc_options["additional_options"]
    # url = "jdbc:sqlserver://" +hostname + ":1433;DatabaseName=" + catalog + ";encrypt=true;trustServerCertificate=true;"
 
    user = dbutils.secrets.get(
        scope=jdbc_options["user"]["secret_scope"], key=jdbc_options["user"]["key"]
    )
    password = dbutils.secrets.get(
        scope=jdbc_options["password"]["secret_scope"],
        key=jdbc_options["password"]["key"],
    )
    reader_options = {
        "host": hostname,
        "port": 1433,
        "user": user,
        "password": password,
        "database":catalog,
        "query": query,
        "fetchsize": "20000",
        # "encrypt": "true",
        # "trustServerCertificate": "true"
    }

    final_reader_options = {**reader_options, **jdbc_options["additional_options"]}
 
    df = (
        spark.read.format("sqlserver")
        .options(**final_reader_options)
        .load()
    )
    return df

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp
 
 
def captureMSSqlSchema(table, jdbc_options):
    table_name = table.split(".")[-1]
    db_name = table.split(".")[-2]
    # catalog_name =  table.split(".")[-3]
    print(f"Capturing MSSQL Schema for the table: {table}")
    # query = f"""select * from information_schema.columns where table_schema='{db_name}' and table_name='{table_name}'"""
    query = f'''
    SELECT 
    a.*,
    b.ColumnComment AS COLUMN_COMMENT
FROM 
    information_schema.columns a
LEFT JOIN
    (select     
    o.name AS Table_Name,
    c.name AS ColumnName,
    CONVERT(VARCHAR(MAX), ep.value) AS ColumnComment from
    sys.columns c
LEFT JOIN 
    sys.objects o ON c.object_id = o.object_id
LEFT JOIN 
    sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
    where lower(ep.name) = 'ms_description'
)b ON 
    a.TABLE_NAME = b.Table_Name and
    a.COLUMN_NAME = b.ColumnName
    where a.table_schema='{db_name}' and a.table_name='{table_name}'
'''
    # print(query)
    df = readMSSql(query, db_name, table, jdbc_options)
    print(f'{table_name}: {df.show()}')
    
    return df

# COMMAND ----------

def captureMSSqlTable(table, jdbc_options, data_load_filter, src_cast_to_string):
    load_filter = data_load_filter if (not data_load_filter is None) else "1=1"
    table_name = table.split(".")[-1]
    db_name = table.split(".")[-2]
    # catalog_name =  table.split(".")[-3]
    query = f"""Select * from {table} where {load_filter}"""
 
    df = readMSSql(query, db_name, table, jdbc_options)
    to_str = df.columns
    if src_cast_to_string:
    #Convert all fields to String
        for col in to_str:
            df = df.select([df[c].cast(StringType()).alias(c) for c in to_str])
    return df
