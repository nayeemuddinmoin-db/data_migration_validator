# Databricks notebook source
# DBTITLE 1,Description
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp, coalesce


def captureDatabricksSchema(tbl, path=None):
    catalog = "source_system"
    splits = tbl.split(".")
    #detect the uc and legacy namespace scheme
    if len(splits) == 2:
        db_name, tbl_name = splits
    elif len(splits) == 3:
        catalog, db_name, tbl_name = splits
    print(f"Capturing Databricks Schema for the table: {tbl}")
    if path is None:
        tbl_schema = spark.sql(f"desc {tbl}")
    else:
        print("Path based read Op...")
        from pyspark.sql.types import StructType, StructField, StringType

        # Define the schema
        schema = StructType([
            StructField("col_name", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("comment", StringType(), True)
        ])

        # Read the ORC file into a DataFrame
        df = spark.read.format("orc").load(f'{path}')

        # Convert schema to dataframe with explicit schema
        tbl_schema = spark.createDataFrame(
            [(f.name, f.dataType.simpleString(), f.metadata.get("comment", None)) for f in df.schema.fields],
            schema
        )

    
    print(f'{tbl}: {tbl_schema.show()}')
    tbl_schema.createOrReplaceTempView(f"tbl_schema_{tbl_name}")
    tbl_schema_with_row_num = spark.sql(
            f"""select '{catalog}' as catalog, '{db_name}' as db_name, '{tbl_name}' as table_name, original_order, col_name, data_type, comment from (SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY col_name
                    ORDER BY
                    original_order
                ) AS rn
                FROM
                (
                    select
                    row_number() over (
                        order by
                        a
                    ) as original_order,
                    col_name as col_name,
                    data_type as data_type,
                    `comment` as
                    comment
                    from
                    (
                        select
                        monotonically_increasing_id() a,
                        *
                        from
                        tbl_schema_{tbl_name}
                    )
                    where
                    -- to address partitioning and clustering columns
                    data_type not in ('data_type', '')
                ))x where rn = 1"""
    )

    return tbl_schema_with_row_num

# COMMAND ----------

import json
from pyspark.sql.functions import coalesce,col,lit
from pyspark.sql import functions as F
def processDatabricksColNames(table, col_mapping, primary_keys_string, mismatch_exclude_fields, path=None):
  cm = col_mapping
  # mismatch_exclude_fields_compiled = mismatch_exclude_fields_string.format(**locals())
  # mismatch_exclude_fields = [field.strip() for field in mismatch_exclude_fields_compiled.split("|")]


  # column_sql = f"SHOW COLUMNS FROM {table}"
  if path is None:
    column_sql = f"SELECT * FROM {table} where 1=0"
    # columns = runSimpleHiveSQL(column_sql, jdbc_options_json)
    columns = spark.sql(column_sql).columns
  else:
    columns = table.columns

  
  # Remove entries against mismatch_exclude_fields
  columns = [col for col in columns if col not in mismatch_exclude_fields]
  col_dict = {}
  # replace the column names with the mapped column names from the user
  col_dict = {col: cm.get(col, col) for col in columns}
  
  # Sorting dictionary by values (ascending order)
  sorted_col = dict(sorted(col_dict.items(), key=lambda item: item[1]))
  if path is None:
    col_cast_list = ", ".join([f"COALESCE(CAST({key} AS STRING),'') as {key}" for key in sorted_col.keys()])
  else:
    col_cast_list = [coalesce(F.col(c).cast("string"), F.lit("")).alias(c) for c in sorted_col.keys()]
  col_list = ", ".join(sorted_col.keys())
  return col_list, col_cast_list


# COMMAND ----------

def captureDatabricksTableHash(table, primary_keys_string, mismatch_exclude_fields, sql_override, data_load_filter, table_mapping, path=None):

  col_mapping = table_mapping.col_mapping
  cm = col_mapping
  load_filter = data_load_filter if (not data_load_filter is None) else "1=1"

  if path is None:
    read_sql = sql_override if (not sql_override is None) else f"select * from {table}"
    read_sql_compiled = f"(SELECT a.* FROM ({read_sql.format(**locals())})a where {load_filter})b"
  else:
    read_sql_compiled = spark.read.format("orc").load(path).where(f"{load_filter}")

  col_list, col_cast_list = processDatabricksColNames(read_sql_compiled, col_mapping, primary_keys_string, mismatch_exclude_fields, path)

  # Handle case where primary_keys_string is empty (for hash-based validation)
  if path is None:
    if primary_keys_string and primary_keys_string.strip():
      p_keys_expr = f'concat_ws(":",{primary_keys_string}) as p_keys'
      row_hash_expr = f'sha2(concat_ws(":",{col_list}),256) as row_hash'
    else:
      # For hash-based validation without primary keys, use the hash itself as the unique identifier
      # Use the same hash for both p_keys and row_hash
      hash_expr = f'sha2(concat_ws(":",{col_list}),256)'
      p_keys_expr = f'{hash_expr} as p_keys'
      row_hash_expr = f'{hash_expr} as row_hash'
    
    sql = f"""SELECT {p_keys_expr}, {row_hash_expr} from (SELECT {col_cast_list} from {read_sql_compiled})c"""
    print(sql)
    df = spark.sql(sql)
  else:
    if primary_keys_string and primary_keys_string.strip():
      p_keys_expr = f'concat_ws(":", {primary_keys_string}) as p_keys'
      row_hash_expr = f'sha2(concat_ws(":", {col_list}), 256) as row_hash'
    else:
      # For hash-based validation without primary keys, use the hash itself as the unique identifier
      # Use the same hash for both p_keys and row_hash
      hash_expr = f'sha2(concat_ws(":", {col_list}), 256)'
      p_keys_expr = f'{hash_expr} as p_keys'
      row_hash_expr = f'{hash_expr} as row_hash'
    
    df = read_sql_compiled.select(col_cast_list).selectExpr(
    p_keys_expr,
    row_hash_expr
    )
  
  df.show()
  return df


# COMMAND ----------

from pyspark.sql.types import StringType    
def captureDatabricksTable(table, sql_override, data_load_filter, src_cast_to_string,path=None):
    load_filter = data_load_filter if (not data_load_filter is None) else "1=1"
    read_sql = sql_override if (not sql_override is None) else f"select * from {table}"
    if path is None:
        read_sql_compiled = f"SELECT a.* FROM ({read_sql.format(**locals())})a where {load_filter}"
        print(f"{read_sql}\n{read_sql_compiled}")
        print(f"Capturing Databricks Contents for the table: {table}")
        df = spark.sql(read_sql_compiled)
    else:
        print("Reading src from path op...")
        df = spark.read.format("orc").load(path).where(f"{load_filter}")
    # df = spark.read.table(table).filter(load_filter)
    to_str = df.columns
    if src_cast_to_string:
    #Convert all fields to String
        for col in to_str:
            df = df.select([df[c].cast(StringType()).alias(c) for c in to_str])
    return df