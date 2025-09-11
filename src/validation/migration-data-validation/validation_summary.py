# Databricks notebook source
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# COMMAND ----------

# MAGIC %run "./conf/constants"

# COMMAND ----------

dbutils.widgets.text("01-iteration_name", "")
iteration_name = dbutils.widgets.get("01-iteration_name")


# COMMAND ----------


# def readValidationLogList():
#   config_sql = f"""select a.*, b.mismatch_exclude_fields from {VALIDATION_LOG_TABLE} a left outer join {VALIDATION_MAPPING_TABLE} b on 
#   a.workflow_name = b.workflow_name
#   AND a.src_warehouse = b.src_warehouse
#   AND a.src_table = b.src_table
#   AND a.tgt_warehouse = b.tgt_warehouse
#   AND a.tgt_table = b.tgt_table
#   AND a.table_family = b.table_family
#   where iteration_name = '{iteration_name}' and validation_run_status ='SUCCESS'"""
#   table_mappings = spark.sql(config_sql).collect()[0]
#   return table_mappings

# table_mapping = readValidationLogList()
# workflow_name = table_mapping["workflow_name"]
# src_wrhse = table_mapping["src_warehouse"]
# tgt_wrhse = table_mapping["tgt_warehouse"]
# src_tbl = table_mapping["src_table"]
# tgt_tbl = table_mapping["tgt_table"]
# table_family = table_mapping["table_family"]
# mismatch_exclude_fields_string = table_mapping["mismatch_exclude_fields"]

# print(src_wrhse, tgt_wrhse, src_tbl, tgt_tbl)
def runner(table_metrics):
  # global workflow_name, src_wrhse, src_tbl, tgt_wrhse, tgt_tbl, table_family, mismatch_exclude_fields_string

  print(f'''Triggering the summary report generation for workflow_name: {table_metrics["workflow_name"]} and table_family {table_metrics["table_family"]}''')
  schema_store_catalog_v()
  save_metrics(table_metrics)
  primary_key_validation_summary(table_metrics)
  ordinal_position_validation(table_metrics)
  schema_validation(table_metrics)
  data_type_compatibility_check(table_metrics)
  mismatch_status(table_metrics)
  extras_status(table_metrics)

# COMMAND ----------

def schema_store_catalog_v():
  sqls = []

  databricks = f'''CREATE OR REPLACE TEMPORARY VIEW schema_store_catalog_v as
  select "databricks" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",catalog,db_name,table_name) TABLE_NAME, original_order COL_ORDER, col_name COL_NAME, comment COL_COMMENT, data_type DATA_TYPE from {DATABRICKS_SCHEMA_STORE}'''

  netezza = f'''select "netezza" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",DATABASE,schema,Name) TABLE_NAME, ATTNUM COL_ORDER, ATTNAME COL_NAME, DESCRIPTION COL_COMMENT, FORMAT_TYPE DATA_TYPE from {NETEZZA_SCHEMA_STORE}'''

  snowflake = f'''select "snowflake" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".", TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) TABLE_NAME, ORDINAL_POSITION COL_ORDER, COLUMN_NAME COL_NAME, COMMENT COL_COMMENT, GENERATED_DATA_TYPE DATA_TYPE from (select * ,
    case
      lower(DATA_TYPE)
      when 'number' then concat(DATA_TYPE,"(",NUMERIC_PRECISION,",",NUMERIC_SCALE,")")
      when 'text' then concat(DATA_TYPE,"(",CHARACTER_MAXIMUM_LENGTH,")")
      else DATA_TYPE
    end as GENERATED_DATA_TYPE
  from {SNOWFLAKE_SCHEMA_STORE})x'''

  oracle = f'''select "oracle" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",OWNER,TABLE_NAME) TABLE_NAME, COLUMN_ID COL_ORDER, COLUMN_NAME COL_NAME, COMMENTS COL_COMMENT, DATA_TYPE DATA_TYPE from (select * ,
   case
     lower(DATA_TYPE)
     when 'number' then concat(DATA_TYPE,"(",DATA_PRECISION,",",DATA_SCALE,")")
     when 'varchar2' then concat(DATA_TYPE,"(",DATA_LENGTH,")")
     when 'nvarchar2' then concat(DATA_TYPE,"(",DATA_LENGTH,")")
     else DATA_TYPE
   end as GENERATED_DATA_TYPE
from {ORACLE_SCHEMA_STORE})x'''

  mssql = f'''select "mssql" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",TABLE_SCHEMA,TABLE_NAME) TABLE_NAME, ORDINAL_POSITION COL_ORDER, COLUMN_NAME COL_NAME, COLUMN_COMMENT COL_COMMENT, DATA_TYPE DATA_TYPE from {MSSQL_SCHEMA_STORE}'''

  teradata = f'''select "teradata" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",DatabaseName,TableName) TABLE_NAME, ColumnOrder COL_ORDER, ColumnName COL_NAME, CommentString COL_COMMENT, GENERATED_DATA_TYPE DATA_TYPE  from (select * ,
  case
    lower(DATA_TYPE)
    when 'decimal' then concat(DATA_TYPE,"(",DecimalTotalDigits,",",DecimalFractionalDigits,")")
    when 'byte' then concat(DATA_TYPE,"(",ColumnLength,")")
    when 'varbyte' then concat(DATA_TYPE,"(",ColumnLength,")")
    when 'char' then concat(DATA_TYPE,"(",ColumnLength,")")
    when 'varchar' then concat(DATA_TYPE,"(",ColumnLength,")")
    else DATA_TYPE
  end as GENERATED_DATA_TYPE 
from {TERADATA_SCHEMA_STORE})x'''
 
  hive = f'''select "hive" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",db_name,table_name) TABLE_NAME, original_order COL_ORDER, col_name COL_NAME, comment COL_COMMENT, data_type DATA_TYPE from {HIVE_SCHEMA_STORE}'''

  if spark.catalog.tableExists(DATABRICKS_SCHEMA_STORE):
    sqls.append(databricks)
  if spark.catalog.tableExists(NETEZZA_SCHEMA_STORE):
    sqls.append(netezza)
  if spark.catalog.tableExists(SNOWFLAKE_SCHEMA_STORE):
    sqls.append(snowflake)
  if spark.catalog.tableExists(ORACLE_SCHEMA_STORE):
    sqls.append(oracle)
  if spark.catalog.tableExists(MSSQL_SCHEMA_STORE):
    sqls.append(mssql)
  if spark.catalog.tableExists(TERADATA_SCHEMA_STORE):
    sqls.append(teradata)
  if spark.catalog.tableExists(HIVE_SCHEMA_STORE):
    sqls.append(hive)

  sql = " UNION ".join(sqls)
  spark.sql(sql)
  # spark.sql(f'''CREATE OR REPLACE TEMPORARY VIEW schema_store_catalog_v as
  # select "databricks" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, concat_ws(".",db_name,table_name) TABLE_NAME, original_order COL_ORDER, col_name COL_NAME, comment COL_COMMENT, data_type DATA_TYPE from {DATABRICKS_SCHEMA_STORE}
  # UNION
  # select "netezza" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, concat_ws(".",DATABASE,schema,Name) TABLE_NAME, ATTNUM COL_ORDER, ATTNAME COL_NAME, DESCRIPTION COL_COMMENT, FORMAT_TYPE DATA_TYPE from {NETEZZA_SCHEMA_STORE}
  # UNION
  # select "snowflake" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, concat_ws(".", TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) TABLE_NAME, ORDINAL_POSITION COL_ORDER, COLUMN_NAME COL_NAME, COMMENT COL_COMMENT, GENERATED_DATA_TYPE DATA_TYPE from (select * ,
  #    case
  #      lower(DATA_TYPE)
  #      when 'number' then concat(DATA_TYPE,"(",NUMERIC_PRECISION,",",NUMERIC_SCALE,")")
  #      when 'text' then concat(DATA_TYPE,"(",CHARACTER_MAXIMUM_LENGTH,")")
  #      else DATA_TYPE
  #    end as GENERATED_DATA_TYPE
  # from {SNOWFLAKE_SCHEMA_STORE})x
  # ''')

# COMMAND ----------

def save_metrics(table_metrics):
    workflow_name = table_metrics["workflow_name"]
    table_family = table_metrics["table_family"]
    src_wrhse = table_metrics["src_wrhse"]
    src_tbl = table_metrics["src_tbl"]
    tgt_wrhse = table_metrics["tgt_wrhse"]
    tgt_tbl = table_metrics["tgt_tbl"]
    col_mapping = table_metrics["col_mapping"]
    tgt_primary_keys = table_metrics["tgt_primary_keys"]
    exclude_fields_from_summary = table_metrics["exclude_fields_from_summary"]
    date_bucket = table_metrics["date_bucket"]
    mismatch_exclude_fields = table_metrics["mismatch_exclude_fields"]
    # triple quotes to fix the issue when the user defined filter has ' in it. Example in teradata case ROW_MODIFY_GMT_DTTM > CURRENT_TIMESTAMP - INTERVAL '5' DAY was causing failure
    src_sql_override = table_metrics["src_sql_override"].replace("'", "\\'") if table_metrics["src_sql_override"] is not None else None
    tgt_sql_override = table_metrics["tgt_sql_override"].replace("'", "\\'") if table_metrics["tgt_sql_override"] is not None else None
    src_data_load_filter = table_metrics["src_data_load_filter"].replace("'", "\\'") if table_metrics["src_data_load_filter"] is not None else None
    tgt_data_load_filter = table_metrics["tgt_data_load_filter"].replace("'", "\\'") if table_metrics["tgt_data_load_filter"] is not None else None
    validation_data_db = table_metrics["validation_data_db"]
    anomalies_table_name = table_metrics["anomalies_table_name"]
    src_records = table_metrics["src_records"]
    tgt_records = table_metrics["tgt_records"]
    src_delta_table_size_bytes = table_metrics["src_delta_table_size"]
    tgt_delta_table_size_bytes = table_metrics["tgt_delta_table_size"]
    src_extras = table_metrics["src_extras"]
    tgt_extras = table_metrics["tgt_extras"]
    mismatches = table_metrics["mismatches"]
    matches = table_metrics["matches"]
    hash_src_records = table_metrics["hash_src_records"]
    hash_tgt_records = table_metrics["hash_tgt_records"]
    hash_src_delta_table_size_bytes = table_metrics["hash_src_delta_table_size"]
    hash_tgt_delta_table_size_bytes = table_metrics["hash_tgt_delta_table_size"]
    hash_src_extras = table_metrics["hash_src_extras"]
    hash_tgt_extras = table_metrics["hash_tgt_extras"]
    hash_mismatches = table_metrics["hash_mismatches"]
    hash_matches = table_metrics["hash_matches"]
    streamlit_user_name = table_metrics["streamlit_user_name"]
    streamlit_user_email = table_metrics["streamlit_user_email"]
    quick_validation = table_metrics["quick_validation"]
    
    spark.sql(
        f"""INSERT INTO
            { VALIDATION_SUMMARY_TABLE } (
              iteration_name,
              workflow_name,
              src_warehouse,
              src_table,
              tgt_warehouse,
              tgt_table,
              table_family,
              col_mapping,
              tgt_primary_keys,
              exclude_fields_from_summary,
              date_bucket,
              mismatch_exclude_fields,
              src_sql_override,
              tgt_sql_override,
              src_data_load_filter,
              tgt_data_load_filter,
              validation_data_db,
              streamlit_user_name,
              streamlit_user_email,
              favorite,
              metrics,
              hash_metrics,
              quick_validation
            )
          VALUES
            (
              '{iteration_name}',
              '{workflow_name}',
              '{src_wrhse}',
              '{src_tbl}',
              '{tgt_wrhse}',
              '{tgt_tbl}',
              '{table_family}',
              '{col_mapping}',
              '{tgt_primary_keys}',
              '{exclude_fields_from_summary}',
              '{date_bucket}',
              '{mismatch_exclude_fields}',
              '{src_sql_override}',
              '{tgt_sql_override}',
              '{src_data_load_filter}',
              '{tgt_data_load_filter}',
              '{validation_data_db}',
              '{streamlit_user_name}',
              '{streamlit_user_email}',
              FALSE,
              STRUCT(
                { src_records },
                { tgt_records },
                { src_delta_table_size_bytes },
                { tgt_delta_table_size_bytes },
                { src_extras },
                { tgt_extras },
                { mismatches },
                { matches }
              ),
              STRUCT(
                { hash_src_records },
                { hash_tgt_records },
                { hash_src_delta_table_size_bytes },
                { hash_tgt_delta_table_size_bytes },
                { hash_src_extras },
                { hash_tgt_extras },
                { hash_mismatches },
                { hash_matches }
              ),
              { quick_validation }
            )"""
    )

# COMMAND ----------

# DBTITLE 1,Primary_Key_Violation
def primary_key_validation_summary(table_metrics):

  iteration_name = table_metrics["iteration_name"]
  table_family = table_metrics["table_family"]
  workflow_name = table_metrics["workflow_name"]
  src_wrhse = table_metrics["src_wrhse"]
  src_tbl = table_metrics["src_tbl"]
  tgt_wrhse = table_metrics["tgt_wrhse"]
  tgt_tbl = table_metrics["tgt_tbl"]
  
  pk_violation = "FAILED" if spark.sql(f'''select count_if(primary_key_violations !=0) rec_count from (select *, total_record_count-distinct_key_count as primary_key_violations from {PRIMARY_KEY_VALIDATION}  where iteration_name ="{iteration_name}" and lower(table_family) = lower("{table_family}") )a''').collect()[0].rec_count >0 else "SUCCESS"
  print(f"iteration_name: {iteration_name} | table_family: {table_family} | pk_violation: {pk_violation}")

  spark.sql(f"""
  UPDATE {VALIDATION_SUMMARY_TABLE}
    SET
    primary_key_compliance_status = '{pk_violation}'
    WHERE iteration_name = '{iteration_name}'
    AND workflow_name ='{workflow_name}'
    AND src_warehouse = '{src_wrhse}' 
    AND src_table = '{src_tbl}'
    AND tgt_warehouse = '{tgt_wrhse}'
    AND tgt_table = '{tgt_tbl}'
    AND table_family = '{table_family}'""")


# COMMAND ----------

# DBTITLE 1,Ordinal Position Validation
def ordinal_position_validation(table_metrics):
  
  iteration_name = table_metrics["iteration_name"]
  mismatch_exclude_fields_string = table_metrics["mismatch_exclude_fields"]
  table_family = table_metrics["table_family"]
  workflow_name = table_metrics["workflow_name"]
  src_wrhse = table_metrics["src_wrhse"]
  src_tbl = table_metrics["src_tbl"]
  tgt_wrhse = table_metrics["tgt_wrhse"]
  tgt_tbl = table_metrics["tgt_tbl"]

  ordinal_sql = f'''
  (
    SELECT
    ITERATION_NAME,
    SRC_TABLE_NAME,
    TGT_TABLE_NAME,
    SRC_COL_NAME,
    TGT_COL_NAME,
    IF(array_contains (split('{mismatch_exclude_fields_string}','\\\|'),TGT_COL_NAME)OR array_contains (split('{mismatch_exclude_fields_string}','\\\|'),SRC_COL_NAME), concat("BYPASS_",col_name_compare),col_name_compare) as col_name_compare
  FROM (SELECT
    a.ITERATION_NAME,
    a.TABLE_NAME SRC_TABLE_NAME,
    b.TABLE_NAME TGT_TABLE_NAME,
    a.COL_NAME SRC_COL_NAME,
    b.COL_NAME TGT_COL_NAME,
    IF(
      lower(a.COL_NAME) <=> lower(b.COL_NAME),
      'MATCH',
      'MISMATCH'
    ) as col_name_compare
  from
  (select * from schema_store_catalog_v where lower(warehouse) = lower('{src_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{src_tbl}"))a
  FULL OUTER JOIN 
  (select * from schema_store_catalog_v where lower(warehouse) = lower('{tgt_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{tgt_tbl}"))b
  ON a.COL_ORDER = b.COL_ORDER
  order by a.COL_ORDER)x)y
  '''

  print(ordinal_sql)
  ordinal_violation = "FAILED" if spark.sql(f'''
  SELECT count_if(col_name_compare=='MISMATCH') rec_count FROM {ordinal_sql}
  ''').collect()[0].rec_count >0 else ("BYPASS_SUCCESS" if spark.sql(f'''
  SELECT count_if(startsWith(col_name_compare, 'BYPASS_')) rec_count FROM {ordinal_sql}
  ''').collect()[0].rec_count >0 else "SUCCESS")

  print(f"iteration_name: {iteration_name} | table_family: {table_family} | ordinal_violation: {ordinal_violation}")

  spark.sql(f"""
  UPDATE {VALIDATION_SUMMARY_TABLE}
    SET
    ordinal_position_compliance_status = '{ordinal_violation}'
    WHERE iteration_name = '{iteration_name}'
    AND workflow_name ='{workflow_name}'
    AND src_warehouse = '{src_wrhse}' 
    AND src_table = '{src_tbl}'
    AND tgt_warehouse = '{tgt_wrhse}'
    AND tgt_table = '{tgt_tbl}'
    AND table_family = '{table_family}'""")

# COMMAND ----------

# DBTITLE 1,Schema Validation
def schema_validation(table_metrics):

  iteration_name = table_metrics["iteration_name"]
  table_family = table_metrics["table_family"]
  workflow_name = table_metrics["workflow_name"]
  src_wrhse = table_metrics["src_wrhse"]
  src_tbl = table_metrics["src_tbl"]
  tgt_wrhse = table_metrics["tgt_wrhse"]
  tgt_tbl = table_metrics["tgt_tbl"]
  
  df = spark.sql(f'''
  SELECT 
  case (count_if(col_name_compare=='MISMATCH'))
  when 0 then 'SUCCESS' else 'FAILED' end as col_name_compare_status ,
  case (count_if(data_type_compare=='MISMATCH'))
  when 0 then 'SUCCESS' else 'FAILED' end as data_type_compare_status ,
  case (count_if(comment_compare=='MISMATCH'))
  when 0 then 'SUCCESS' else 'FAILED' end as comment_compare_status
  FROM (SELECT
    a.ITERATION_NAME,
    a.TABLE_NAME SRC_TABLE_NAME,
    b.TABLE_NAME TGT_TABLE_NAME,
    a.COL_NAME SRC_COL_NAME,
    b.COL_NAME TGT_COL_NAME,
    IF(
      lower(a.COL_NAME) <=> lower(b.COL_NAME),
      'MATCH',
      'MISMATCH'
    ) as col_name_compare,
    a.DATA_TYPE SRC_DATA_TYPE,
    b.DATA_TYPE TGT_DATA_TYPE,
    IF(
      lower(a.DATA_TYPE) <=> lower(b.DATA_TYPE),
      'MATCH',
      'MISMATCH'
    ) as data_type_compare,
    a.COL_COMMENT SRC_COL_COMMENT,
    b.COL_COMMENT TGT_COL_COMMENT,
    IF(
      lower(a.COL_COMMENT) <=> lower(b.COL_COMMENT),
      'MATCH',
      'MISMATCH'
    ) as comment_compare
  from
  (select * from schema_store_catalog_v where lower(warehouse) = lower('{src_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{src_tbl}"))a
  FULL OUTER JOIN 
  (select * from schema_store_catalog_v where lower(warehouse) = lower('{tgt_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{tgt_tbl}"))b
  ON lower(a.COL_NAME) = lower(b.COL_NAME)
  order by a.COL_ORDER)x  where lower(SRC_TABLE_NAME) = lower("{src_tbl}") and lower(TGT_TABLE_NAME) = lower("{tgt_tbl}")
  ''').collect()

  col_name_compare_status, data_type_compare_status, comment_compare_status = df[0].col_name_compare_status, df[0].data_type_compare_status, df[0].comment_compare_status
  print(f"iteration_name: {iteration_name} | table_family: {table_family} | col_name_compare_status: {col_name_compare_status}; data_type_compare_status: {data_type_compare_status};  comment_compare_status: {comment_compare_status}")


  spark.sql(f"""
  UPDATE {VALIDATION_SUMMARY_TABLE}
    SET
    col_name_compare_status = '{col_name_compare_status}',
    data_type_compare_status = '{data_type_compare_status}',
    comment_compare_status = '{comment_compare_status}'
    WHERE iteration_name = '{iteration_name}'
    AND workflow_name ='{workflow_name}'
    AND src_warehouse = '{src_wrhse}' 
    AND src_table = '{src_tbl}'
    AND tgt_warehouse = '{tgt_wrhse}'
    AND tgt_table = '{tgt_tbl}'
    AND table_family = '{table_family}'""")

# COMMAND ----------

# DBTITLE 1,Datatype Compatibility Matrix
def data_type_compatibility_check(table_metrics):

  iteration_name = table_metrics["iteration_name"]
  table_family = table_metrics["table_family"]
  workflow_name = table_metrics["workflow_name"]
  src_wrhse = table_metrics["src_wrhse"]
  src_tbl = table_metrics["src_tbl"]
  tgt_wrhse = table_metrics["tgt_wrhse"]
  tgt_tbl = table_metrics["tgt_tbl"]
  
  data_type_compatibility_violation = "SUCCESS" if spark.sql(f'''
  SELECT count_if(not final_compatibility) rec_count FROM(
  select *, if(
        rlike(
          lower(src_data_type),
          src_wrhse_compatibility_regex),
        true,
        false
      )as data_type_compatibility,
    if(y.has_precision,  
    if(
      (
        regexp_extract(
          lower(tgt_data_type),
          tgt_wrhse_compatibility_regex,
          2
        )
      ) -(
        regexp_extract(
          lower(src_data_type),
          src_wrhse_compatibility_regex,
          2
        )
      ) >= 0,
      true,
      false
    ),true) precision_compatible,
    if(y.has_precision,
      if(
      (
        nvl(int(regexp_extract(
          lower(tgt_data_type),
          tgt_wrhse_compatibility_regex,
          4
        )),0)
      ) -(
        nvl(int(regexp_extract(
          lower(src_data_type),
          src_wrhse_compatibility_regex,
          4
        )),0)
      ) >= 0,
      true,
      false
    ),true) scale_compatible,
    (data_type_compatibility and precision_compatible and scale_compatible) final_compatibility
    from (select a.iteration_name, a.table_name src_table_name, b.table_name tgt_table_name, b.col_name, a.data_type src_data_type, b.data_type tgt_data_type from (select * from schema_store_catalog_v where lower(warehouse) = lower('{src_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{src_tbl}"))a
  FULL OUTER JOIN 
  (select * from schema_store_catalog_v where lower(warehouse) = lower('{tgt_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) =lower("{tgt_tbl}"))b
  ON lower(a.COL_NAME) = lower(b.COL_NAME)
  -- where lower(a.DATA_TYPE) <> lower(b.DATA_TYPE)
  order by b.COL_ORDER) x
  left outer join (select data_type lkp_data_type, warehouse, data_type data_type_aliases, has_precision, {src_wrhse}_compatibility_regex src_wrhse_compatibility_regex, {tgt_wrhse}_compatibility_regex tgt_wrhse_compatibility_regex from {DATA_TYPE_COMPATIBILITY_MATRIX}
  where lower(warehouse) = lower('{tgt_wrhse}')) y
  on rlike(lower(x.tgt_data_type),lkp_data_type)
  where src_wrhse_compatibility_regex is not null)z where lower(SRC_TABLE_NAME) = lower("{src_tbl}") and lower(TGT_TABLE_NAME) = lower("{tgt_tbl}")
  ''').collect()[0].rec_count == 0  else "FAILED"



  print(f"iteration_name: {iteration_name} | table_family: {table_family} | data_type_compatibility_violation: {data_type_compatibility_violation}")

  spark.sql(f"""
  UPDATE {VALIDATION_SUMMARY_TABLE}
    SET
    data_type_compatibility_status = '{data_type_compatibility_violation}'
    WHERE iteration_name = '{iteration_name}'
    AND workflow_name ='{workflow_name}'
    AND src_warehouse = '{src_wrhse}' 
    AND src_table = '{src_tbl}'
    AND tgt_warehouse = '{tgt_wrhse}'
    AND tgt_table = '{tgt_tbl}'
    AND table_family = '{table_family}'""")

# COMMAND ----------

# DBTITLE 1,Mismatch Status
from pyspark.sql.functions import col
def mismatch_status(table_metrics):

  iteration_name = table_metrics["iteration_name"]
  table_family = table_metrics["table_family"]
  workflow_name = table_metrics["workflow_name"]
  src_wrhse = table_metrics["src_wrhse"]
  src_tbl = table_metrics["src_tbl"]
  tgt_wrhse = table_metrics["tgt_wrhse"]
  tgt_tbl = table_metrics["tgt_tbl"]
  # mismatch_exclude_fields_string = table_metrics["mismatch_exclude_fields"]
  
  # mismatch_exclude_fields_compiled = mismatch_exclude_fields_string.format(**locals())
  # mismatch_exclude_fields = mismatch_exclude_fields_compiled.split("|")
  
  mismatch_exclude_fields = table_metrics["mismatch_exclude_fields"]
  columns_for_mismatch_validation = ",".join(f'"{cols}"' 
          for cols in list(
                spark.sql(f"show columns from {tgt_tbl}")
                .filter(~col("col_name").isin(mismatch_exclude_fields))
                .toPandas()["col_name"]
            )
  )
  print(columns_for_mismatch_validation)

  mismatches = spark.sql(f"""select
    case
      when count_if(mismatches > 0) = 0 then 'SUCCESS'
      else 'FAILED'
    end as mismatches,
    case
      when count_if(
        col_name in ({ columns_for_mismatch_validation })
        and mismatches > 0
      ) =0 then 'SUCCESS'
      else 'FAILED' 
    end mismatches_after_exclusion
  from
    { MISMATCH_METRICS }
  where
    iteration_name = '{iteration_name}'
    and table_family = '{table_family}'""").collect()

  mismatches_status, mismatches_after_exclusion_status = mismatches[0].mismatches, mismatches[0].mismatches_after_exclusion
  print(f"iteration_name: {iteration_name} | table_family: {table_family} | mismatches_status: {mismatches_status}; mismatches_after_exclusion_status: {mismatches_after_exclusion_status}")


  spark.sql(f"""
  UPDATE {VALIDATION_SUMMARY_TABLE}
    SET
    mismatches_status = '{mismatches_status}',
    mismatches_after_exclusion_status = '{mismatches_after_exclusion_status}'
    WHERE iteration_name = '{iteration_name}'
    AND workflow_name ='{workflow_name}'
    AND src_warehouse = '{src_wrhse}' 
    AND src_table = '{src_tbl}'
    AND tgt_warehouse = '{tgt_wrhse}'
    AND tgt_table = '{tgt_tbl}'
    AND table_family = '{table_family}'""")

# COMMAND ----------

# DBTITLE 1,Extra Records Status
def extras_status(table_metrics):

  iteration_name = table_metrics["iteration_name"]
  table_family = table_metrics["table_family"]
  workflow_name = table_metrics["workflow_name"]
  src_wrhse = table_metrics["src_wrhse"]
  src_tbl = table_metrics["src_tbl"]
  tgt_wrhse = table_metrics["tgt_wrhse"]
  tgt_tbl = table_metrics["tgt_tbl"]
  validation_data_db = table_metrics["validation_data_db"]
  anomalies_table_name = f"{validation_data_db}.{workflow_name}___{table_family}__anomalies"
  extras = spark.sql(f"""select count(*) from {anomalies_table_name} where iteration_name = '{iteration_name}' and `type` in ("tgt_extras","src_extras")""").collect()[0][0]
  extras_status = "SUCCESS" if extras == 0 else "FAILED"
  print(f"iteration_name: {iteration_name} | table_family: {table_family} | extras_status: {extras_status}")

  spark.sql(f"""
  UPDATE {VALIDATION_SUMMARY_TABLE}
    SET
    extras_status = '{extras_status}'
    WHERE iteration_name = '{iteration_name}'
    AND workflow_name ='{workflow_name}'
    AND src_warehouse = '{src_wrhse}' 
    AND src_table = '{src_tbl}'
    AND tgt_warehouse = '{tgt_wrhse}'
    AND tgt_table = '{tgt_tbl}'
    AND table_family = '{table_family}'""")

