# Databricks notebook source
# DBTITLE 1,Description
##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# COMMAND ----------

# MAGIC %run "./conf/constants"

# COMMAND ----------

import logging
from tkinter import NO
# ---- Logging Setup ----
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# COMMAND ----------

triggered_from_workflow = False
streamlit_user_name = None
streamlit_user_email = None
# Comment the following to trigger the job as a notebook run 
triggered_from_workflow = dbutils.widgets.get("00-triggered_from_workflow")
streamlit_user_name = dbutils.widgets.get("04-user_name")
streamlit_user_email = dbutils.widgets.get("05-user_email")

# COMMAND ----------

# DBTITLE 0,Initialize Widgets
def initializeWidgets(validation_mapping_table):
    dbutils.widgets.text("01-iteration_name_suffix", "")
    iteration_name_suffix = dbutils.widgets.get("01-iteration_name_suffix")

    default_workflow = spark.sql(
        f"select workflow_name from {validation_mapping_table} where validation_is_active limit 1"
    ).collect()[0]["workflow_name"]
    dbutils.widgets.multiselect(
        "02-workflow_name",
        default_workflow,
        [
            workflow_name[0]
            for workflow_name in spark.sql(
                f"select distinct workflow_name from {validation_mapping_table} where validation_is_active"
            ).collect()
        ],
    )
    workflow = (
        ""
        if dbutils.widgets.get("02-workflow_name") is None
        else dbutils.widgets.get("02-workflow_name")
    )
    workflow_where_condition = (
        "and true" if workflow == "" else f"and workflow_name ='{workflow}'"
    )

    default_workflow_table_family = spark.sql(
        f"select concat_ws(':',workflow_name, table_family) workflow_table_family from {validation_mapping_table} where validation_is_active {workflow_where_condition} limit 1"
    ).collect()[0]["workflow_table_family"]
    dbutils.widgets.multiselect(
        "03-workflow:table_family",
        default_workflow_table_family,
        [
            table_family[0]
            for table_family in spark.sql(
                f"select distinct concat_ws(':',workflow_name, table_family) workflow_table_family from {validation_mapping_table} where validation_is_active {workflow_where_condition}"
            ).collect()
        ],
    )
    workflow_table_family = ",".join(
        f'"{table_families}"'
        for table_families in (dbutils.widgets.get("03-workflow:table_family").split(","))
    )

    workflow_table_family = (
        ",".join(
            [
                f'"{workflow_table_family[0]}"'
                for workflow_table_family in spark.sql(
                    f"select distinct concat_ws(':',workflow_name, table_family) workflow_table_family from {validation_mapping_table} where validation_is_active {workflow_where_condition}"
                ).collect()
            ]
        )
        if (workflow_table_family == '""')
        else workflow_table_family
    )
    return iteration_name_suffix, workflow_table_family

# COMMAND ----------

validation_mapping_table = VALIDATION_MAPPING_TABLE
if(not triggered_from_workflow):
  initializeWidgets(validation_mapping_table)

# COMMAND ----------

# MAGIC %run "./integrations/netezza/netezza-extract"

# COMMAND ----------

# MAGIC %run "./integrations/databricks/databricks-extract"

# COMMAND ----------

# MAGIC %run "./integrations/snowflake/snowflake-extract"

# COMMAND ----------

# MAGIC %run "./integrations/oracle/oracle-extract"

# COMMAND ----------

# MAGIC %run "./integrations/mssql/mssql-extract"

# COMMAND ----------

# MAGIC %run "./integrations/teradata/teradata-extract"

# COMMAND ----------

#%run "./integrations/hive/hive-extract"

# COMMAND ----------

import json

class TableMapping:
    def __init__(self, entry_id, workflow_name, table_family, src_connection_name, src_table, 
                 tgt_connection_name, tgt_table, col_mapping, tgt_primary_keys, date_bucket, 
                 mismatch_exclude_fields, exclude_fields_from_summary, filter, addtnl_filters,
                 src_sql_override, tgt_sql_override, src_data_load_filter, tgt_data_load_filter,
                 validation_data_db, retain_tables_list, quick_validation, validation_is_active,
                 src_warehouse, src_jdbc_options, src_cast_to_string, tgt_warehouse, tgt_jdbc_options):
        
        # Initialize the attributes, trimming strings and setting empty ones to None
        self._entry_id = entry_id
        self._workflow_name = self._clean_value(workflow_name)
        self._table_family = self._clean_value(table_family)
        self._src_connection_name = self._clean_value(src_connection_name)
        self._src_table = self._clean_value(src_table)
        self._tgt_connection_name = self._clean_value(tgt_connection_name)
        self._tgt_table = self._clean_value(tgt_table)
        self._col_mapping = self._clean_json(col_mapping)
        self._tgt_primary_keys = self._clean_value(tgt_primary_keys)
        self._date_bucket = self._clean_value(date_bucket)  # Assuming this is not a string; leaving it unchanged
        self._mismatch_exclude_fields = self._clean_value(mismatch_exclude_fields)
        self._exclude_fields_from_summary = self._clean_value(exclude_fields_from_summary)
        self._filter = self._clean_value(filter)
        self._addtnl_filters = self._clean_value(addtnl_filters)
        self._src_sql_override = self._clean_value(src_sql_override)
        self._tgt_sql_override = self._clean_value(tgt_sql_override)
        self._src_data_load_filter = self._clean_value(src_data_load_filter)
        self._tgt_data_load_filter = self._clean_value(tgt_data_load_filter)
        self._validation_data_db = self._clean_value(validation_data_db)
        self._retain_tables_list = self._clean_value(retain_tables_list)
        self._quick_validation = quick_validation  # Assuming this is a boolean; leaving it unchanged
        self._validation_is_active = validation_is_active  # Assuming this is a boolean; leaving it unchanged
        self._src_warehouse = self._clean_value(src_warehouse)
        self._src_jdbc_options = self._clean_json(src_jdbc_options)
        self._src_cast_to_string = src_cast_to_string  # Assuming this is a boolean; leaving it unchanged
        self._tgt_warehouse = self._clean_value(tgt_warehouse)
        self._tgt_jdbc_options = self._clean_json(tgt_jdbc_options)

    def _clean_value(self, value):
        """ Helper method to clean string values: trims strings and sets empty values to None """
        if isinstance(value, str):
            cleaned_value = value.strip() if value else None
            return cleaned_value
        return value  # Non-string values (e.g., numbers, booleans) remain unchanged

    def _clean_json(self, value):
        """ Helper method to clean JSON input. Cleans both keys and values (if valid JSON string) """
        if isinstance(value, str):
            try:
                # Parse the string as JSON
                parsed_json = json.loads(value)
                # Clean the keys and values of the parsed JSON
                return self._clean_json_dict(parsed_json)
            except (json.JSONDecodeError, TypeError):
                # If the value is not a valid JSON string, return it as is
                return None
        return value  # Return value unchanged if it's not a string

    def _clean_json_dict(self, json_dict):
        """ Recursively cleans a JSON dictionary (keys and values) """
        if isinstance(json_dict, dict):
            cleaned_dict = {}
            for key, value in json_dict.items():
                cleaned_key = self._clean_value(key)  # Clean the key (strip spaces)
                cleaned_value = self._clean_value(value)  # Clean the value (strip spaces, or set to None)
                cleaned_dict[cleaned_key] = cleaned_value
            return cleaned_dict
        return json_dict  # If it's not a dictionary, return it unchanged
    
    # Getter and setter for entry_id
    @property
    def entry_id(self):
        return self._entry_id

    @entry_id.setter
    def entry_id(self, value):
        self._entry_id = value

    # Getter and setter for workflow_name
    @property
    def workflow_name(self):
        return self._workflow_name

    @workflow_name.setter
    def workflow_name(self, value):
        self._workflow_name = self._clean_value(value)

    # Getter and setter for table_family
    @property
    def table_family(self):
        return self._table_family

    @table_family.setter
    def table_family(self, value):
        self._table_family = self._clean_value(value)

    # Getter and setter for src_connection_name
    @property
    def src_connection_name(self):
        return self._src_connection_name

    @src_connection_name.setter
    def src_connection_name(self, value):
        self._src_connection_name = self._clean_value(value)

    # Getter and setter for src_table
    @property
    def src_table(self):
        return self._src_table

    @src_table.setter
    def src_table(self, value):
        self._src_table = self._clean_value(value)

    # Getter and setter for tgt_connection_name
    @property
    def tgt_connection_name(self):
        return self._tgt_connection_name

    @tgt_connection_name.setter
    def tgt_connection_name(self, value):
        self._tgt_connection_name = self._clean_value(value)

    # Getter and setter for tgt_table
    @property
    def tgt_table(self):
        return self._tgt_table

    @tgt_table.setter
    def tgt_table(self, value):
        self._tgt_table = self._clean_value(value)

    # Getter and setter for col_mapping
    @property
    def col_mapping(self):
        return self._col_mapping
    
    @property
    def pretty_col_mapping(self):
        # Convert the col_mapping to a pretty-printed JSON string
        return json.dumps(self._col_mapping, indent=4)

    @col_mapping.setter
    def col_mapping(self, value):
        self._col_mapping = self._clean_json(value)
        

    # Getter and setter for tgt_primary_keys
    @property
    def tgt_primary_keys(self):
        if self._tgt_primary_keys:
            _primary_keys_list = [col.strip() for col in self._tgt_primary_keys.split("|")]
            return _primary_keys_list
        return []  # Return empty list when no primary keys are defined
    
    @property
    def has_primary_keys(self):
        """Check if primary keys are defined"""
        return bool(self._tgt_primary_keys and self._tgt_primary_keys.strip())
    
    @property
    def validation_strategy(self):
        """Determine validation strategy based on primary key availability"""
        return "primary_key_based" if self.has_primary_keys else "hash_based"

    @tgt_primary_keys.setter
    def tgt_primary_keys(self, value):    
        self._tgt_primary_keys = self._clean_value(value)

    # Getter and setter for date_bucket
    @property
    def date_bucket(self):
        return self._date_bucket

    @date_bucket.setter
    def date_bucket(self, value):
        self._date_bucket = self._clean_value(value)

    # Getter and setter for mismatch_exclude_fields
    @property
    def mismatch_exclude_fields(self):
        _mismatch_exclude_fields_list = [col.strip() for col in self._mismatch_exclude_fields.split("|")]
        return _mismatch_exclude_fields_list

    @mismatch_exclude_fields.setter
    def mismatch_exclude_fields(self, value):
        self._mismatch_exclude_fields = self._clean_value(value)

    # Getter and setter for exclude_fields_from_summary
    @property
    def exclude_fields_from_summary(self):
        return self._exclude_fields_from_summary

    @exclude_fields_from_summary.setter
    def exclude_fields_from_summary(self, value):
        self._exclude_fields_from_summary = self._clean_value(value)

    # Getter and setter for filter
    @property
    def filter(self):
        return self._filter

    @filter.setter
    def filter(self, value):
        self._filter = self._clean_value(value)

    # Getter and setter for addtnl_filters
    @property
    def addtnl_filters(self):
        return self._addtnl_filters

    @addtnl_filters.setter
    def addtnl_filters(self, value):
        self._addtnl_filters = self._clean_value(value)

    # Getter and setter for src_sql_override
    @property
    def src_sql_override(self):
        return self._src_sql_override

    @src_sql_override.setter
    def src_sql_override(self, value):
        self._src_sql_override = self._clean_value(value)

    # Getter and setter for tgt_sql_override
    @property
    def tgt_sql_override(self):
        return self._tgt_sql_override

    @tgt_sql_override.setter
    def tgt_sql_override(self, value):
        self._tgt_sql_override = self._clean_value(value)

    # Getter and setter for src_data_load_filter
    @property
    def src_data_load_filter(self):
        return self._src_data_load_filter

    @src_data_load_filter.setter
    def src_data_load_filter(self, value):
        self._src_data_load_filter = self._clean_value(value)

    # Getter and setter for tgt_data_load_filter
    @property
    def tgt_data_load_filter(self):
        return self._tgt_data_load_filter

    @tgt_data_load_filter.setter
    def tgt_data_load_filter(self, value):
        self._tgt_data_load_filter = self._clean_value(value)

    # Getter and setter for validation_data_db
    @property
    def validation_data_db(self):
        return self._validation_data_db

    @validation_data_db.setter
    def validation_data_db(self, value):
        self._validation_data_db = self._clean_value(value)

    # Getter and setter for retain_tables_list
    @property
    def retain_tables_list(self):
        return self._retain_tables_list

    @retain_tables_list.setter
    def retain_tables_list(self, value):
        self._retain_tables_list = self._clean_value(value)

    # Getter and setter for quick_validation
    @property
    def quick_validation(self):
        return self._quick_validation

    @quick_validation.setter
    def quick_validation(self, value):
        self._quick_validation = value

    # Getter and setter for validation_is_active
    @property
    def validation_is_active(self):
        return self._validation_is_active

    @validation_is_active.setter
    def validation_is_active(self, value):
        self._validation_is_active = value

    # Getter and setter for src_warehouse
    @property
    def src_warehouse(self):
        return self._src_warehouse

    @src_warehouse.setter
    def src_warehouse(self, value):
        self._src_warehouse = self._clean_value(value)

    # Getter and setter for src_jdbc_options
    @property
    def src_jdbc_options(self):
        return self._src_jdbc_options
    
    @property
    def pretty_src_jdbc_options(self):
        # Convert the src_jdbc_options to a pretty-printed JSON string
        return json.dumps(self._src_jdbc_options, indent=4)

    @src_jdbc_options.setter
    def src_jdbc_options(self, value):
        self._src_jdbc_options = self._clean_json(value)

    # Getter and setter for src_cast_to_string
    @property
    def src_cast_to_string(self):
        return self._src_cast_to_string

    @src_cast_to_string.setter
    def src_cast_to_string(self, value):
        self._src_cast_to_string = value

    # Getter and setter for tgt_warehouse
    @property
    def tgt_warehouse(self):
        return self._tgt_warehouse

    @tgt_warehouse.setter
    def tgt_warehouse(self, value):
        self._tgt_warehouse = self._clean_value(value)

    # Getter and setter for tgt_jdbc_options
    @property
    def tgt_jdbc_options(self):
        return self._tgt_jdbc_options
    
    @property
    def pretty_tgt_jdbc_options(self):
        # Convert the tgt_jdbc_options to a pretty-printed JSON string
        return json.dumps(self._tgt_jdbc_options, indent=4)

    @tgt_jdbc_options.setter
    def tgt_jdbc_options(self, value):
        self._tgt_jdbc_options = self._clean_json(value)


# COMMAND ----------

def generate_iteration_details(iteration_name_suffix):

    current_timestamp = spark.sql(
        f"""select now(), date_format(now(),"yyyy-MM-dd'T'HH:mm:ss.SSSZ")"""
    ).collect()
    run_timestamp = current_timestamp[0][0]
    if iteration_name_suffix:
        iteration_name = f"{current_timestamp[0][1]}_{iteration_name_suffix}"
    else:
        iteration_name = current_timestamp[0][1]
    logger.info(f"run_timestamp: {run_timestamp} | iteration_name: {iteration_name}")
    return run_timestamp, iteration_name

# COMMAND ----------

def generate_src_columns(src_columns, col_mapping):
  # primary_keys_string = pk_columns.replace("|", ",")
  logger.info(src_columns)
  # primary_keys_list = [col.strip() for col in pk_columns.split("|")]
  # print (primary_keys_list)
  cm = col_mapping
  
  # Handle case where src_columns is empty or None
  if not src_columns:
    print("src_columns_string: (empty)")
    return ""
  
  #convert the tgt_pks to corresponding src_pks, if col_mapping is available
  columns_list_src = {col: next((key for key, value in cm.items() if value.lower() == col.lower()), col) for col in src_columns}
  src_columns_string = ','.join(columns_list_src.values())
  logger.info(f"src_columns_string: {src_columns_string}")
  return src_columns_string


# COMMAND ----------

import json


def read_table_config(validation_config_table):
    config_sql = f"select * from {validation_config_table}"
    table_configs = spark.sql(config_sql)
    return table_configs

# COMMAND ----------

def write_data(df_data, table):

    df = df_data.withColumn(
        "run_timestamp__mmp", to_timestamp(lit(f"{run_timestamp}"))
    ).withColumn("iteration_name__mmp", lit(f"{iteration_name}"))
    df.write.mode("append").saveAsTable(table)
    return table

# COMMAND ----------

def captureHashTable(warehouse, table, pk_columns, mismatch_exclude_fields, jdbc_options, sql_override, data_load_filter, table_mapping, path=None, src_path_part_params=None, batch_load_ids=None):
  validation_data_db =table_mapping.validation_data_db
  workflow_name = table_mapping.workflow_name
  match warehouse:
      # case "netezza":
      #     df_data = captureNetezzaTable(table, jdbc_options, data_load_filter, src_cast_to_string)
      case "databricks":
          df_data = captureDatabricksTableHash(table, pk_columns, mismatch_exclude_fields, sql_override, data_load_filter, table_mapping, path, src_path_part_params,batch_load_ids)
      # case "snowflake":
      #     df_data = captureSnowflakeTable(table, jdbc_options, data_load_filter, src_cast_to_string)
      # case "oracle":
      #     df_data = captureOracleTable(table, jdbc_options, data_load_filter, src_cast_to_string)
      # case "mssql":
      #     df_data = captureMSSqlTable(table, jdbc_options, data_load_filter, src_cast_to_string)
      # case "teradata":
      #     df_data = captureTeradataTable(table, jdbc_options, data_load_filter, src_cast_to_string)
      case "hive":
          df_data = captureHiveTableHash(table, pk_columns, mismatch_exclude_fields, sql_override, data_load_filter, table_mapping, jdbc_options)
      case _:
          logger.error(f"The warehouse, {warehouse}, is either invalid or is not currently supported for quick validation")
          raise Exception(f"The warehouse, {warehouse}, is either invalid or is not currently supported for quick validation")
  hash_validation_tbl = f"{validation_data_db}.{warehouse}_____{workflow_name}____{table.replace('.', '___')}__hash"
  return write_data(df_data, hash_validation_tbl)

# COMMAND ----------

def captureTable(warehouse, table, pk_columns, jdbc_options, sql_override, data_load_filter, table_mapping,path=None,src_path_part_params=None,batch_load_ids=None):
    src_cast_to_string = table_mapping.src_cast_to_string
    validation_data_db =table_mapping.validation_data_db
    workflow_name = table_mapping.workflow_name
    match warehouse:
        case "netezza":
            df_data = captureNetezzaTable(table, jdbc_options, data_load_filter, src_cast_to_string)
        case "databricks":
            df_data = captureDatabricksTable(table, sql_override, data_load_filter, src_cast_to_string,path,src_path_part_params,batch_load_ids)
        case "snowflake":
            df_data = captureSnowflakeTable(table, jdbc_options, data_load_filter, src_cast_to_string)
        case "oracle":
            df_data = captureOracleTable(table, jdbc_options, data_load_filter, src_cast_to_string)
        case "mssql":
            df_data = captureMSSqlTable(table, jdbc_options, data_load_filter, src_cast_to_string)
        case "teradata":
            df_data = captureTeradataTable(table, jdbc_options, data_load_filter, src_cast_to_string)
        case "hive":
            df_data = captureHiveTable(table, pk_columns, sql_override, data_load_filter, table_mapping, jdbc_options)
        case _:
            logger.error(f"The warehouse, {warehouse}, is either invalid or is not currently supported")
            raise Exception(f"The warehouse, {warehouse}, is either invalid or is not currently supported")
    validation_tbl = f"{validation_data_db}.{warehouse}_____{workflow_name}____{table.replace('.', '___')}"
    logger.info(f"validation_tbl: {validation_tbl}")
    return write_data(df_data, validation_tbl)

# COMMAND ----------

def readValidationTableList(validation_mapping_table, table_config_table, workflow_table_families):
    # config_sql = f"select * from {validation_mapping_table} where table_family in ({table_families})"
    config_sql = f'''
        with src_details as (
        select
        a.*,
        b.warehouse src_warehouse,
        b.jdbc_options src_jdbc_options,
        b.cast_to_string as src_cast_to_string
        from
        (select * from
        {validation_mapping_table} where concat_ws(':', workflow_name, table_family) in ({workflow_table_families}))a
        left outer join {table_config_table} b on a.src_connection_name = b.connection_name
        )
        select
        a.*,
        b.warehouse tgt_warehouse,
        b.jdbc_options tgt_jdbc_options
        from
        src_details a
        left outer join {table_config_table} b on a.tgt_connection_name = b.connection_name'''
    # print(config_sql)
    table_mappings = spark.sql(config_sql).collect()
    return table_mappings

# COMMAND ----------

def writeSchema(df_schema, target_store, table_mapping):
    workflow_name = table_mapping.workflow_name
    table_family = table_mapping.table_family
    df = (
        df_schema.withColumn("run_timestamp", to_timestamp(lit(f"{run_timestamp}")))
        .withColumn("iteration_name", lit(f"{iteration_name}"))
        .withColumn("workflow_name", lit(f"{workflow_name}"))
        .withColumn("table_family", lit(f"{table_family}"))
    )
    df.write.partitionBy("iteration_name", "workflow_name", "table_family").mode(
        "append"
    ).saveAsTable(target_store)

# COMMAND ----------

def captureSchema(warehouse, table, jdbc_options, table_mapping,path=None, src_path_part_params = None):
    match warehouse:
        case "netezza":
            df_schema = captureNetezzaSchema(table, jdbc_options)
            writeSchema(df_schema, NETEZZA_SCHEMA_STORE, table_mapping)
        case "databricks":
            if path is None:
                df_schema = captureDatabricksSchema(table)
                writeSchema(df_schema, DATABRICKS_SCHEMA_STORE, table_mapping)
            else:
                df_schema = captureDatabricksSchema(table,path,src_path_part_params)
                writeSchema(df_schema, DATABRICKS_SCHEMA_STORE, table_mapping)
        case "snowflake":
            df_schema = captureSnowflakeSchema(table, jdbc_options)
            writeSchema(df_schema, SNOWFLAKE_SCHEMA_STORE, table_mapping)
        case "oracle":
            df_schema = captureOracleSchema(table, jdbc_options)
            writeSchema(df_schema, ORACLE_SCHEMA_STORE, table_mapping)
        case "mssql":
            df_schema = captureMSSqlSchema(table, jdbc_options)
            writeSchema(df_schema, MSSQL_SCHEMA_STORE, table_mapping)
        case "teradata":
            df_schema = captureTeradataSchema(table, jdbc_options)
            writeSchema(df_schema, TERADATA_SCHEMA_STORE, table_mapping)
        case "hive":
            df_schema = captureHiveSchema(table, jdbc_options)
            writeSchema(df_schema, HIVE_SCHEMA_STORE, table_mapping)
        case _:
            logger.error("The warehouse is either invalid or is not currently supported")

# COMMAND ----------

def captureSrcSchema(src_warehouse, src_table, src_jdbc_options, table_mapping,src_path, src_path_part_params):
    logger.info("Capturing Source Schema:")
    captureSchema(src_warehouse, src_table, src_jdbc_options, table_mapping,src_path, src_path_part_params)

# COMMAND ----------

def captureTgtSchema(tgt_warehouse, tgt_table, tgt_jdbc_options, table_mapping):
    logger.info("Capturing Target Schema:")
    captureSchema(tgt_warehouse, tgt_table, tgt_jdbc_options, table_mapping)

# COMMAND ----------

def captureSrcTableHash(src_warehouse, src_table, src_jdbc_options, src_sql_override, src_data_load_filter, table_mapping, src_path,src_path_part_params):
    print("Capturing Source Hash Contents:")
    pk_columns = table_mapping.tgt_primary_keys
    mismatch_exclude_fields = table_mapping.mismatch_exclude_fields
    col_mapping = table_mapping.col_mapping
    
    # Handle case where primary keys are empty (for hash-based validation)
    if pk_columns:
        src_pk_columns = generate_src_columns(pk_columns, col_mapping)
    else:
        src_pk_columns = ""  # Empty string for hash-based validation
    
    if mismatch_exclude_fields:
        src_mismatch_exclude_fields = generate_src_columns(mismatch_exclude_fields, col_mapping).split(",")
    else:
        src_mismatch_exclude_fields = []
    
    logger.info(f"src_mismatch_exclude_fields: {src_mismatch_exclude_fields}")
    return captureHashTable(src_warehouse, src_table, src_pk_columns, src_mismatch_exclude_fields, src_jdbc_options, src_sql_override, src_data_load_filter, table_mapping, src_path, src_path_part_params)

# COMMAND ----------

def captureTgtTableHash(tgt_warehouse, tgt_table, tgt_jdbc_options, tgt_sql_override, tgt_data_load_filter, table_mapping,batch_load_ids):
    print("Capturing Target Hash Contents:")
    
    # Handle case where primary keys are empty (for hash-based validation)
    if table_mapping.tgt_primary_keys:
        tgt_pk_columns = ",".join(table_mapping.tgt_primary_keys)
    else:
        tgt_pk_columns = ""  # Empty string for hash-based validation
    
    tgt_mismatch_exclude_fields = table_mapping.mismatch_exclude_fields
    return captureHashTable(tgt_warehouse, tgt_table, tgt_pk_columns, tgt_mismatch_exclude_fields, tgt_jdbc_options, tgt_sql_override, tgt_data_load_filter, table_mapping,batch_load_ids=batch_load_ids)

# COMMAND ----------

def captureSrcTable(src_warehouse, src_table, src_jdbc_options, src_sql_override, src_data_load_filter, table_mapping, path, src_path_part_params):
    pk_columns = table_mapping.tgt_primary_keys
    col_mapping = table_mapping.col_mapping
    src_pk_columns = generate_src_columns(pk_columns, col_mapping)
    return captureTable(src_warehouse, src_table, src_pk_columns, src_jdbc_options, src_sql_override, src_data_load_filter, table_mapping,path, src_path_part_params)

# COMMAND ----------

def captureTgtTable(tgt_warehouse, tgt_table, tgt_jdbc_options, tgt_sql_override, tgt_data_load_filter, table_mapping,batch_load_ids):
    logger.info("Capturing Target Contents:")
    tgt_pk_columns = table_mapping.tgt_primary_keys
    return captureTable(tgt_warehouse, tgt_table, tgt_pk_columns, tgt_jdbc_options, tgt_sql_override, tgt_data_load_filter, table_mapping,batch_load_ids=batch_load_ids)

# COMMAND ----------

def generate_col_list(suffix, columns):
    loop = 0
    for column in columns:
        col = column["col_name"]
        if loop == 0:
            col_list = f" {suffix}.{col} {col}_{suffix}"
        else:
            col_list = ", \n".join([col_list, f" {suffix}.{col} {col}_{suffix}"])
        loop += 1
    return col_list

# COMMAND ----------

import json


def generate_join_condition(primary_keys, col_mapping, validation_strategy="primary_key_based"):
    """
    Generate join condition supporting both primary key and hash-based strategies
    Args:
        primary_keys: List of primary key columns
        col_mapping: Column mapping dictionary
        validation_strategy: "primary_key_based" or "hash_based"
    Returns:
        SQL join condition string
    """
    if validation_strategy == "hash_based" or not primary_keys:
        return generate_hash_join_condition()
    
    # Original primary key logic
    loop = 0
    for key in primary_keys:
        if loop == 0:
            join_cond = f" tgt.{key} = src.{key}"
        else:
            join_cond = " AND \n".join([join_cond, f" tgt.{key} = src.{key}"])
        loop += 1
    return join_cond

# COMMAND ----------

def create_full_outer_report(
    src_table, tgt_table, src_columns, tgt_columns, table_mapping
):

    primary_keys = table_mapping.tgt_primary_keys
    validation_data_db = table_mapping.validation_data_db
    col_mapping = table_mapping.col_mapping
    workflow_name = table_mapping.workflow_name
    table_family = table_mapping.table_family
    validation_strategy = table_mapping.validation_strategy
    
    # Generate column lists
    src_col_list = generate_col_list("src", src_columns)
    tgt_col_list = generate_col_list("tgt", tgt_columns)
    
    # Generate join condition based on validation strategy
    join_condition = generate_join_condition(primary_keys, col_mapping, validation_strategy)
    
    full_outer_table = f"{validation_data_db}.{workflow_name}___{table_family}__full_outer"
    
    # For hash-based validation, we need to add hash columns
    if validation_strategy == "hash_based":
        # Generate row hash expressions for source and target
        src_columns_list = [col['col_name'] for col in src_columns]
        tgt_columns_list = [col['col_name'] for col in tgt_columns]
        
        src_hash_expr = generate_row_hash_expression(src_columns_list, table_mapping.mismatch_exclude_fields)
        tgt_hash_expr = generate_row_hash_expression(tgt_columns_list, table_mapping.mismatch_exclude_fields)
        
        # Add hash columns to the column lists (both as p_keys and row_hash)
        src_col_list += f",\n{src_hash_expr} as src_p_keys,\n{src_hash_expr} as src_row_hash"
        tgt_col_list += f",\n{tgt_hash_expr} as tgt_p_keys,\n{tgt_hash_expr} as tgt_row_hash"
    if spark.catalog.tableExists(full_outer_table):
        create_full_outer_table_sql = f"""
INSERT INTO TABLE {full_outer_table}
SELECT 
{src_col_list},
{tgt_col_list},
"{run_timestamp}" as VALIDATION_DATE,
"{iteration_name}" as iteration_name,
"{workflow_name}" as workflow_name,
"{table_family}" as table_family
FROM {tgt_table} tgt 
FULL OUTER JOIN {src_table} src
ON 
{join_condition}
"""

    else:
        create_full_outer_table_sql = f"""CREATE TABLE {full_outer_table}
PARTITIONED BY (iteration_name,workflow_name,table_family)
AS SELECT 
{src_col_list},
{tgt_col_list},
"{run_timestamp}" as VALIDATION_DATE,
"{iteration_name}" as iteration_name,
"{workflow_name}" as workflow_name,
"{table_family}" as table_family
FROM {tgt_table} tgt 
FULL OUTER JOIN {src_table} src
ON 
{join_condition}
"""
    logger.info(f"src_columns: {src_columns}")
    logger.info("creating full outer table report")
    spark.sql(create_full_outer_table_sql)
    return full_outer_table

# COMMAND ----------

# def generate_hash_where_condition(primary_keys, col_mapping):
#     loop = 0
#     for tgt_pk in primary_keys:
#         src_pk = generate_src_pk_columns(tgt_pk, col_mapping)
#         if loop == 0:
#             where_cond = f" {tgt_pk} = {src_pk}"
#         else:
#             where_cond = " AND \n".join([where_cond, f" {tgt_pk}_tgt = {src_pk}_src"])
#         loop += 1
#     return where_cond

# COMMAND ----------

def generate_where_condition(primary_keys, col_mapping, validation_strategy="primary_key_based"):
    """
    Generate where condition supporting both primary key and hash-based strategies
    Args:
        primary_keys: List of primary key columns
        col_mapping: Column mapping dictionary
        validation_strategy: "primary_key_based" or "hash_based"
    Returns:
        SQL where condition string
    """
    if validation_strategy == "hash_based" or not primary_keys:
        return generate_hash_where_condition()
    
    # Original primary key logic
    loop = 0
    for key in primary_keys:
        if loop == 0:
            where_cond = f" {key}_tgt = {key}_src"
        else:
            where_cond = " AND \n".join([where_cond, f" {key}_tgt = {key}_src"])
        loop += 1
    return where_cond

# COMMAND ----------

def generate_col_suffix(suffix, columns):
    loop = 0
    for column in columns:
        col = column["col_name"]
        if loop == 0:
            col_suffix = f"{col}_{suffix} {col}"
        else:
            col_suffix = ", ".join([col_suffix, f" {col}_{suffix}  {col}"])
        loop += 1
    return col_suffix

# COMMAND ----------

def getHashAnomalies(src_hash_validation_tbl, tgt_hash_validation_tbl, primary_keys, col_mapping, workflow_name, table_family):
  # where_condition = generate_hash_where_condition(primary_keys, col_mapping)

  anomalies_hash = spark.sql(f"""
  select 'matches' as comparison_type, a.p_keys, a.row_hash as src_row_hash, b.row_hash as tgt_row_hash, cast("{run_timestamp}" as timestamp) as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family from {src_hash_validation_tbl} a inner join {tgt_hash_validation_tbl} b on a.p_keys = b.p_keys and a.row_hash = b.row_hash and a.iteration_name__mmp = b.iteration_name__mmp where a.iteration_name__mmp = '{iteration_name}'
  UNION
  select 'mismatches' as comparison_type, a.p_keys, a.row_hash as src_row_hash, b.row_hash as tgt_row_hash, cast("{run_timestamp}" as timestamp) as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family from {src_hash_validation_tbl} a inner join {tgt_hash_validation_tbl} b on a.p_keys = b.p_keys and a.row_hash != b.row_hash and a.iteration_name__mmp = b.iteration_name__mmp where a.iteration_name__mmp = '{iteration_name}' 
  UNION
  select 'src_extras' as comparison_type, a.p_keys, a.row_hash as src_row_hash, NULL as tgt_row_hash, cast("{run_timestamp}" as timestamp) as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family from {src_hash_validation_tbl} a left anti join {tgt_hash_validation_tbl} b on a.p_keys = b.p_keys and a.iteration_name__mmp = b.iteration_name__mmp where a.iteration_name__mmp = '{iteration_name}'
  UNION
  select 'tgt_extras' as comparison_type, a.p_keys, NULL as src_row_hash, a.row_hash as tgt_row_hash, cast("{run_timestamp}" as timestamp) as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family from {tgt_hash_validation_tbl} a left anti join {src_hash_validation_tbl} b on a.p_keys = b.p_keys and a.iteration_name__mmp = b.iteration_name__mmp where a.iteration_name__mmp = '{iteration_name}'
  """)

  src_anomalies_hash_row_list = (anomalies_hash.filter(col("comparison_type")=='mismatches').limit(1000).select('p_keys')).union(anomalies_hash.filter(col("comparison_type")=='src_extras').select('p_keys').limit(1000)).collect()

  tgt_anomalies_hash_row_list = (anomalies_hash.filter(col("comparison_type")=='mismatches').limit(1000).select('p_keys')).union(anomalies_hash.filter(col("comparison_type")=='tgt_extras').select('p_keys').limit(1000)).collect()

  src_anomalies_hash_list = [row['p_keys'] for row in src_anomalies_hash_row_list]
  tgt_anomalies_hash_list = [row['p_keys'] for row in tgt_anomalies_hash_row_list]


  src_anomalies_hash_in_clause = ', '.join([f"'{item}'" for item in src_anomalies_hash_list])
  tgt_anomalies_hash_in_clause = ', '.join([f"'{item}'" for item in tgt_anomalies_hash_list])



#   print(src_anomalies_hash_in_clause, tgt_anomalies_hash_in_clause)
#   print("anomalies_hash")
#   anomalies_hash.show()


  logger.info("writing table hash anomalies")
  anomalies_hash.write.mode("append").saveAsTable(TABLE_HASH_ANOMALIES)

  # src_rec_count = 
  # tgt_rec_count = 

  return src_anomalies_hash_in_clause, tgt_anomalies_hash_in_clause


# COMMAND ----------

def generate_sql_override_for_hash_anomalies(table, sql_override, pk_columns, anomalies_hash_in_clause, src_path=None):
  table = sql_override.format(**locals()) if sql_override else f"select * from {table}"
  
  # Handle case where pk_columns is empty (for hash-based validation without primary keys)
  if pk_columns and pk_columns.strip():
    pk_columns_list = pk_columns.split(",")
    pk_columns_formatted = ", ".join([f"COALESCE(CAST({key} AS STRING),'')" for key in pk_columns_list])
    condition = f"""concat_ws(":",{pk_columns_formatted}) IN ({anomalies_hash_in_clause})""" if anomalies_hash_in_clause else "FALSE"
  else:
    # For hash-based validation without primary keys, we can't filter by primary keys
    # Instead, we'll use a different approach or return the full table
    condition = "FALSE"  # This will effectively return no rows, which might be the intended behavior

  if src_path is None:
    new_sql_override = f"""SELECT * from ({table})t where {condition}"""
  else:
    new_sql_override = condition
#   print(new_sql_override)
  return new_sql_override

# COMMAND ----------

# Row-Level Hashing Functions for Tables Without Primary Keys

def generate_row_hash_expression(columns, exclude_fields=None):
    """
    Generate SQL expression to create row hash from all columns
    Args:
        columns: List of column names
        exclude_fields: List of fields to exclude from hashing
    Returns:
        SQL expression for row hash
    """
    if exclude_fields is None:
        exclude_fields = []
    
    # Filter out excluded fields and metadata columns
    hash_columns = [col for col in columns 
                   if col not in exclude_fields 
                   and col not in ['run_timestamp__mmp', 'iteration_name__mmp']]
    
    # Create hash expression using MD5 of concatenated values
    hash_expr = f"MD5(CONCAT_WS('|', {', '.join([f'COALESCE(CAST({col} AS STRING), \'NULL\')' for col in hash_columns])}))"
    
    return hash_expr

def create_row_hash_validation_table(table_name, columns, exclude_fields, validation_data_db, workflow_name, table_family, run_timestamp, iteration_name):
    """
    Create validation table with row hashes instead of primary keys
    Args:
        table_name: Source table name
        columns: List of column names
        exclude_fields: Fields to exclude from hashing
        validation_data_db: Validation database name
        workflow_name: Workflow name
        table_family: Table family name
        run_timestamp: Run timestamp
        iteration_name: Iteration name
    Returns:
        Tuple of (hash_table_name, create_sql)
    """
    row_hash_expr = generate_row_hash_expression(columns, exclude_fields)
    hash_table_name = f"{validation_data_db}.{workflow_name}___{table_family.replace('.', '_')}__row_hash_validation"
    
    create_sql = f"""
    CREATE OR REPLACE TABLE {hash_table_name} AS
    SELECT 
        {row_hash_expr} as p_keys,
        {row_hash_expr} as row_hash,
        '{table_name}' as source_table,
        CAST('{run_timestamp}' AS TIMESTAMP) as run_timestamp,
        '{iteration_name}' as iteration_name__mmp
    FROM {table_name}
    """
    
    return hash_table_name, create_sql

def generate_hash_join_condition():
    """
    Generate join condition for hash-based comparison
    Returns:
        SQL join condition string
    """
    return "src.p_keys = tgt.p_keys"

def generate_hash_where_condition():
    """
    Generate where condition for hash-based comparison
    Returns:
        SQL where condition string
    """
    return "src_p_keys = tgt_p_keys"

def getHashAnomaliesNoPK(src_hash_validation_tbl, tgt_hash_validation_tbl, workflow_name, table_family, run_timestamp, iteration_name):
    """
    Hash-based anomaly detection for tables without primary keys
    Args:
        src_hash_validation_tbl: Source hash validation table
        tgt_hash_validation_tbl: Target hash validation table
        workflow_name: Workflow name
        table_family: Table family name
        run_timestamp: Run timestamp
        iteration_name: Iteration name
    Returns:
        DataFrame with hash-based anomalies
    """
    anomalies_hash = spark.sql(f"""
    SELECT 
        'matches' as comparison_type, 
        a.p_keys,
        a.row_hash as src_row_hash, 
        b.row_hash as tgt_row_hash,
        CAST('{run_timestamp}' AS TIMESTAMP) as run_timestamp, 
        '{iteration_name}' as iteration_name, 
        '{workflow_name}' as workflow_name, 
        '{table_family}' as table_family 
    FROM {src_hash_validation_tbl} a 
    INNER JOIN {tgt_hash_validation_tbl} b 
        ON a.p_keys = b.p_keys 
        AND a.iteration_name__mmp = b.iteration_name__mmp 
    WHERE a.iteration_name__mmp = '{iteration_name}'
    
    UNION ALL
    
    SELECT 
        'src_extras' as comparison_type, 
        a.p_keys,
        a.row_hash as src_row_hash, 
        NULL as tgt_row_hash,
        CAST('{run_timestamp}' AS TIMESTAMP) as run_timestamp, 
        '{iteration_name}' as iteration_name, 
        '{workflow_name}' as workflow_name, 
        '{table_family}' as table_family 
    FROM {src_hash_validation_tbl} a 
    LEFT ANTI JOIN {tgt_hash_validation_tbl} b 
        ON a.p_keys = b.p_keys 
        AND a.iteration_name__mmp = b.iteration_name__mmp 
    WHERE a.iteration_name__mmp = '{iteration_name}'
    
    UNION ALL
    
    SELECT 
        'tgt_extras' as comparison_type, 
        b.p_keys,
        NULL as src_row_hash, 
        b.row_hash as tgt_row_hash,
        CAST('{run_timestamp}' AS TIMESTAMP) as run_timestamp, 
        '{iteration_name}' as iteration_name, 
        '{workflow_name}' as workflow_name, 
        '{table_family}' as table_family 
    FROM {tgt_hash_validation_tbl} b 
    LEFT ANTI JOIN {src_hash_validation_tbl} a 
        ON a.p_keys = b.p_keys 
        AND a.iteration_name__mmp = b.iteration_name__mmp 
    WHERE b.iteration_name__mmp = '{iteration_name}'
    """)
    
    return anomalies_hash

# COMMAND ----------

import json
from pyspark.sql.functions import col


def generate_validation_results(
    src_tbl,
    tgt_tbl,
    table_mapping,
    run_timestamp,
    iteration_name,
    missing_src_cols, 
    missing_tgt_cols
):

    logger.info("The Target Table is : " + tgt_tbl)
    workflow_name = table_mapping.workflow_name
    table_family = table_mapping.table_family
    tgt_primary_keys = table_mapping.tgt_primary_keys
    addtnl_filters = table_mapping.addtnl_filters
    validation_data_db = table_mapping.validation_data_db
    col_mapping = table_mapping.col_mapping

    src_columns = (
        spark.sql(f"show columns from {src_tbl}")
        .filter(~col("col_name").isin("run_timestamp__mmp", "iteration_name__mmp"))
        .collect()
    )
    # replace the column names with the mapped column names from the user
    src_columns = [col_mapping.get(sc, sc) for sc in src_columns]

    # print(f"show columns from {src_tbl}")
    # print(src_columns)
    tgt_columns = (
        spark.sql(f"show columns from {tgt_tbl}")
        .filter(~col("col_name").isin("run_timestamp__mmp", "iteration_name__mmp"))
        .collect()
    )

    logger.info(
        f"Triggering Validation Capture for : Target Table - {tgt_tbl} and Source Table - {src_tbl} for the Iteration - {iteration_name}"
    )

    full_outer_table = create_full_outer_report(
        src_tbl, tgt_tbl, src_columns, tgt_columns, table_mapping
    )
    where_condition = generate_where_condition(tgt_primary_keys, col_mapping, table_mapping.validation_strategy)

    capture_mismatches = []
    for addtnl_filter in json.loads(addtnl_filters):
        if addtnl_filter["capture_mismatches"]:
            capture_mismatches.append(addtnl_filter["filter"])
    capture_mismatches = ",".join(f'"{cm}"' for cm in capture_mismatches)

    df_list = []

    print("Columns List :")
    for a in tgt_columns:
        column = a["col_name"]
        print(a["col_name"])
        # the following is to prevent the <=> issue with situations like SELECT cast (null as int) <=> "THIS COLUMN DOES NOT EXIST IN SRC" AS expression_output; This is resulting in true which is not correct as per the requirement
        if column in missing_src_cols:
            column_where_condition = f"(cast ({column}_tgt as string) <=> {column}_src)"
        elif column in missing_tgt_cols:
            column_where_condition = f"(cast ({column}_src as string) <=> {column}_tgt)"
        else:
            column_where_condition = f"({column}_tgt <=> {column}_src)"
        sql = f"""
      select cast("{run_timestamp}" as timestamp) run_timestamp, 
      "{table_family}" table_family,
      "{tgt_tbl}" tgt_tbl, 
      "{column}" col_name, 
      "N/A" addtnl_filter, 
      "{iteration_name}" as iteration_name,
      (select count(*) from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}") total_overlaps, 
      (select count(*) from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" and {column_where_condition}) total_matches, 
      (select count(*) from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" and {column_where_condition}) not_null_matches, 
      (select count(*) from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" and not {column_where_condition}) mismatches
      """
        df = spark.sql(sql)
        df_list.append(df.collect()[0])
        for addtnl_filter_entry in json.loads(addtnl_filters):
            if addtnl_filter_entry["filter_name"] == "N/A":
                continue
            addtnl_filter = addtnl_filter_entry["filter"]
            addtnl_filter_compliled = addtnl_filter.format(**locals())
            sql_addtnl = f"""
        select cast("{run_timestamp}" as timestamp) run_timestamp, 
        "{table_family}" table_family,
        "{tgt_tbl}" tgt_tbl, 
        "{column}" col_name, 
        "{addtnl_filter}" addtnl_filter, 
        "{iteration_name}" as iteration_name,
        (select count(*) from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" AND ({addtnl_filter_compliled})) total_overlaps, 
        (select count(*) from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" AND ({addtnl_filter_compliled}) and {column_where_condition}) total_matches, 
        (select count(*) from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" AND ({addtnl_filter_compliled}) and {column_where_condition}) not_null_matches, 
        (select count(*) from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" AND ({addtnl_filter_compliled}) and not {column_where_condition}) mismatches
        """
            df_addtnl = spark.sql(sql_addtnl)
            df_list.append(df_addtnl.collect()[0])
    df1 = spark.createDataFrame(data=df_list)
    for df in df1.filter(
        f"mismatches>0 AND addtnl_filter IN ({capture_mismatches})"
    ).collect():
        table_family = df.table_family
        col_name = df.col_name

        # the following is to prevent the <=> issue with situations like SELECT cast (null as int) <=> "THIS COLUMN DOES NOT EXIST IN SRC" AS expression_output; This is resulting in true which is not correct as per the requirement
        if col_name in missing_src_cols:
            column_where_condition = f"(cast ({col_name}_tgt as string) <=> {col_name}_src)"
        elif col_name in missing_tgt_cols:
            column_where_condition = f"(cast ({col_name}_src as string) <=> {col_name}_tgt)"
        else:
            column_where_condition = f"({col_name}_tgt <=> {col_name}_src)"

        print(table_family, col_name)
        addtnl_filter = df.addtnl_filter
        addtnl_filter_compliled = (
            addtnl_filter.format(**locals()) if (addtnl_filter != "N/A") else "1=1"
        )
        iteration_name = df.iteration_name
        tgt_col_suffix = generate_col_suffix("tgt", tgt_columns)
        src_col_suffix = generate_col_suffix("src", tgt_columns)
        mismatches_data_table = f"{validation_data_db}.{workflow_name}____{table_family}__mismatch_data"
        primary_keys_string = ", ".join(table_mapping.tgt_primary_keys)
        mismatches_data_sql = f"""
                      select cast("{run_timestamp}" as timestamp) run_timestamp, 
                      "{col_name}" col_name, 
                      "{addtnl_filter}" addtnl_filter, 
                      "{iteration_name}" as iteration_name,
                      *
                      from (
                      (select "tgt_mismatches" mismatch_tbl, {tgt_col_suffix} from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" AND ({addtnl_filter_compliled}) and not {column_where_condition})
                      union
                      (select "src_mismatches" mismatch_tbl, {src_col_suffix} from {full_outer_table} where ({where_condition}) and iteration_name = "{iteration_name}" AND ({addtnl_filter_compliled}) and not {column_where_condition})
                      )
                      order by {primary_keys_string}, col_name, mismatch_tbl
                      """

        if spark.catalog.tableExists(mismatches_data_table):
            print(
                f'Inserting "mismatched data grouped at mismatched column level" records into table: {mismatches_data_table} to the "iteration_name" partition {iteration_name}'
            )
            spark.sql(
                f"INSERT INTO TABLE {mismatches_data_table} PARTITION (iteration_name) {mismatches_data_sql}"
            )
        else:
            print(
                f'Table "{mismatches_data_table}" does not exist. Creating and Inserting "mismatched data grouped at mismatched column level" records into table: {mismatches_data_table} to the "iteration_name" partition {iteration_name}'
            )
            spark.sql(
                f"CREATE TABLE {mismatches_data_table} PARTITIONED BY (iteration_name) AS {mismatches_data_sql}"
            )

    logger.info(f"Writing to {MISMATCH_METRICS}...")
    df1.write.mode("append").saveAsTable(MISMATCH_METRICS)
    return full_outer_table

# COMMAND ----------

from pyspark.sql.functions import col


def windowed_validation(
    src_tbl,
    tgt_tbl,
    table_mapping,
    run_timestamp,
    iteration_name,
):
    windowed_validation_tbl = WINDOWED_VALIDATION_METRICS
    update_tstmp_timeline_table_name = UPDATE_TSTMP_TIMELINE

    logger.info("The Target Table is : " + tgt_tbl)

    col_mapping = table_mapping.col_mapping

    workflow_name = table_mapping.workflow_name
    table_family = table_mapping.table_family
    date_bucket = table_mapping.date_bucket
    filter = table_mapping.filter if (not table_mapping.filter is None) else "1=1"
    tgt_primary_keys = table_mapping.tgt_primary_keys
    validation_data_db = table_mapping.validation_data_db

    # mismatch_exclude_fields_string = table_mapping.mismatch_exclude_fields
    # mismatch_exclude_fields_compiled = mismatch_exclude_fields_string.format(**locals())
    # mismatch_exclude_fields = mismatch_exclude_fields_compiled.split("|")
    mismatch_exclude_fields = table_mapping.mismatch_exclude_fields
    columns_for_mismatch_validation = ",".join(
        list(
            spark.sql(f"show columns from {tgt_tbl}")
            .filter(~col("col_name").isin(mismatch_exclude_fields))
            .toPandas()["col_name"]
        )
    )
    join_condition = generate_join_condition(tgt_primary_keys, col_mapping, table_mapping.validation_strategy)

    anomalies_table_name = f"{validation_data_db}.{workflow_name}___{table_family}__anomalies"

    src_columns = (
        spark.sql(f"show columns from {src_tbl}")
        .filter(~col("col_name").isin("run_timestamp__mmp", "iteration_name__mmp"))
        .collect()
    )

    # replace the column names with the mapped column names from the user
    src_columns = [col_mapping.get(sc, sc) for sc in src_columns]

    tgt_columns = (
        spark.sql(f"show columns from {tgt_tbl}")
        .filter(~col("col_name").isin("run_timestamp__mmp", "iteration_name__mmp"))
        .collect()
    )

    tgt_extras_sql = f"select * from (select tgt.* from {tgt_tbl} tgt LEFT ANTI JOIN {src_tbl} src ON {join_condition}) where ({filter})"

    print("*tgt_extras_sql**")
    spark.sql(tgt_extras_sql).show()

    src_extras_sql = f"select * from(select src.* from {src_tbl} src LEFT ANTI JOIN {tgt_tbl} tgt ON ({join_condition})) where ({filter})"

    print("*src_extras_sql**")
    spark.sql(src_extras_sql).show()

    tgt_mismatches_sql = f"""
  (with x as (select * from (select tgt.* from {tgt_tbl} tgt Inner Join {src_tbl} src ON ({join_condition})) where ({filter})),
        y as (select * from (select src.* from {tgt_tbl} tgt Inner Join {src_tbl} src ON ({join_condition})) where ({filter}))
  select tgt.* from x tgt inner join (
  select {columns_for_mismatch_validation} from x
  except all
  select {columns_for_mismatch_validation} from y) src 
  ON ({join_condition})
  )"""
    
    print("*tgt_mismatches_sql**")
    spark.sql(tgt_mismatches_sql).show()

    src_mismatches_sql = f"""
  (with x as (select * from (select tgt.* from {tgt_tbl} tgt Inner Join {src_tbl} src ON ({join_condition})) where ({filter})),
        y as (select * from (select src.* from {tgt_tbl} tgt Inner Join {src_tbl} src ON ({join_condition})) where ({filter}))
  select src.* from y src inner join (
  select {columns_for_mismatch_validation} from y
  except all
  select {columns_for_mismatch_validation} from x) tgt
  ON ({join_condition})
  )"""
    
    print("*src_mismatches_sql**")
    spark.sql(src_mismatches_sql).show()

    insert_anomalies = f"""select * from (
  select "tgt_extras" as type, *, "{iteration_name}" as iteration_name, "{workflow_name}" as workflow_name, "{table_family}" as table_family from ({tgt_extras_sql}) union 
  select "src_extras" as type, *, "{iteration_name}" as iteration_name, "{workflow_name}" as workflow_name, "{table_family}" as table_family from ({src_extras_sql}) union
  select "tgt_mismatches" as type, *, "{iteration_name}" as iteration_name, "{workflow_name}" as workflow_name, "{table_family}" as table_family from ({tgt_mismatches_sql}) union
  select "src_mismatches" as type, *, "{iteration_name}" as iteration_name, "{workflow_name}" as workflow_name, "{table_family}" as table_family from ({src_mismatches_sql}) 
  ) anomalies
  """
    
    print("**insert_anomalies**",insert_anomalies)

    spark.sql(insert_anomalies).show()

    if spark.catalog.tableExists(anomalies_table_name):
        print(
            f'Inserting "anomalies" records into table: {anomalies_table_name} to the "iteration_name" partition {iteration_name}, {workflow_name}, {table_family}'
        )
        spark.sql(
            f"INSERT INTO TABLE {anomalies_table_name} PARTITION (iteration_name, workflow_name, table_family) {insert_anomalies}"
        )
    else:
        print(
            f'Table "{anomalies_table_name}" does not exist. Creating and Inserting "anomalies" records into table: {anomalies_table_name} to the "iteration_name" partition {iteration_name}, {workflow_name}, {table_family}'
        )
        spark.sql(
            f"CREATE TABLE {anomalies_table_name} PARTITIONED BY (iteration_name, workflow_name, table_family) AS {insert_anomalies}"
        )

    if not date_bucket is None:
        windowed_sql = f"""
    INSERT INTO {windowed_validation_tbl} (workflow_name, table_family, run_timestamp, iteration_name, table_type, last_update_date_field, last_updated_date, num_records)
    (select "{workflow_name}", "{table_family}", "{run_timestamp}", "{iteration_name}","src" as tbl, "{date_bucket}", date({date_bucket}) last_updated_date, count(*)  records from {src_tbl} where ({filter}) group by all)
    union
    (select "{workflow_name}", "{table_family}", "{run_timestamp}", "{iteration_name}","tgt" as tbl, "{date_bucket}", date({date_bucket}) last_updated_date,count(*) records from {tgt_tbl}  where ({filter}) group by all)
    union 
    (select "{workflow_name}", "{table_family}", "{run_timestamp}", "{iteration_name}","tgt_extras" as tbl, "{date_bucket}", date({date_bucket}) last_updated_date,count(*) from ({tgt_extras_sql}) group by all)
    union
    (select "{workflow_name}", "{table_family}", "{run_timestamp}", "{iteration_name}","src_extras" as tbl, "{date_bucket}", date({date_bucket}) last_updated_date,count(*) from ({src_extras_sql}) group by all)
    union
    (select "{workflow_name}", "{table_family}", "{run_timestamp}", "{iteration_name}","tgt_mismatches" as tbl, "{date_bucket}", date({date_bucket}) last_updated_date,count(*) from ({tgt_mismatches_sql})group by all)
    union
    (select "{workflow_name}", "{table_family}", "{run_timestamp}", "{iteration_name}","src_mismatches" as tbl, "{date_bucket}", date({date_bucket}) last_updated_date,count(*) from ({src_mismatches_sql})group by all)
    """
        logger.info(
            f'Inserting "windowed validation results" into table: {windowed_validation_tbl} to the "iteration_name" partition {iteration_name}, {workflow_name}, {table_family}'
        )
        spark.sql(windowed_sql)

        update_tstmp_timeline_sql = f"""INSERT INTO {update_tstmp_timeline_table_name} (run_timestamp, workflow_name, table_family, type, date_bucket, last_update_tstmp, iteration_name)
              (select "{run_timestamp}", "{workflow_name}", "{table_family}", "src_min_update" type, "{date_bucket}" date_bucket, (select min({date_bucket}) from {src_tbl} where ({filter})), "{iteration_name}")
              union
              (select "{run_timestamp}", "{workflow_name}", "{table_family}", "src_max_update" type, "{date_bucket}" date_bucket, (select max({date_bucket}) from {src_tbl} where ({filter})), "{iteration_name}")
              union
              (select "{run_timestamp}", "{workflow_name}", "{table_family}", "tgt_min_update" type, "{date_bucket}" date_bucket, (select min({date_bucket}) from {tgt_tbl} where ({filter})), "{iteration_name}")
              union
              (select "{run_timestamp}", "{workflow_name}", "{table_family}", "tgt_max_update" type, "{date_bucket}" date_bucket, (select max({date_bucket}) from {tgt_tbl} where ({filter})), "{iteration_name}")"""

        logging.info(
            f'Inserting "last update tstmp timeline" records into table: {update_tstmp_timeline_table_name} to the "iteration_name" partition {iteration_name}, {workflow_name}, {table_family}'
        )
        spark.sql(update_tstmp_timeline_sql)

# COMMAND ----------

def get_rec_counts_with_primary_keys(
    table_mapping, table, validation_tbl, run_timestamp, iteration_name
):
    """
    Enhanced record count function supporting both primary key and hash-based strategies
    """
    workflow_name = table_mapping.workflow_name
    table_family = table_mapping.table_family
    validation_strategy = table_mapping.validation_strategy
    
    if validation_strategy == "hash_based":
        # For hash-based validation, we don't have primary keys
        # The p_keys column in hash validation tables is actually a row hash, not a primary key
        # We should only count total records and distinct row hashes
        # try:
            # Try to get column names from the validation table
        columns_df = spark.sql(f"SHOW COLUMNS FROM {validation_tbl}")
        columns = [row.col_name for row in columns_df.collect()]

        df = spark.sql(
            f"select '{table}' as table_name, (select count(*) from {validation_tbl}) as total_record_count, (select count(distinct row_hash) from {validation_tbl}) as distinct_key_count, to_timestamp('{run_timestamp}') as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family"
        )
            
        #     if 'row_hash' in columns:
        #         # This is a hash validation table - count total records and distinct row hashes
        #         # Note: p_keys in hash validation is actually a row hash, not a primary key
        #         df = spark.sql(
        #             f"select '{table}' as table_name, (select count(*) from {validation_tbl}) as total_record_count, NULL::LONG as distinct_key_count, (select count(distinct row_hash) from {validation_tbl}) as distinct_hash_count, to_timestamp('{run_timestamp}') as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family"
        #         )
        #     else:
        #         # This is a regular validation table without hash columns, use total count only
        #         df = spark.sql(
        #             f"select '{table}' as table_name, (select count(*) from {validation_tbl}) as total_record_count, NULL::LONG as distinct_key_count, NULL::LONG as distinct_hash_count, to_timestamp('{run_timestamp}') as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family"
        #         )
        # except Exception as e:
        #     # Fallback: assume it's a regular table and use total count
        #     print(f"Warning: Could not determine table structure for {validation_tbl}, using fallback approach: {e}")
        #     df = spark.sql(
        #         f"select '{table}' as table_name, (select count(*) from {validation_tbl}) as total_record_count, NULL::LONG as distinct_key_count, NULL::LONG as distinct_hash_count, to_timestamp('{run_timestamp}') as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family"
        #     )
    else:
        # Original primary key logic
        primary_keys = ", ".join(table_mapping.tgt_primary_keys)
        df = spark.sql(
            f"select '{table}' as table_name, (select count(*) from {validation_tbl}) as total_record_count, (select count(distinct {primary_keys}) from {validation_tbl}) as distinct_key_count, to_timestamp('{run_timestamp}') as run_timestamp, '{iteration_name}' as iteration_name, '{workflow_name}' as workflow_name, '{table_family}' as table_family"
        )
    return df

# COMMAND ----------

def primary_key_validation(
    src_validation_tbl, tgt_validation_tbl, table_mapping, run_timestamp, iteration_name
):
    df_list = []
    tgt_table = table_mapping.tgt_table
    src_table = table_mapping.src_table

    logger.info(f"Triggering Primary Key Validation for the following tables")
    logger.info(f"The Table is: {tgt_table}")

    df_list.append(
        get_rec_counts_with_primary_keys(
            table_mapping,
            src_table,
            src_validation_tbl,
            run_timestamp,
            iteration_name,
        ).collect()[0]
    )
    df_list.append(
        get_rec_counts_with_primary_keys(
            table_mapping,
            tgt_table,
            tgt_validation_tbl,
            run_timestamp,
            iteration_name,
        ).collect()[0]
    )

    df1 = spark.createDataFrame(data=df_list)
    logger.info(f"Writing to {PRIMARY_KEY_VALIDATION}...")
    df1.write.mode("append").saveAsTable(PRIMARY_KEY_VALIDATION)

# COMMAND ----------

def generate_src_alias(all_columns, src_columns, col_mapping):
    loop = 0
    missing_src_cols = []
    cm = col_mapping
    for column in all_columns:
        col_name = column["col_name"]
        # to avoid the "THIS COLUMN DOES NOT EXIST IN SRC" for the already mapped out/ renamed column from tgt
        if col_name in cm.values():
            continue
        if col_name in src_columns:
            val = col_name
        else:
            val = '"THIS COLUMN DOES NOT EXIST IN SRC"'
            missing_src_cols.append(col_name)
        if (col_name in cm.keys()):
            src_alias = cm.get(col_name)
        else:
            src_alias = col_name
        if loop == 0:
            src_col_list = f" {val} as {src_alias}"
        else:
            src_col_list = ", \n".join([src_col_list, f" {val} as {src_alias}"])
        loop += 1
    print(src_col_list)
    return src_col_list, missing_src_cols

# COMMAND ----------

def generate_tgt_alias(all_columns, tgt_columns, col_mapping):
    loop = 0
    missing_tgt_cols = []
    cm = col_mapping

    for column in all_columns:
        col_name = column["col_name"]
       
        # to avoid the "THIS COLUMN DOES NOT EXIST IN TGT" for the already mapped out/ renamed column from src
        if col_name in cm.keys():
            continue
        if col_name in tgt_columns:
            val = col_name
        else:
            val = '"THIS COLUMN DOES NOT EXIST IN TGT"'
            missing_tgt_cols.append(col_name)
        if loop == 0:
            tgt_col_list = f" {val} as {col_name}"
        else:
            tgt_col_list = ", \n".join([tgt_col_list, f" {val} as {col_name}"])
        loop += 1
    print(tgt_col_list)
    return tgt_col_list, missing_tgt_cols

# COMMAND ----------

from pyspark.sql.window import Window

def create_normailzed_views(
    src_validation_tbl, tgt_validation_tbl, col_mapping, iteration_name
):

    src_columns = spark.sql(f"show columns from {src_validation_tbl}").filter(
        ~col("col_name").isin("run_timestamp__mmp", "iteration_name__mmp")
    )
    tgt_columns = spark.sql(f"show columns from {tgt_validation_tbl}").filter(
        ~col("col_name").isin("run_timestamp__mmp", "iteration_name__mmp")
    )

    all_columns = (
        src_columns.withColumn("index", monotonically_increasing_id())
        .withColumn("id", row_number().over(Window.orderBy("index")))
        .union(
            tgt_columns.withColumn("index", monotonically_increasing_id()).withColumn(
                "id", row_number().over(Window.orderBy("index"))
            )
        )
        .drop("index")
        .withColumn("row", row_number().over(Window.partitionBy("col_name").orderBy("id")))
        .filter(col("row") == 1)
        .orderBy("id")
        .drop("row","id")
        .select("col_name")
        .collect()
    )
    # print(src_columns.withColumn("index", monotonically_increasing_id())
    #     .withColumn("id", row_number().over(Window.orderBy("index"))).collect())
    # print(tgt_columns.withColumn("index", monotonically_increasing_id()).withColumn(
    #             "id", row_number().over(Window.orderBy("index"))
    #         ).collect())
    # src_columns.withColumn("index", monotonically_increasing_id()).withColumn("id", row_number().over(Window.orderBy("index"))).union(
    #         tgt_columns.withColumn("index", monotonically_increasing_id()).withColumn(
    #             "id", row_number().over(Window.orderBy("index"))
    #         )
    #     ).drop("index").withColumn("row", row_number().over(Window.partitionBy("col_name").orderBy("id"))).filter(col("row") == 1).orderBy("id").drop("row","id").show()
    
    print(all_columns)
    src_col_list, missing_src_cols = generate_src_alias(
        all_columns, list(src_columns.toPandas()["col_name"]), col_mapping
    )
    tgt_col_list, missing_tgt_cols = generate_tgt_alias(
        all_columns, list(tgt_columns.toPandas()["col_name"]), col_mapping
    )
    src_view_name = f"{src_validation_tbl.split('.')[-1]}_view"
    tgt_view_name = f"{tgt_validation_tbl.split('.')[-1]}_view"
    src_view_sql = f"create or replace temporary view {src_view_name} as select {src_col_list} from {src_validation_tbl} where iteration_name__mmp ='{iteration_name}'"
    tgt_view_sql = f"create or replace temporary view {tgt_view_name} as select {tgt_col_list} from {tgt_validation_tbl} where iteration_name__mmp ='{iteration_name}'"
    spark.sql(src_view_sql)
    spark.sql(tgt_view_sql)
    return (src_view_name, tgt_view_name, missing_src_cols, missing_tgt_cols)

# COMMAND ----------

def capture_metrics(iteration_name, table_mapping, src_validation_tbl, tgt_validation_tbl, src_hash_validation_tbl, tgt_hash_validation_tbl,batch_load_id=None):

  metrics = {}
  metrics['iteration_name'] = iteration_name
  metrics['streamlit_user_name'] = streamlit_user_name
  metrics['streamlit_user_email'] = streamlit_user_email
  metrics['batch_load_id'] = batch_load_id
  metrics['workflow_name'] = workflow_name = table_mapping.workflow_name
  metrics['table_family'] = table_family = table_mapping.table_family
  metrics['src_wrhse'] = table_mapping.src_warehouse
  metrics['src_tbl'] = src_tbl = table_mapping.src_table
  metrics['tgt_wrhse'] = table_mapping.tgt_warehouse
  metrics['tgt_tbl'] = tgt_tbl = table_mapping.tgt_table
  metrics['col_mapping'] = table_mapping.pretty_col_mapping
  metrics['tgt_primary_keys'] = ", ".join(table_mapping.tgt_primary_keys) if table_mapping.tgt_primary_keys else ""
  metrics['exclude_fields_from_summary'] = table_mapping.exclude_fields_from_summary
  metrics['date_bucket'] = table_mapping.date_bucket
  metrics['mismatch_exclude_fields'] = "|".join(table_mapping.mismatch_exclude_fields) if table_mapping.mismatch_exclude_fields else ""
  metrics['src_sql_override'] = table_mapping.src_sql_override
  metrics['tgt_sql_override'] = table_mapping.tgt_sql_override
  metrics['src_data_load_filter'] = table_mapping.src_data_load_filter
  metrics['tgt_data_load_filter'] = table_mapping.tgt_data_load_filter
  metrics['validation_data_db'] = validation_data_db = table_mapping.validation_data_db
  metrics['anomalies_table_name'] = anomalies_table_name = f"{validation_data_db}.{workflow_name}___{table_family}__anomalies"
  metrics['quick_validation'] = table_mapping.quick_validation

  # Check if this is hash-based validation (src_hash_validation_tbl and tgt_hash_validation_tbl are provided)
  if src_hash_validation_tbl and tgt_hash_validation_tbl and table_mapping.validation_strategy == "hash_based":
    # Hash-based validation metrics
    print("Capturing metrics for hash-based validation...")
    metrics['src_records'] = spark.sql(f'select count(*) as src_records from {src_hash_validation_tbl} where iteration_name__mmp ="{iteration_name}"').collect()[0]["src_records"]
    metrics['tgt_records'] = spark.sql(f'select count(*) as tgt_records from {tgt_hash_validation_tbl} where iteration_name__mmp ="{iteration_name}"').collect()[0]["tgt_records"]
    
    # Use hash anomalies table for hash-based validation
    hash_anomalies_table_name = f"{validation_data_db}.{workflow_name}___{table_family.replace('.', '_')}__hash_anomalies"
    metrics['src_extras'] = spark.sql(f'select count(comparison_type) as src_extras from {hash_anomalies_table_name} where iteration_name ="{iteration_name}" and comparison_type= "src_extras"').collect()[0]["src_extras"]
    metrics['tgt_extras'] = spark.sql(f'select count(comparison_type) as tgt_extras from {hash_anomalies_table_name} where iteration_name ="{iteration_name}" and comparison_type= "tgt_extras"').collect()[0]["tgt_extras"]
    metrics['mismatches'] = spark.sql(f'select count(comparison_type) as mismatches from {hash_anomalies_table_name} where iteration_name ="{iteration_name}" and comparison_type= "mismatches"').collect()[0]["mismatches"]
    metrics['matches'] = spark.sql(f'select count(comparison_type) as matches from {hash_anomalies_table_name} where iteration_name ="{iteration_name}" and comparison_type= "matches"').collect()[0]["matches"]
    
    metrics['src_delta_table_size'] = spark.sql(f"""describe detail {src_hash_validation_tbl}""").select("sizeInBytes").collect()[0]["sizeInBytes"]
    metrics['tgt_delta_table_size'] = spark.sql(f"""describe detail {tgt_hash_validation_tbl}""").select("sizeInBytes").collect()[0]["sizeInBytes"]
    metrics['hash_src_records'] = metrics['src_records']
    metrics['hash_tgt_records'] = metrics['tgt_records']
    metrics['hash_mismatches'] = metrics['mismatches']
    metrics['hash_src_extras'] = metrics['src_extras']
    metrics['hash_tgt_extras'] = metrics['tgt_extras']
    metrics['hash_matches'] = metrics['matches']
    metrics['hash_src_delta_table_size'] = metrics['src_delta_table_size']
    metrics['hash_tgt_delta_table_size'] = metrics['tgt_delta_table_size']
  else:
    # Primary key validation metrics (original logic)
    print("Capturing metrics for primary key validation...")
    metrics['src_records'] = spark.sql(f'select total_record_count src_records from {PRIMARY_KEY_VALIDATION} where iteration_name ="{iteration_name}" and lower(table_family) = lower("{table_family}") and table_name = "{src_tbl}"').collect()[0]["src_records"]
    metrics['tgt_records'] = spark.sql(f'select total_record_count tgt_records from {PRIMARY_KEY_VALIDATION} where iteration_name ="{iteration_name}" and lower(table_family) = lower("{table_family}") and table_name = "{tgt_tbl}"').collect()[0]["tgt_records"]
    metrics['src_extras'] = spark.sql(f'select count(type) src_extras from {anomalies_table_name} where iteration_name ="{iteration_name}" and type= "src_extras"').collect()[0]["src_extras"]
    metrics['tgt_extras'] = spark.sql(f'select count(type) tgt_extras from {anomalies_table_name} where iteration_name ="{iteration_name}" and type= "tgt_extras"').collect()[0]["tgt_extras"]
    metrics['mismatches'] = spark.sql(f'select count(type) mismatches from {anomalies_table_name} where iteration_name ="{iteration_name}" and type= "src_mismatches"').collect()[0]["mismatches"]
    metrics['matches'] = metrics['src_records'] - metrics['mismatches'] - metrics['src_extras'] 
    metrics['src_delta_table_size'] = spark.sql(f"""describe detail {src_validation_tbl}""").select("sizeInBytes").collect()[0]["sizeInBytes"]
    metrics['tgt_delta_table_size'] = spark.sql(f"""describe detail {tgt_validation_tbl}""").select("sizeInBytes").collect()[0]["sizeInBytes"]
    metrics['hash_src_records'] = "NULL"
    metrics['hash_tgt_records'] = "NULL"
  metrics['hash_mismatches'] = "NULL"
  metrics['hash_src_extras'] = "NULL"
  metrics['hash_tgt_extras'] = "NULL"
  metrics['hash_matches'] = "NULL"
  metrics['hash_src_delta_table_size'] = "NULL"
  metrics['hash_tgt_delta_table_size'] = "NULL"

  if metrics['quick_validation'] and table_mapping.validation_strategy != "hash_based":
    metrics['hash_src_records'] = spark.sql(f'select count(*) hash_src_records from {src_hash_validation_tbl} where iteration_name__mmp ="{iteration_name}"').collect()[0]["hash_src_records"]
    metrics['hash_tgt_records'] = spark.sql(f'select count(*) hash_tgt_records from {tgt_hash_validation_tbl} where iteration_name__mmp ="{iteration_name}"').collect()[0]["hash_tgt_records"]
    metrics['hash_mismatches'] = spark.sql(f'select count(comparison_type) hash_mismatches from {TABLE_HASH_ANOMALIES} where iteration_name ="{iteration_name}" and workflow_name="{workflow_name}" and table_family="{table_family}" and comparison_type= "mismatches"').collect()[0]["hash_mismatches"]
    metrics['hash_src_extras'] = spark.sql(f'select count(comparison_type) hash_src_extras from {TABLE_HASH_ANOMALIES} where iteration_name ="{iteration_name}" and workflow_name="{workflow_name}" and table_family="{table_family}" and comparison_type= "src_extras"').collect()[0]["hash_src_extras"]
    metrics['hash_tgt_extras'] = spark.sql(f'select count(comparison_type) hash_tgt_extras from {TABLE_HASH_ANOMALIES} where iteration_name ="{iteration_name}" and workflow_name="{workflow_name}" and table_family="{table_family}" and comparison_type= "tgt_extras"').collect()[0]["hash_tgt_extras"]
    metrics['hash_matches'] = metrics['hash_src_records'] - metrics['hash_mismatches'] - metrics['hash_src_extras']
    metrics['hash_src_delta_table_size'] = spark.sql(f"""describe detail {src_hash_validation_tbl}""").select("sizeInBytes").collect()[0]["sizeInBytes"]
    metrics['hash_tgt_delta_table_size'] = spark.sql(f"""describe detail {tgt_hash_validation_tbl}""").select("sizeInBytes").collect()[0]["sizeInBytes"]


  return metrics

# COMMAND ----------

def purge_tables(retain_tables_list,src_validation_tbl,tgt_validation_tbl,full_outer_table):
    logger.info(f"retain_tables_list: {retain_tables_list}")
    retain_list = [] if retain_tables_list is None else retain_tables_list.split("|")
    logger.info(f"Retain Table List: {retain_list}")
    if "src_tbl" not in retain_list: 
        logger.info(f'DROPPING table {src_validation_tbl}')
        spark.sql(f'DROP TABLE {src_validation_tbl}')
    if "tgt_tbl" not in retain_list: 
        logger.info(f'DROPPING table {tgt_validation_tbl}')
        spark.sql(f'DROP TABLE {tgt_validation_tbl}')
    if "full_outer" not in retain_list:
        logger.info(f'DROPPING table {full_outer_table}')
        spark.sql(f'DROP TABLE {full_outer_table}')

# COMMAND ----------

# MAGIC %run "./validation_summary"

# COMMAND ----------

def trigger_validation(table_mapping):
  
  table_mapping_dict = table_mapping.asDict()
  table_mapping = TableMapping(**table_mapping_dict)
  logger.info(f"table_mapping: {table_mapping}")

  workflow_name = table_mapping.workflow_name
  logger.info(f"workflow_name: {workflow_name}")
  #   src_connection_name = table_mapping["src_connection_name"]
  #   src_warehouse = table_configs.select("warehouse").where(f"connection_name == '{src_connection_name}'").collect()[0]["warehouse"]
  src_table = table_mapping.src_table
  logger.info(f"src_table: {src_table}")
  #   tgt_connection_name = table_mapping["tgt_connection_name"]
  #   tgt_warehouse = table_configs.select("warehouse").where(f"connection_name == '{tgt_connection_name}'").collect()[0]["warehouse"]

  src_warehouse = table_mapping.src_warehouse
  logger.info(f"src_warehouse: {src_warehouse}")
  tgt_warehouse = table_mapping.tgt_warehouse
  logger.info(f"tgt_warehouse: {tgt_warehouse}")
  src_jdbc_options = table_mapping.src_jdbc_options
  logger.info(f"src_jdbc_options: {src_jdbc_options}")
  tgt_jdbc_options = table_mapping.tgt_jdbc_options
  logger.info(f"tgt_jdbc_options: {tgt_jdbc_options}")
  tgt_table = table_mapping.tgt_table
  logger.info(f"tgt_table: {tgt_table}")
  col_mapping = table_mapping.col_mapping
  logger.info(f"col_mapping: {col_mapping}")
  tgt_primary_keys = table_mapping.tgt_primary_keys
  logger.info(f"tgt_primary_keys: {tgt_primary_keys}")
  validation_data_db = table_mapping.validation_data_db
  logger.info(f"validation_data_db: {validation_data_db}")
  table_family = table_mapping.table_family
  logger.info(f"table_family: {table_family}")
  src_sql_override = table_mapping.src_sql_override
  logger.info(f"src_sql_override {src_sql_override}")
  tgt_sql_override = table_mapping.tgt_sql_override
  logger.info(f"tgt_sql_override: {tgt_sql_override}")
  src_data_load_filter = table_mapping.src_data_load_filter
  logger.info(f"src_data_load_filter: {src_data_load_filter}")
  tgt_data_load_filter = table_mapping.tgt_data_load_filter
  logger.info(f"tgt_data_load_filter: {tgt_data_load_filter}")
  retain_tables_list = table_mapping.retain_tables_list
  logger.info(f"retain_tables_list: {retain_tables_list}")
  src_cast_to_string = table_mapping.src_cast_to_string
  logger.info(f"src_cast_to_string: {src_cast_to_string}")
  quick_validation = table_mapping.quick_validation
  logger.info(f"quick_validation: {quick_validation}")

  # Define log_update function for status tracking
  def log_update(status):
    spark.sql(f"""
    UPDATE {validation_log_table}
      SET
      validation_run_status = '{status}',
      validation_run_end_time = now()
      WHERE iteration_name = '{iteration_name}'
      AND workflow_name ='{workflow_name}'
      AND src_warehouse = '{src_warehouse}' 
      AND src_table = '{src_table}'
      AND tgt_warehouse = '{tgt_warehouse}'
      AND tgt_table = '{tgt_table}'
      AND table_family = '{table_family}'"""
      )

  # Get source path information (needed for both validation strategies)
#   src_path = [
#     row['source_file_path']
#     for row in spark.sql(f"""
#         select t1.source_file_path
#         from {INGESTION_METADATA_TABLE} t1
#         inner join {INGESTION_AUDIT_TABLE} t2
#             on (t1.table_name = t2.target_table_name and t1.batch_load_id = t2.batch_load_id)
#         where t2.status = 'COMPLETED'
#           and table_name = '{tgt_table}'
#           and t2.batch_load_id = (
#               select max(batch_load_id) as max_batch_load_id
#               from {INGESTION_AUDIT_TABLE}
#               where status = 'COMPLETED'
#                 and target_table_name = '{tgt_table}'
#           )
#         """
#     ).collect()
#     ]
  
  # Get all eligible batches and their source paths at once
  ingestion_data = spark.sql(f"""
    select
        t1.source_file_path,
        t2.batch_load_id
    from {INGESTION_METADATA_TABLE} t1
    inner join {INGESTION_AUDIT_TABLE} t2
        on (t1.table_name = t2.target_table_name and t1.batch_load_id = t2.batch_load_id)
    inner join {INGESTION_CONFIG_TABLE} t4
        on t1.table_name = concat_ws('.', t4.target_catalog, t4.target_schema, t4.target_table)
    left join (select batch_load_id,validation_run_status,row_number() over(partition by batch_load_id order by validation_run_start_time desc) rnk from (
select explode(batch_load_id) as batch_load_id,validation_run_status,validation_run_start_time from {VALIDATION_LOG_TABLE})) t3
        on t3.batch_load_id = t2.batch_load_id
    where t2.status = 'COMPLETED'
        and t1.table_name = '{tgt_table}'
        and (
            (t4.write_mode = 'overwrite' and t2.batch_load_id = (
                select max(batch_load_id)
                from {INGESTION_AUDIT_TABLE}
                where status = 'COMPLETED'
                    and target_table_name = '{tgt_table}'
            ))
            or t4.write_mode <> 'overwrite'
        )
        and ((t3.validation_run_status <> 'SUMMARY_SUCCESS' AND t3.rnk = 1)
            or t3.batch_load_id is null)
    """).collect()
  
  # Handle case where no batches are found
  if not ingestion_data:
      logger.warning(f"No batches found for validation for table: {tgt_table}")
      return
  
  # Collect all batch_load_ids and source paths at once
  batch_load_ids = list(set([row['batch_load_id'] for row in ingestion_data]))
  src_path = [row['source_file_path'] for row in ingestion_data]

  logger.info(f"Processing all eligible batches at once: {batch_load_ids}")
  logger.info(f"Total source files across all batches: {len(src_path)}")
  

#   logger.info(f"Source Files: {src_path}")
  base_file_path = None
  partition_columns = spark.sql(f"""select partition_column from {INGESTION_CONFIG_TABLE}
                             where concat_ws('.',target_catalog, target_schema, target_table) = '{tgt_table}'""")
  partition_columns = partition_columns.first()['partition_column'].split(',')
  logger.info(f"source partition columns: {partition_columns}")
  
  d_partition_col_datatype_mapping_df = spark.sql(f"""select partition_column_name,datatype from {INGESTION_SRC_TABLE_PARTITION_MAPPING} where concat_ws('.',schema_name, table_name) = '{src_table.replace('source_system.','')}' order by index""")

  d_partition_col_datatype_mapping = {row['partition_column_name']: row['datatype'] for row in d_partition_col_datatype_mapping_df.collect()}
  logger.info(f"source partition col datatype: {d_partition_col_datatype_mapping}")

  src_path_part_params = {"partition_columns": partition_columns, "base_file_path": base_file_path, "d_partition_col_datatype_mapping": d_partition_col_datatype_mapping}
  logger.info(f"src_path_part_params: {src_path_part_params}")

  # Capture source and target schemas (needed for both validation strategies)
  log_update("SRC_SCHEMA_INITIATED")
  logger.info("capturing source schema")
  captureSrcSchema(src_warehouse, src_table, src_jdbc_options, table_mapping, src_path, src_path_part_params)
  log_update("TGT_SCHEMA_INITIATED")
  logger.info("capturing target schema")
  captureTgtSchema(tgt_warehouse, tgt_table, tgt_jdbc_options, table_mapping)

  logger.info(f"""Triggering Validation Run for Workflow: ", {workflow_name}, Table Family: {table_family})""")
  logger.info(f"streamlit_user_name: {streamlit_user_name} | streamlit_user_email: {streamlit_user_email}")
  batch_load_id_array = f"array({','.join([repr(x) for x in batch_load_ids])})"

  # Check validation strategy based on primary key availability
  validation_strategy = table_mapping.validation_strategy
  logger.info(f"validation_strategy: {validation_strategy}")

  spark.sql(f"INSERT INTO {validation_log_table} (batch_load_id,workflow_name, src_warehouse, src_table, tgt_warehouse, tgt_table, validation_run_status, validation_run_start_time, streamlit_user_name, streamlit_user_email, iteration_name, table_family) VALUES ({batch_load_id_array},'{workflow_name}', '{src_warehouse}', '{src_table}','{tgt_warehouse}','{tgt_table}','STARTED',now(), '{streamlit_user_name}', '{streamlit_user_email}','{iteration_name}','{table_family}')")
  
  if validation_strategy == "hash_based":
    logger.info(f"Running hash-based validation for {table_family}")
    return run_hash_based_validation(table_mapping, run_timestamp, iteration_name, src_path, src_path_part_params,batch_load_ids)
  else:
    logger.info(f"Running primary key-based validation for {table_family}")
    # Continue with existing primary key-based validation flow

  try:
      
    # logger.info(f"""Triggering Validation Run for Workflow: ", {workflow_name}, Table Family: {table_family})""")
    # logger.info(f"streamlit_user_name: {streamlit_user_name} | streamlit_user_email: {streamlit_user_email}")
    # batch_load_id_array = f"array({','.join([repr(x) for x in batch_load_ids])})"

    # spark.sql(f"INSERT INTO {validation_log_table} (batch_load_id,workflow_name, src_warehouse, src_table, tgt_warehouse, tgt_table, validation_run_status, validation_run_start_time, streamlit_user_name, streamlit_user_email, iteration_name, table_family) VALUES ({batch_load_id_array},'{workflow_name}', '{src_warehouse}', '{src_table}','{tgt_warehouse}','{tgt_table}','STARTED',now(), '{streamlit_user_name}', '{streamlit_user_email}','{iteration_name}','{table_family}')")
    
    src_hash_validation_tbl = None
    tgt_hash_validation_tbl = None
    
    if quick_validation:
        logger.info("capturing source snapshot hash")
        log_update("SRC_SNAPSHOT_HASH_INITIATED")
        src_hash_validation_tbl = captureSrcTableHash(
        src_warehouse, src_table, src_jdbc_options, src_sql_override, src_data_load_filter, table_mapping, src_path, src_path_part_params)
        log_update("SRC_SNAPSHOT_HASH_COMPLETED")

        logger.info("capturing target snapshot hash")
        log_update("TGT_SNAPSHOT_HASH_INITIATED")
        tgt_hash_validation_tbl = captureTgtTableHash(
        tgt_warehouse, tgt_table, tgt_jdbc_options, tgt_sql_override, tgt_data_load_filter, table_mapping,batch_load_ids=batch_load_ids)
        log_update("TGT_SNAPSHOT_HASH_COMPLETED")

        logger.info("capturing hash anomalies")
        log_update("HASH_ANOMALIES_CAPTURE")
        src_anomalies_hash_in_clause, tgt_anomalies_hash_in_clause = getHashAnomalies(
        src_hash_validation_tbl, tgt_hash_validation_tbl, tgt_primary_keys, col_mapping, workflow_name, table_family)
        src_pk_columns = generate_src_columns(tgt_primary_keys, col_mapping)
        tgt_pk_columns = ",".join(tgt_primary_keys)
        src_sql_override = generate_sql_override_for_hash_anomalies(src_table, src_sql_override, src_pk_columns, src_anomalies_hash_in_clause, src_path)
        tgt_sql_override = generate_sql_override_for_hash_anomalies(tgt_table, tgt_sql_override, tgt_pk_columns, tgt_anomalies_hash_in_clause)
        log_update("HASH_ANOMALIES_COMPLETED")
        
    logger.info("capturing source snapshot")
    log_update("SRC_SNAPSHOT_INITIATED")
    src_validation_tbl = captureSrcTable(
        src_warehouse, src_table, src_jdbc_options, src_sql_override, src_data_load_filter, table_mapping,src_path,src_path_part_params)
    log_update("SRC_SNAPSHOT_COMPLETED")

    logger.info("capturing target snapshot")
    log_update("TGT_SNAPSHOT_INITIATED")
    tgt_validation_tbl = captureTgtTable(
        tgt_warehouse, tgt_table, tgt_jdbc_options, tgt_sql_override, tgt_data_load_filter, table_mapping,batch_load_ids=batch_load_ids)
    log_update("TGT_SNAPSHOT_COMPLETED")

    logger.info("creating normalized view")
    log_update("NORM_VIEW_INITIATED")
    src_validation_view, tgt_validation_view, missing_src_cols, missing_tgt_cols = create_normailzed_views(
        src_validation_tbl, tgt_validation_tbl, col_mapping, iteration_name
    )
    log_update("NORM_VIEW_COMPLETED")

    # Only run primary key validation for primary key-based strategy
    if table_mapping.validation_strategy != "hash_based":
        log_update("PK_VALIDATION_INITIATED")
        primary_key_validation(
            src_validation_view,
            tgt_validation_view,
            table_mapping,
            run_timestamp,
            iteration_name,
        )
        log_update("PK_VALIDATION_COMPLETED")
    else:
        print("Skipping primary key validation for hash-based validation strategy")
        log_update("PK_VALIDATION_SKIPPED_FOR_HASH_BASED")

    logger.info("data mismatch validation")
    log_update("MISMATCH_VALIDATION_INITIATED")
    full_outer_table = generate_validation_results(
        src_validation_view,
        tgt_validation_view,
        table_mapping,
        run_timestamp,
        iteration_name,
        missing_src_cols, 
        missing_tgt_cols
    )
    log_update("MISMATCH_VALIDATION_COMPLETED")
    
    logger.info("window based data mismatch validation")
    log_update("WINDOWED_VALIDATION_INITIATED")
    windowed_validation(
        src_validation_view,
        tgt_validation_view,
        table_mapping,
        run_timestamp,
        iteration_name,
    )
    log_update("WINDOWED_VALIDATION_COMPLETED")

    logger.info("capturing metrics")
    log_update("METRICS_CAPTURE_INITIATED")
    table_metrics = capture_metrics(iteration_name, table_mapping, src_validation_tbl, tgt_validation_tbl, src_hash_validation_tbl, tgt_hash_validation_tbl)
    log_update("METRICS_CAPTURE_COMPLETED")

    logger.info("purging temp tables")
    log_update("PURGE_TABLES_INITIATED")
    purge_tables(retain_tables_list,src_validation_tbl,tgt_validation_tbl,full_outer_table)
    log_update("PURGE_TABLES_COMPLETED")
        
    logger.info(f"""Completed Validation Run for Workflow: {workflow_name}; Table Family: {table_family})""")
    log_update("RUN_SUCCESS")

    log_update("SUMMARY_INITIATED")
    runner(table_metrics)
    logger.info(f"""Completed Validation Summary for Workflow: {workflow_name}; Table Family: {table_family})""")
    log_update("SUMMARY_SUCCESS")

  except Exception as exc:
    logger.error(f'Catch inside trigger_validation: {exc}')
    exc= str(exc).replace("'", "\\'")
    spark.sql(f"""
    UPDATE {validation_log_table}
      SET
      validation_run_status = 'FAILED',
      validation_run_end_time = now(),
      exception = '{exc}'
      WHERE iteration_name = '{iteration_name}'
      AND workflow_name ='{workflow_name}'
      AND src_warehouse = '{src_warehouse}' 
      AND src_table = '{src_table}'
      AND tgt_warehouse = '{tgt_warehouse}'
      AND tgt_table = '{tgt_table}'
      AND table_family = '{table_family}'""")

    raise exc
    
    
            

# COMMAND ----------

validation_mapping_table = VALIDATION_MAPPING_TABLE
if(not triggered_from_workflow):
  iteration_name_suffix, workflow_table_families = initializeWidgets(validation_mapping_table)
  logger.info(workflow_table_families)
else :
  iteration_name_suffix = dbutils.widgets.get("01-iteration_name_suffix")
  workflow_table_families = dbutils.widgets.get("03-workflow:table_family")
  logger.info(workflow_table_families)


# COMMAND ----------

def run_hash_based_validation(table_mapping, run_timestamp, iteration_name, src_path=None, src_path_part_params=None,batch_load_ids=None):
    """
    Hash-based validation flow for tables without primary keys
    Always runs quick validation since full validation is designed for primary key-based validation
    Args:
        table_mapping: TableMapping object
        run_timestamp: Run timestamp
        iteration_name: Iteration name
    Returns:
        Validation results
    """
    # Extract variables for logging
    workflow_name = table_mapping.workflow_name
    table_family = table_mapping.table_family
    src_warehouse = table_mapping.src_warehouse
    src_table = table_mapping.src_table
    tgt_warehouse = table_mapping.tgt_warehouse
    tgt_table = table_mapping.tgt_table
    # Use global variables for streamlit user info (not TableMapping attributes)
    validation_log_table = VALIDATION_LOG_TABLE
    
    # Define log update function for hash-based validation
    def log_update(status):
        spark.sql(f"""
        UPDATE {validation_log_table}
          SET
          validation_run_status = '{status}',
          validation_run_end_time = now()
          WHERE iteration_name = '{iteration_name}'
          AND workflow_name ='{workflow_name}'
          AND src_warehouse = '{src_warehouse}' 
          AND src_table = '{src_table}'
          AND tgt_warehouse = '{tgt_warehouse}'
          AND tgt_table = '{tgt_table}'
          AND table_family = '{table_family}'""")
    
    try:
        # print(f"Running hash-based validation for {table_family}")
        # print(f"streamlit_user_name: {streamlit_user_name} | streamlit_user_email: {streamlit_user_email}")
        
        # # Insert initial validation log entry
        # spark.sql(f"INSERT INTO {validation_log_table} (batch_load_id, workflow_name, src_warehouse, src_table, tgt_warehouse, tgt_table, validation_run_status, validation_run_start_time, streamlit_user_name, streamlit_user_email, iteration_name, table_family) VALUES ({batch_load_ids},'{workflow_name}', '{src_warehouse}', '{src_table}','{tgt_warehouse}','{tgt_table}','STARTED',now(), '{streamlit_user_name}', '{streamlit_user_email}','{iteration_name}','{table_family}')")
        
        log_update("HASH_VALIDATION_INITIATED")
        
        # Use source path information passed from trigger_validation
        # logger.info(f"Source Files: {src_path}")
        logger.info(f"src_path_part_params: {src_path_part_params}")
        
        # 1. Get table columns for hash validation
        # log_update("HASH_SCHEMA_INITIATED")
        # src_columns = spark.sql(f"show columns from {table_mapping.src_table}").filter(~col("col_name").isin("run_timestamp__mmp", "iteration_name__mmp")).collect()
        # tgt_columns = spark.sql(f"show columns from {table_mapping.tgt_table}").filter(~col("col_name").isin("run_timestamp__mmp", "iteration_name__mmp")).collect()
        # log_update("HASH_SCHEMA_COMPLETED")
        
        # 2. Create row hash validation tables (for tables without primary keys)
        log_update("HASH_TABLE_CREATION_INITIATED")
        
        # For source table, always use captureSrcTableHash with source path parameters
        logger.info("capturing source snapshot hash with path parameters")
        src_hash_validation_tbl = captureSrcTableHash(
            table_mapping.src_warehouse, table_mapping.src_table, table_mapping.src_jdbc_options, 
            table_mapping.src_sql_override, table_mapping.src_data_load_filter, table_mapping, 
            src_path, src_path_part_params)
        
        # For target table, use captureTgtTableHash for consistency with PK validation
        logger.info("capturing target snapshot hash")
        tgt_hash_validation_tbl = captureTgtTableHash(
            table_mapping.tgt_warehouse, table_mapping.tgt_table, table_mapping.tgt_jdbc_options, 
            table_mapping.tgt_sql_override, table_mapping.tgt_data_load_filter, table_mapping,batch_load_ids=batch_load_ids)
        
        log_update("HASH_TABLE_CREATION_COMPLETED")
        
        # For hash-based validation, we always use quick validation since full validation
        # (with column-level comparisons) is designed for primary key-based validation
        log_update("HASH_ANOMALIES_INITIATED")
        print("Running hash-based validation (always quick validation)...")
        anomalies = getHashAnomaliesNoPK(
            src_hash_validation_tbl, tgt_hash_validation_tbl, 
            table_mapping.workflow_name, table_mapping.table_family,
            run_timestamp, iteration_name
        )
        
        # Save hash anomalies to dedicated table
        hash_anomalies_table_name = f"{table_mapping.validation_data_db}.{table_mapping.workflow_name}___{table_mapping.table_family.replace('.', '_')}__hash_anomalies"
        anomalies.write.mode("append").saveAsTable(hash_anomalies_table_name)
        log_update("HASH_ANOMALIES_COMPLETED")
        
        # Generate validation summary for hash-based validation
        log_update("HASH_SUMMARY_INITIATED")
        print("Generating validation summary for hash-based validation...")
        table_metrics = capture_metrics(iteration_name, table_mapping, None, None, src_hash_validation_tbl, tgt_hash_validation_tbl)
        runner(table_metrics)
        print(f"Completed Validation Summary for Hash-based Workflow: {table_mapping.workflow_name}; Table Family: {table_mapping.table_family}")
        log_update("HASH_SUMMARY_COMPLETED")
        
        log_update("SUCCESS")
        print(f"Completed Hash-based Validation Run for Workflow: {workflow_name}; Table Family: {table_family}")

        logger.info("purging temp tables")
        log_update("PURGE_TABLES_INITIATED")
        purge_tables(retain_tables_list=None,src_hash_validation_tbl=src_hash_validation_tbl,tgt_hash_validation_tbl=tgt_hash_validation_tbl,full_outer_table=None)
        log_update("PURGE_TABLES_COMPLETED")
        
        return {
            'src_hash_table': src_hash_validation_tbl,
            'tgt_hash_table': tgt_hash_validation_tbl,
            'anomalies': anomalies,
            'hash_anomalies_table': hash_anomalies_table_name,
            'validation_strategy': 'hash_based'
        }
        
    except Exception as exc:
        print(f'Catch inside run_hash_based_validation: {exc}')
        exc = str(exc).replace("'", "\\'")
        spark.sql(f"""
        UPDATE {validation_log_table}
          SET
          validation_run_status = 'FAILED',
          validation_run_end_time = now(),
          exception = '{exc}'
          WHERE iteration_name = '{iteration_name}'
          AND workflow_name ='{workflow_name}'
          AND src_warehouse = '{src_warehouse}' 
          AND src_table = '{src_table}'
          AND tgt_warehouse = '{tgt_warehouse}'
          AND tgt_table = '{tgt_table}'
          AND table_family = '{table_family}'""")
        
        raise exc

# COMMAND ----------

# DBTITLE 0,able 
from pyspark.sql.functions import lit, to_timestamp
from concurrent.futures import ThreadPoolExecutor
from  itertools import repeat


table_config_table = TABLE_CONFIG_TABLE
validation_log_table = VALIDATION_LOG_TABLE

run_timestamp, iteration_name = generate_iteration_details(iteration_name_suffix)
# mappings = readValidationTableList(validation_mapping_table, table_families)
mappings = readValidationTableList(validation_mapping_table, table_config_table, workflow_table_families)
# table_configs = read_table_config(table_config_table)
# trigger_validation(mappings[0],table_configs)
# with ThreadPoolExecutor(max_workers=10) as exe:
#   exe.map(trigger_validation,mappings,repeat(table_configs))

# with ThreadPoolExecutor(max_workers=10) as exe:
#   exe.map(trigger_validation,mappings)

with ThreadPoolExecutor(max_workers=PARALLELISM) as exe:
    try:
        for r in exe.map(trigger_validation,mappings):
            try:
                print(r)
            except Exception as exc:
                logger.error(f'Catch inside: {exc}')
    except Exception as exc:
        logger.error(f'Catch outside: {exc}')

# for table_mapping in mappings:
#   trigger_validation(table_mapping)
dbutils.jobs.taskValues.set(key = "iteration_name", value = iteration_name)
logger.info("Validation Job Run Completed")