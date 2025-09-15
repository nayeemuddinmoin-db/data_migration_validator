##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

import pandas as pd
import streamlit as st
import conf.constants as constants
# PySpark and databricks-connect related imports.
from databricks.connect.session import DatabricksSession
# from pyspark.sql.functions import col
# from pyspark.sql.types import StringType
# import pyspark.sql.functions as F
import plotly.express as px
# from streamlit_dynamic_filters import DynamicFilters
# import json
from validation_sqls import retrieve_summary

spark = DatabricksSession.builder.clusterId(constants.CLUSTER_ID).getOrCreate()
# spark = DatabricksSession.builder.serverless(True).getOrCreate()
import altair as alt




iteration_name_choices = [iteration_name[0] for iteration_name in spark.sql(f"select iteration_name from (select distinct iteration_name from {constants.VALIDATION_SUMMARY_TABLE} order by iteration_name desc)x").collect()]
iteration_name_index = 0 if "iteration_name" not in st.session_state else iteration_name_choices.index(st.session_state['iteration_name'])
iteration_name = st.sidebar.selectbox("Iteration Name",iteration_name_choices,index=iteration_name_index) 

workflow_name_choices = [workflow_name[0] for workflow_name in spark.sql(f"SELECT distinct workflow_name from {constants.VALIDATION_SUMMARY_TABLE} where iteration_name = '{iteration_name}' order by workflow_name").collect()]
workflow_name_index = 0 if "workflow_name" not in st.session_state else workflow_name_choices.index(st.session_state['workflow_name'])
workflow_name = st.sidebar.selectbox(
"Workflow Name",workflow_name_choices,index=workflow_name_index) 

table_family_choices = [table_family[0] for table_family in spark.sql(f"SELECT distinct table_family from {constants.VALIDATION_SUMMARY_TABLE} where iteration_name = '{iteration_name}' and workflow_name = '{workflow_name}' order by table_family").collect()]
#get the first in the list in case the session state is not set or if set the value is not present in the selected workflow
table_family_index = 0 if ("table_family" not in st.session_state or st.session_state['table_family'] not in table_family_choices) else table_family_choices.index(st.session_state['table_family'])
table_family = st.sidebar.selectbox(
"Table Family",table_family_choices,index=table_family_index) 

if (spark.sql(f"select count(*) rec_count from {constants.VALIDATION_SUMMARY_TABLE}" ).collect()[0]["rec_count"] == 0):
    st.warning("Kindly initiate the first validation iteration from the Runner page. Upon successful completion, you will be able to access the detailed validation reports in this page.")
    st.stop()

def update_favorite(favorite):
    if favorite == "🤍":
        favorite = True
    else:
        favorite = False
    spark.sql(f"update {constants.VALIDATION_SUMMARY_TABLE} set favorite = {favorite} where iteration_name = '{iteration_name}' and table_family = '{table_family}'")

col1, col2 = st.columns([10,1])
with col1:
    st.header("Detailed Validation Report", divider=True)
with col2:
    favorite = "♥️" if spark.sql(f"select favorite from {constants.VALIDATION_SUMMARY_TABLE} where iteration_name = '{iteration_name}' and table_family = '{table_family}'").collect()[0]["favorite"] else "🤍"
    st.button(favorite,type="tertiary",on_click=update_favorite,args=(favorite,))

sqls = []

databricks = f'''CREATE OR REPLACE TEMPORARY VIEW schema_store_catalog_v as
select "databricks" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",catalog,db_name,table_name) TABLE_NAME, original_order COL_ORDER, col_name COL_NAME, comment COL_COMMENT, data_type DATA_TYPE from {constants.DATABRICKS_SCHEMA_STORE}'''

netezza = f'''select "netezza" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",DATABASE,schema,Name) TABLE_NAME, ATTNUM COL_ORDER, ATTNAME COL_NAME, DESCRIPTION COL_COMMENT, FORMAT_TYPE DATA_TYPE from {constants.NETEZZA_SCHEMA_STORE}'''

snowflake = f'''select "snowflake" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".", TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) TABLE_NAME, ORDINAL_POSITION COL_ORDER, COLUMN_NAME COL_NAME, COMMENT COL_COMMENT, GENERATED_DATA_TYPE DATA_TYPE from (select * ,
case
    lower(DATA_TYPE)
    when 'number' then concat(DATA_TYPE,"(",NUMERIC_PRECISION,",",NUMERIC_SCALE,")")
    when 'text' then concat(DATA_TYPE,"(",CHARACTER_MAXIMUM_LENGTH,")")
    else DATA_TYPE
end as GENERATED_DATA_TYPE
from {constants.SNOWFLAKE_SCHEMA_STORE})x'''

oracle = f'''select "oracle" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",OWNER,TABLE_NAME) TABLE_NAME, COLUMN_ID COL_ORDER, COLUMN_NAME COL_NAME, COMMENTS COL_COMMENT, DATA_TYPE DATA_TYPE from (select * ,
   case
     lower(DATA_TYPE)
     when 'number' then concat(DATA_TYPE,"(",DATA_PRECISION,",",DATA_SCALE,")")
     when 'varchar2' then concat(DATA_TYPE,"(",DATA_LENGTH,")")
     when 'nvarchar2' then concat(DATA_TYPE,"(",DATA_LENGTH,")")
     else DATA_TYPE
   end as GENERATED_DATA_TYPE
from {constants.ORACLE_SCHEMA_STORE})x'''

mssql = f'''select "mssql" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",TABLE_SCHEMA,TABLE_NAME) TABLE_NAME, ORDINAL_POSITION COL_ORDER, COLUMN_NAME COL_NAME, COLUMN_COMMENT COL_COMMENT, DATA_TYPE DATA_TYPE from {constants.MSSQL_SCHEMA_STORE}'''

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
from {constants.TERADATA_SCHEMA_STORE})x'''

hive = f'''select "hive" WAREHOUSE, run_timestamp RUN_TIMESTAMP, iteration_name ITERATION_NAME, workflow_name WORKFLOW_NAME, table_family TABLE_FAMILY, concat_ws(".",db_name,table_name) TABLE_NAME, original_order COL_ORDER, col_name COL_NAME, comment COL_COMMENT, data_type DATA_TYPE from {constants.HIVE_SCHEMA_STORE}'''
 

if spark.catalog.tableExists(constants.DATABRICKS_SCHEMA_STORE):
  sqls.append(databricks)
if spark.catalog.tableExists(constants.NETEZZA_SCHEMA_STORE):
  sqls.append(netezza)
if spark.catalog.tableExists(constants.SNOWFLAKE_SCHEMA_STORE):
  sqls.append(snowflake)
if spark.catalog.tableExists(constants.ORACLE_SCHEMA_STORE):
  sqls.append(oracle)
if spark.catalog.tableExists(constants.MSSQL_SCHEMA_STORE):
  sqls.append(mssql)
if spark.catalog.tableExists(constants.TERADATA_SCHEMA_STORE):
  sqls.append(teradata)
if spark.catalog.tableExists(constants.HIVE_SCHEMA_STORE):
  sqls.append(hive)

sql = " UNION ".join(sqls)
df = spark.sql(sql)

# st.subheader("Validation Summary")

# Function to convert bytes to a human-readable format
def human_readable_size(size_in_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024


with st.expander("Validation Summary", expanded=True):
    # st.subheader("Validation Summary")
    summary_df = retrieve_summary()
        
    summary_df['src_delta_tbl_size'] = summary_df.apply(lambda row: f"{human_readable_size(row['src_delta_tbl_bytes'])} ({row['src_delta_tbl_bytes']} bytes)", axis=1)
    summary_df['tgt_delta_tbl_size'] = summary_df.apply(lambda row: f"{human_readable_size(row['tgt_delta_tbl_bytes'])} ({row['tgt_delta_tbl_bytes']} bytes)", axis=1)

    summary_df_filtered = summary_df[(summary_df['iteration_name'] == iteration_name) & (summary_df['workflow_name'] == workflow_name) & (summary_df['table_family'] == table_family)]

    col1, col2, col3, col4 = st.columns([2,2,2,2],border=True)

    with col1:
        st.caption("Details")
        summary_df_1 = summary_df_filtered[['iteration_name','workflow_name','table_family','src_warehouse','src_table','tgt_warehouse','tgt_table','validation_data_db']]
        summary_df_transpose = summary_df_1.T
        summary_df_transpose.columns = ['Values']
        st.dataframe(summary_df_transpose, width=10000)
    with col2:
        st.caption("Configs")
        summary_df_2 = summary_df_filtered[['col_mapping','tgt_primary_keys','exclude_fields_from_summary_array', 'date_bucket', 'mismatch_exclude_fields', 'src_sql_override', 'tgt_sql_override','src_data_load_filter', 'tgt_data_load_filter', 'quick_validation']]
        summary_df_transpose = summary_df_2.T
        summary_df_transpose.columns = ['Values']
        st.dataframe(summary_df_transpose, width=10000)
    with col3:
        st.caption("Metrics")
        summary_df_3 = summary_df_filtered[['src_delta_tbl_size','tgt_delta_tbl_size', 'src_records','tgt_records', 'src_extras','tgt_extras', 'matches', 'mismatches_before_exclusion']]
        summary_df_transpose = summary_df_3.T
        summary_df_transpose.columns = ['Metrics']
        st.dataframe(summary_df_transpose, width=10000)
    with col4:
        st.caption("Validation Status")
        summary_df_4 = summary_df_filtered[['final_validation_status','primary_key_compliance_status','ordinal_position_compliance_status','col_name_compare_status','data_type_compare_status','comment_compare_status','data_type_compatibility_status','mismatches_status','mismatches_after_exclusion_status','extras_status']]
        summary_df_transpose = summary_df_4.T
        summary_df_transpose.columns = ['Validation Status']
        st.dataframe(summary_df_transpose, width=10000)

# st.divider()

txt = st.text_area(
    "Enter Comments",""
)

st.write(f"You wrote {len(txt)} characters.")

######
if summary_df_filtered['quick_validation'].any():
    st.warning("Please note that this is a report of a quick validation. The mismatch graphs shown are for truncated data and just represent the anomalies from sample of the source and target", icon="⚠️")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Primary Key", "Schema", "Mismatches", "Windowed Validation", "Anomalies"])




def readValidationTableList(validation_mapping_table):
    config_sql = f"select * from {validation_mapping_table} where workflow_name = '{workflow_name}' and table_family = '{table_family}'"
    table_mappings = spark.sql(config_sql).collect()[0]
    return table_mappings


def read_table_config(validation_config_table):
    config_sql = f"select * from {validation_config_table}"
    table_configs = spark.sql(config_sql)
    return table_configs

table_configs = read_table_config(constants.TABLE_CONFIG_TABLE)
table_mapping = readValidationTableList(constants.VALIDATION_MAPPING_TABLE)
src_connection_name = table_mapping["src_connection_name"]
src_wrhse = table_configs.select("warehouse").where(f"connection_name == '{src_connection_name}'").collect()[0]["warehouse"]
src_table = table_mapping["src_table"]
tgt_connection_name = table_mapping["tgt_connection_name"]
tgt_wrhse = table_configs.select("warehouse").where(f"connection_name == '{tgt_connection_name}'").collect()[0]["warehouse"]
src_tbl = table_mapping["src_table"]
tgt_tbl = table_mapping["tgt_table"]
workflow_name = table_mapping["workflow_name"]
table_family = table_mapping["table_family"]
mismatch_exclude_fields = table_mapping["mismatch_exclude_fields"]
primary_keys_string = table_mapping["tgt_primary_keys"].replace("|", ",")

print(src_wrhse, tgt_wrhse, src_tbl, tgt_tbl)

######
def color_survived(val):
    color = '#A8D1D1' if val == 'MATCH' else ('#ff584f' if val == 'MISMATCH' else ('#e0e0e0' if (val == 'BYPASS_MISMATCH' or val == 'BYPASS_MATCH') else ''))
    return f'background-color: {color}; font-style: italic'
def exclude_fields_ordinal(s):
    color = '#e0e0e0' if s.col_name_compare == 'BYPASS_MISMATCH' else ''
    return [f'background-color: {color}']*len(s)

def exclude_fields_data_type(s):
    color = '#e0e0e0' if (s.col_name_compare == 'BYPASS_MISMATCH') else ''
    return [f'background-color: {color}; font-style: italic']*len(s)

with tab1:
    st.header("Primary Key Validation")
    #Primary Key Validation
    st.dataframe(spark.sql(f'''select * from (select *, total_record_count-distinct_key_count as primary_key_violations from {constants.PRIMARY_KEY_VALIDATION}  where iteration_name ="{iteration_name}" and lower(table_family) = lower("{table_family}") )a where primary_key_violations !=0''').toPandas())
    with st.expander("See explanation"):
        st.caption('''
            The chart above shows the tables that violated primary keys. There were duplicates found in the primary key fields. Please make sure if this is expected or update the primary keys to maintain the uniqueness
        ''')
with tab2:
    st.header("Schema Validation")
    st.subheader("Ordinal Position Validation")
    st.dataframe(spark.sql(f'''
    SELECT SRC_COL_NAME,
          TGT_COL_NAME,
          IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),TGT_COL_NAME)OR array_contains (split('{mismatch_exclude_fields}','\\\|'),SRC_COL_NAME), concat("BYPASS_",col_name_compare),col_name_compare) as col_name_compare
          from(
    SELECT
    a.COL_NAME SRC_COL_NAME,
    b.COL_NAME TGT_COL_NAME,
    IF(
        lower(a.COL_NAME) <=> lower(b.COL_NAME),
        'MATCH',
        'MISMATCH'
    ) as col_name_compare,
    coalesce(a.col_order, b.col_order) col_order
    from
    (select * from schema_store_catalog_v where lower(warehouse) = lower('{src_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{src_tbl}"))a
    FULL OUTER JOIN 
    (select * from schema_store_catalog_v where lower(warehouse) = lower('{tgt_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{tgt_tbl}"))b
    ON a.COL_ORDER = b.COL_ORDER
    order by col_order)x
    ''').toPandas().style.apply(exclude_fields_ordinal, axis=1).applymap(color_survived, subset=['col_name_compare']),width=10000)

    st.subheader("Column Name, Data Type, Comment Validation, Data Type Compatibility")


    #Chart

    # df = spark.sql(f'''
    # select col_name, 
    # iteration_name,
    # IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),tgt_col_name) OR array_contains (split('{mismatch_exclude_fields}','\\\|'),src_col_name), concat("BYPASS_",col_name_compare),col_name_compare) as col_name_compare, 
    # IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),tgt_col_name) OR array_contains (split('{mismatch_exclude_fields}','\\\|'),src_col_name), concat("BYPASS_",data_type_compare),data_type_compare) as data_type_compare, 
    # IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),tgt_col_name) OR array_contains (split('{mismatch_exclude_fields}','\\\|'),src_col_name), concat("BYPASS_",comment_compare),comment_compare) as comment_compare, 
    # IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),tgt_col_name) OR array_contains (split('{mismatch_exclude_fields}','\\\|'),src_col_name), concat("BYPASS_",datatype_congruency),datatype_congruency) as datatype_congruency
    # from (                      
    # select *, 
    #         IF(
    #     lower(x.src_COL_NAME) <=> lower(x.tgt_COL_NAME),
    #     'MATCH',
    #     'MISMATCH'
    # ) as col_name_compare,
    # IF(
    #     lower(x.src_DATA_TYPE) <=> lower(x.tgt_DATA_TYPE),
    #     'MATCH',
    #     'MISMATCH'
    # ) as data_type_compare,
    # IF(
    #     lower(x.src_COL_COMMENT) <=> lower(x.tgt_COL_COMMENT),
    #     'MATCH',
    #     'MISMATCH'
    # ) as comment_compare,
    # if(
    #     rlike(
    #         lower(src_data_type),
    #         {src_wrhse}_compatibility_regex),
    #     true,
    #     false
    #     )as data_type_compatibility,
    # if(y.has_precision,  
    # if(
    #     (
    #     regexp_extract(
    #         lower(tgt_data_type),
    #         {src_wrhse}_compatibility_regex,
    #         2
    #     )
    #     ) -(
    #     regexp_extract(
    #         lower(src_data_type),
    #         {src_wrhse}_compatibility_regex,
    #         2
    #     )
    #     ) >= 0,
    #     true,
    #     false
    # ),true) precision_compatible,
    # if(y.has_precision,
    #     if(
    #     (
    #     nvl(int(regexp_extract(
    #         lower(tgt_data_type),
    #         {src_wrhse}_compatibility_regex,
    #         4
    #     )),0)
    #     ) -(
    #     nvl(int(regexp_extract(
    #         lower(src_data_type),
    #         {src_wrhse}_compatibility_regex,
    #         4
    #     )),0)
    #     ) >= 0,
    #     true,
    #     false
    # ),true) scale_compatible,
    # (data_type_compatibility and precision_compatible and scale_compatible) final_compatibility,
    # IF(
    #     final_compatibility,
    #     'MATCH',
    #     'MISMATCH'
    # ) as datatype_congruency
    # from (select coalesce(a.iteration_name,b.iteration_name) as iteration_name, a.table_name src_table_name, b.table_name tgt_table_name, coalesce(a.col_name,b.col_name) as col_name, a.col_name src_col_name, b.col_name tgt_col_name, coalesce(a.col_order, b.col_order) col_order, a.data_type src_data_type, b.data_type tgt_data_type, a.col_comment src_col_comment, b.col_comment tgt_col_comment from (select * from schema_store_catalog_v where lower(warehouse) = lower('{src_wrhse}') and iteration_name ="{iteration_name}" and lower(TABLE_NAME) = lower("{src_tbl}"))a
    # FULL OUTER JOIN 
    # (select * from schema_store_catalog_v where lower(warehouse) = lower('{tgt_wrhse}') and iteration_name ="{iteration_name}" and lower(TABLE_NAME) =lower("{tgt_tbl}"))b
    # ON lower(a.COL_NAME) = lower(b.COL_NAME)
    # -- where lower(a.DATA_TYPE) <> lower(b.DATA_TYPE)
    # order by b.COL_ORDER) x
    # left outer join (select data_type lkp_data_type, warehouse, data_type data_type_aliases, has_precision, {src_wrhse}_compatibility_regex from {constants.DATA_TYPE_COMPATIBILITY_MATRIX}
    # where lower(warehouse) = lower('{tgt_wrhse}')) y
    # on rlike(lower(x.tgt_data_type),lkp_data_type)
    # --where {src_wrhse}_compatibility_regex is not null
    # )q order by COL_ORDER
    # ''').toPandas()

    # selection = alt.selection_single(fields=['iteration_name', 'table_family'], empty='all')

    # chart = (
    #     alt.Chart(df)
    #     .mark_bar()
    #     .encode(
    #         alt.Y("col_name:O"),
    #         alt.X("iteration_name"),
    #         # alt.Tooltip(["iteration_name", "table_family","final_validation_status"]),
    #         alt.Tooltip(['col_name','col_name_compare','data_type_compare','datatype_congruency','comment_compare']),
    #         color=alt.condition(
    #         alt.datum.col_name_compare == 'MATCH',  # Condition for the specific value
    #         # alt.value('#538c50'),          # Color for the specific value
    #         # alt.value('#ff392e')     # Color for other values
    #         alt.value('#A8D1D1'),          # Color for the specific value
    #         alt.value('#ff584f')     # Color for other values
    #     ),
    #     ).add_params(
    #     selection  # This line adds the parameter to the chart
    # )
    #     .interactive()
    # )
    # selected_data = st.altair_chart(chart,use_container_width=False,on_select="rerun",selection_mode=None,key="df_point")

    #DataType Compatibility

    st.dataframe(spark.sql(f'''
    select col_name, 
    IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),tgt_col_name) OR array_contains (split('{mismatch_exclude_fields}','\\\|'),src_col_name), concat("BYPASS_",col_name_compare),col_name_compare) as col_name_compare, 
    IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),tgt_col_name) OR array_contains (split('{mismatch_exclude_fields}','\\\|'),src_col_name), concat("BYPASS_",data_type_compare),data_type_compare) as data_type_compare, 
    IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),tgt_col_name) OR array_contains (split('{mismatch_exclude_fields}','\\\|'),src_col_name), concat("BYPASS_",comment_compare),comment_compare) as comment_compare, 
    IF(array_contains (split('{mismatch_exclude_fields}','\\\|'),tgt_col_name) OR array_contains (split('{mismatch_exclude_fields}','\\\|'),src_col_name), concat("BYPASS_",datatype_congruency),datatype_congruency) as datatype_congruency
    from (                      
    select *,
            IF(
        lower(x.src_COL_NAME) <=> lower(x.tgt_COL_NAME),
        'MATCH',
        'MISMATCH'
    ) as col_name_compare,
    IF(
        lower(x.src_DATA_TYPE) <=> lower(x.tgt_DATA_TYPE),
        'MATCH',
        'MISMATCH'
    ) as data_type_compare,
    IF(
        lower(x.src_COL_COMMENT) <=> lower(x.tgt_COL_COMMENT),
        'MATCH',
        'MISMATCH'
    ) as comment_compare,
    if(
        rlike(
            lower(src_data_type),
            {src_wrhse}_compatibility_regex),
        true,
        false
        )as data_type_compatibility,
    if(y.has_precision,  
    if(
        (
        regexp_extract(
            lower(tgt_data_type),
            {src_wrhse}_compatibility_regex,
            2
        )
        ) -(
        regexp_extract(
            lower(src_data_type),
            {src_wrhse}_compatibility_regex,
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
            {src_wrhse}_compatibility_regex,
            4
        )),0)
        ) -(
        nvl(int(regexp_extract(
            lower(src_data_type),
            {src_wrhse}_compatibility_regex,
            4
        )),0)
        ) >= 0,
        true,
        false
    ),true) scale_compatible,
    (data_type_compatibility and precision_compatible and scale_compatible) final_compatibility,
    IF(
        final_compatibility,
        'MATCH',
        'MISMATCH'
    ) as datatype_congruency
    from (select a.iteration_name, a.table_name src_table_name, b.table_name tgt_table_name, coalesce(a.col_name,b.col_name) as col_name, a.col_name src_col_name, b.col_name tgt_col_name, coalesce(a.col_order, b.col_order) col_order, a.data_type src_data_type, b.data_type tgt_data_type, a.col_comment src_col_comment, b.col_comment tgt_col_comment from (select * from schema_store_catalog_v where lower(warehouse) = lower('{src_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{src_tbl}"))a
    FULL OUTER JOIN 
    (select * from schema_store_catalog_v where lower(warehouse) = lower('{tgt_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) =lower("{tgt_tbl}"))b
    ON lower(a.COL_NAME) = lower(b.COL_NAME)
    -- where lower(a.DATA_TYPE) <> lower(b.DATA_TYPE)
    order by b.COL_ORDER) x
    left outer join (select data_type lkp_data_type, warehouse, data_type data_type_aliases, has_precision, {src_wrhse}_compatibility_regex from {constants.DATA_TYPE_COMPATIBILITY_MATRIX}
    where lower(warehouse) = lower('{tgt_wrhse}')) y
    on rlike(lower(x.tgt_data_type),lkp_data_type)
    --where {src_wrhse}_compatibility_regex is not null
    )q order by COL_ORDER
    ''').toPandas().style.apply(exclude_fields_data_type, axis=1).applymap(color_survived, subset=['datatype_congruency','col_name_compare','data_type_compare','comment_compare']),width=10000)

    with st.expander("More Details"):
        st.dataframe(spark.sql(f'''
        select *,
                IF(
            lower(x.src_COL_NAME) <=> lower(x.tgt_COL_NAME),
            'MATCH',
            'MISMATCH'
        ) as col_name_compare,
        IF(
            lower(x.src_DATA_TYPE) <=> lower(x.tgt_DATA_TYPE),
            'MATCH',
            'MISMATCH'
        ) as data_type_compare,
        IF(
            lower(x.src_COL_COMMENT) <=> lower(x.tgt_COL_COMMENT),
            'MATCH',
            'MISMATCH'
        ) as comment_compare,
        if(
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
        (data_type_compatibility and precision_compatible and scale_compatible) final_compatibility,
        IF(
            final_compatibility,
            'MATCH',
            'MISMATCH'
        ) as datatype_congruency
        from (select a.iteration_name, a.table_name src_table_name, b.table_name tgt_table_name, coalesce(a.col_name,b.col_name) as col_name, a.col_name src_col_name, b.col_name tgt_col_name, coalesce(a.col_order, b.col_order) col_order, a.data_type src_data_type, b.data_type tgt_data_type, a.col_comment src_col_comment, b.col_comment tgt_col_comment from (select * from schema_store_catalog_v where lower(warehouse) = lower('{src_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{src_tbl}"))a
        FULL OUTER JOIN 
        (select * from schema_store_catalog_v where lower(warehouse) = lower('{tgt_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) =lower("{tgt_tbl}"))b
        ON lower(a.COL_NAME) = lower(b.COL_NAME)
        -- where lower(a.DATA_TYPE) <> lower(b.DATA_TYPE)
        order by b.COL_ORDER) x
        left outer join (select data_type lkp_data_type, warehouse, data_type data_type_aliases, has_precision, {src_wrhse}_compatibility_regex src_wrhse_compatibility_regex, {tgt_wrhse}_compatibility_regex tgt_wrhse_compatibility_regex from {constants.DATA_TYPE_COMPATIBILITY_MATRIX}
        where lower(warehouse) = lower('{tgt_wrhse}')) y
        on rlike(lower(x.tgt_data_type),lkp_data_type)
        --where src_wrhse_compatibility_regex is not null
        order by COL_ORDER
        ''').toPandas().style.applymap(color_survived, subset=['datatype_congruency','col_name_compare','data_type_compare','comment_compare']))


with tab3:
    st.header("Mismatches")
    #Mismatches
    if summary_df_filtered['quick_validation'].any():
        char_dt_count = spark.sql(f'''select count(DATA_TYPE) char_dt_count from schema_store_catalog_v where lower(warehouse) = lower('{src_wrhse}') and iteration_name ="{iteration_name}" and workflow_name = "{workflow_name}" and table_family = "{table_family}" and lower(TABLE_NAME) = lower("{src_tbl}") and WAREHOUSE = "hive" and lower(DATA_TYPE) like "char%" ''').collect()[0]["char_dt_count"]
        if char_dt_count !=0:
            st.error("CHAR data type found in the schema. Quick Validation ignores the trailing spaces in CHAR datatype across the entire dataset. If you would like to consider the trailing spaces differences please run a Detailed Validation instead.")

    addtnl_filter = st.selectbox(
    "Additional Filters for Mismatch Chart only",
    [addtnl_filter[0] for addtnl_filter in spark.sql(f"SELECT distinct addtnl_filter from {constants.MISMATCH_METRICS}").collect()],
)
    data_df = spark.sql(f"select * from {constants.MISMATCH_METRICS} where iteration_name = '{iteration_name}' and table_family ='{table_family}' and addtnl_filter ='{addtnl_filter}' order by mismatches desc").toPandas()
   
    # Define a color map for each category  
    color_map = {'mismatches': '#ff584f', 'total_matches': '#A8D1D1'}

    # Create the bar chart
    fig = px.bar(data_df, x='col_name', y=['mismatches','total_matches'], 
                color_discrete_map=color_map,log_y=True)

    st.plotly_chart(fig)

    with st.expander("More Details..."):
        st.dataframe(data_df)


    @st.fragment
    def mismatch_fragment():
        with st.container(border=True):
            def mismatch_table(s):
                color = '#D2C8D9' if s.mismatch_tbl == 'src_mismatches' else '#DFE6F2'
                return [f'background-color: {color}']*len(s)
            
            def mismatch_col(val):
                color = 'red'
                return f'color: {color}'
        
            columns_with_mismatch = spark.sql(f"select mismatch from (select distinct col_name as mismatch, mismatches from {constants.MISMATCH_METRICS} where mismatches >0 and iteration_name ='{iteration_name}' and table_family ='{table_family}' order by mismatches desc)x").toPandas()
            col1, col2 = st.columns(2)
            with col1:
                selected_name = st.selectbox("Columns with Mismatch:", columns_with_mismatch)
            with col2:
                limit = st.selectbox("Row limit:", ("100", "1000", "10000", "100000"), index=1)

            mismatch_data_tbl=spark.sql(f"select concat(validation_data_db,'.',workflow_name,'____',table_family,'__mismatch_data') as mismatch_data from {constants.VALIDATION_MAPPING_TABLE} where workflow_name = '{workflow_name}' and table_family ='{table_family}'").collect()[0]["mismatch_data"]
                     
            
            if(spark.catalog.tableExists(mismatch_data_tbl) and not columns_with_mismatch.empty):
                st.warning(f'Truncated Data limited to {limit} records', icon="⚠️")
                st.dataframe((spark.sql(f"select * from {mismatch_data_tbl} where iteration_name ='{iteration_name}' and col_name ='{selected_name}' limit {limit}")).toPandas().style.apply(mismatch_table, axis=1).set_properties(subset=[selected_name], **{'color': 'red'}))

    mismatch_fragment()
with tab4:
    st.header("Windowed Validation")
    windowed_df = spark.sql(f'''select * from {constants.WINDOWED_VALIDATION_METRICS} where iteration_name ="{iteration_name}" and workflow_name = '{workflow_name}' and lower(table_family) = lower("{table_family}") order by last_updated_date, table_type''').toPandas()

    # Define a color map for each category  
    color_map = {'src_mismatches': 'red', 'tgt_mismatches': 'orange', 'src': 'blue', 'tgt': 'yellow'}

    # Create the bar chart
    fig = px.bar(windowed_df, x='last_updated_date', y=['num_records'],
                color="table_type",
                barmode="group", 
                color_discrete_map=color_map)

    st.plotly_chart(fig)

    with st.expander("More Details..."):
        st.dataframe(windowed_df)

with tab5:
    st.header("Anomalies")

    # Check for both primary key anomalies and hash anomalies
    anomalies_data_tbl = spark.sql(f"select concat(validation_data_db,'.',workflow_name,'___',table_family,'__anomalies') as anomalies_data from {constants.VALIDATION_MAPPING_TABLE} where workflow_name = '{workflow_name}' and table_family ='{table_family}'").collect()[0]["anomalies_data"]
    hash_anomalies_data_tbl = spark.sql(f"select concat(validation_data_db,'.',workflow_name,'___',replace(table_family, '.', '_'),'__hash_anomalies') as hash_anomalies_data from {constants.VALIDATION_MAPPING_TABLE} where workflow_name = '{workflow_name}' and table_family ='{table_family}'").collect()[0]["hash_anomalies_data"]

    # Check which anomaly tables exist
    pk_anomalies_exist = spark.catalog.tableExists(anomalies_data_tbl)
    hash_anomalies_exist = spark.catalog.tableExists(hash_anomalies_data_tbl)

    if pk_anomalies_exist or hash_anomalies_exist:
        # Create tabs for different anomaly types
        if pk_anomalies_exist and hash_anomalies_exist:
            anomaly_tab1, anomaly_tab2 = st.tabs(["Primary Key Anomalies", "Hash Anomalies"])
        elif pk_anomalies_exist:
            anomaly_tab1 = st.container()
            anomaly_tab2 = None
        else:
            anomaly_tab1 = None
            anomaly_tab2 = st.container()

        # Primary Key Anomalies Tab
        if pk_anomalies_exist:
            with (anomaly_tab1 if pk_anomalies_exist and hash_anomalies_exist else st.container()):
                st.subheader("Primary Key Anomalies")
                anomalies = spark.sql(f"select distinct `type` as anomaly from {anomalies_data_tbl} where iteration_name ='{iteration_name}'").toPandas()
                
                if (not anomalies.empty):
                    @st.fragment
                    def pk_anomaly_fragment():
                        selected_anomaly = st.selectbox("Types of Primary Key Anomalies:", anomalies, key="pk_anomalies")
                        st.warning('Truncated Data limited to 10000 records', icon="⚠️")
                        st.dataframe(spark.sql(f"select * from {anomalies_data_tbl} where iteration_name ='{iteration_name}' and `type` = '{selected_anomaly}' limit 10000").toPandas())
                    pk_anomaly_fragment()
                else:
                    st.info("No primary key anomalies found for this iteration.")

        # Hash Anomalies Tab
        if hash_anomalies_exist:
            with (anomaly_tab2 if pk_anomalies_exist and hash_anomalies_exist else st.container()):
                st.subheader("Hash Anomalies")
                hash_anomalies = spark.sql(f"select distinct comparison_type as anomaly from {hash_anomalies_data_tbl} where iteration_name ='{iteration_name}'").toPandas()
                
                if (not hash_anomalies.empty):
                    @st.fragment
                    def hash_anomaly_fragment():
                        selected_hash_anomaly = st.selectbox("Types of Hash Anomalies:", hash_anomalies, key="hash_anomalies")
                        st.warning('Truncated Data limited to 10000 records', icon="⚠️")
                        st.dataframe(spark.sql(f"select * from {hash_anomalies_data_tbl} where iteration_name ='{iteration_name}' and comparison_type = '{selected_hash_anomaly}' limit 10000").toPandas())
                    hash_anomaly_fragment()
                else:
                    st.info("No hash anomalies found for this iteration.")
    else:
        st.info("No anomaly tables found for this table family.")

