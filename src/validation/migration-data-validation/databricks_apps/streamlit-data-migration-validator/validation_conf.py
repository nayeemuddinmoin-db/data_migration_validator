##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# import pandas as pd
import streamlit as st
import conf.constants as constants
# PySpark and databricks-connect related imports.
from databricks.connect.session import DatabricksSession
# from pyspark.sql.functions import col
# from pyspark.sql.types import StringType
# import pyspark.sql.functions as F
# from streamlit_dynamic_filters import DynamicFilters
import time

#https://levelup.gitconnected.com/dynamic-dataframe-filtering-in-streamlit-aeae5de0f92a

st.header("Validation Configuration", divider=True)

st.caption("The following features the Embedded Delta Table Editors enabling the editing of the underlying Delta tables housing these configurations.")

# spark = DatabricksSession.builder.clusterId(constants.CLUSTER_ID).getOrCreate()
spark = DatabricksSession.builder.serverless(True).getOrCreate()


tab1, tab2  = st.tabs(["Validation Mapping Table", "Table Config"])


with tab1:
  validation_mapping_sp_df = spark.sql(f"select * from {constants.VALIDATION_MAPPING_TABLE}")
  validation_mapping_schema = validation_mapping_sp_df.schema
  validation_mapping = validation_mapping_sp_df.toPandas()

  def edit_mapping():
    if st.button("Edit", key="mapping_table"):
      @st.fragment
      def submit_mapping():
        with st.form("test"):
          validation_mapping_edited = st.data_editor(validation_mapping,num_rows="dynamic", key ="mapping_table_edited")
          submitted = st.form_submit_button("Save the Changes")
          if submitted:
            spark_df = spark.createDataFrame(validation_mapping_edited,validation_mapping_schema)
            # st.write(spark_df.schema)
            spark_df.write.format("delta").mode("overwrite").saveAsTable(constants.VALIDATION_MAPPING_TABLE)
            st.info(f"Changes Saved to {constants.VALIDATION_MAPPING_TABLE}")
            st.toast(f"Changes Saved to {constants.VALIDATION_MAPPING_TABLE}")
            time.sleep(2)
            st.rerun()
      submit_mapping()
    else:
      st.data_editor(validation_mapping,num_rows="dynamic",disabled=True, key ="mapping_table_edited")
  edit_mapping()

with tab2:
  table_config_sp_df = spark.sql(f"select * from {constants.TABLE_CONFIG_TABLE}")
  table_config_schema = table_config_sp_df.schema
  table_config = table_config_sp_df.toPandas()

  def edit_mapping():
    if st.button("Edit", key="config_table"):
      @st.fragment
      def submit_mapping():
        with st.form("test"):
          table_config_edited = st.data_editor(table_config,num_rows="dynamic", key ="config_table_edited")
          submitted = st.form_submit_button("Save the Changes")
          if submitted:
            table_config_spark_df = spark.createDataFrame(table_config_edited,table_config_schema)
            # st.write(spark_df.schema)
            table_config_spark_df.write.format("delta").mode("overwrite").saveAsTable(constants.TABLE_CONFIG_TABLE)
            st.info(f"Changes Saved to {constants.TABLE_CONFIG_TABLE}")
            st.toast(f"Changes Saved to {constants.TABLE_CONFIG_TABLE}")
            time.sleep(2)
            st.rerun()
      submit_mapping()
    else:
      st.data_editor(table_config,num_rows="dynamic",disabled=True, key ="config_table_edited")
  edit_mapping()



