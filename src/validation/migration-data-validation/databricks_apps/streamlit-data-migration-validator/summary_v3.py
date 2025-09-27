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
# import plotly.express as px
# import plotly.graph_objects as go
import altair as alt
from validation_sqls import retrieve_summary

from streamlit_dynamic_filters import DynamicFilters
#https://levelup.gitconnected.com/dynamic-dataframe-filtering-in-streamlit-aeae5de0f92a


st.header("Validation Summary", divider=True)

# spark = DatabricksSession.builder.clusterId(constants.CLUSTER_ID).getOrCreate()
spark = DatabricksSession.builder.serverless(True).getOrCreate()



def color_survived(val):
    # color = '#ff392e' if val == 'FAILED' else ('#538c50' if val == 'SUCCESS' else '')
    color = '#ff584f' if val == 'FAILED' else ('#A8D1D1' if val == 'SUCCESS' else '')

    return f'background-color: {color}'

def color_survived_1(val):
    color = '#a9a9a9' if (val == '✔️' or val == '✖️') else ''
    return f'background-color: {color}'


# Function to convert bytes to a human-readable format
def human_readable_size(size_in_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

df = retrieve_summary()

df['src_delta_tbl_size'] = df.apply(lambda row: f"{human_readable_size(row['src_delta_tbl_bytes'])} ({row['src_delta_tbl_bytes']} bytes)", axis=1)
df['tgt_delta_tbl_size'] = df.apply(lambda row: f"{human_readable_size(row['tgt_delta_tbl_bytes'])} ({row['tgt_delta_tbl_bytes']} bytes)", axis=1)

# df = retrieve_summary()

dynamic_filters = DynamicFilters(df=df, filters=['user_name','iteration_name','workflow_name','table_family','src_warehouse','favorite'],filters_name='filters_2')
dynamic_filters.display_filters(location='sidebar')



data_df = dynamic_filters.filter_df()[["iteration_name", "workflow_name", "table_family", "duration", "src_delta_tbl_bytes","tgt_delta_tbl_bytes","final_validation_status","primary_key_compliance_status", "ordinal_position_compliance_status", "col_name_compare_status", "data_type_compare_status", "comment_compare_status", "data_type_compatibility_status", "mismatches_status", "mismatches_after_exclusion_status","extras_status"]]

with st.expander("Validation Summary", expanded=True):
    selected_rows = st.dataframe(data_df.style.applymap(color_survived, subset=['final_validation_status']).applymap(color_survived_1, subset=['primary_key_compliance_status','ordinal_position_compliance_status','col_name_compare_status','data_type_compare_status','comment_compare_status','data_type_compatibility_status','mismatches_status','mismatches_after_exclusion_status',"extras_status"]),width=10000,
                #  on_select="rerun",selection_mode="single-row",key="df_key",
                )


#Heatmap Details
st.header("Validation Iteration Summary", divider=True)

selection = alt.selection_single(fields=['iteration_name', 'workflow_name', 'table_family'], empty='all')

chart = (
    alt.Chart(dynamic_filters.filter_df())
    .mark_bar()
    .encode(
        alt.X("iteration_name:O"),
        alt.Y("workflow_table_family"),
        # alt.Tooltip(["iteration_name", "table_family","final_validation_status"]),
        alt.Tooltip(['user_name','iteration_name','workflow_name','table_family','src_warehouse','src_table','tgt_warehouse','tgt_table','exclude_fields_from_summary_array', 'src_delta_tbl_size', 'tgt_delta_tbl_size', 'duration','primary_key_compliance_status','ordinal_position_compliance_status','col_name_compare_status','data_type_compare_status','comment_compare_status','data_type_compatibility_status','mismatches_status','mismatches_after_exclusion_status',"extras_status",'favorite']),
        color=alt.condition(
        alt.datum.final_validation_status == 'SUCCESS',  # Condition for the specific value
        # alt.value('#538c50'),          # Color for the specific value
        # alt.value('#ff392e')     # Color for other values
        alt.value('#A8D1D1'),          # Color for the specific value
        alt.value('#ff584f')     # Color for other values
    ),
    ).add_params(
    selection  # This line adds the parameter to the chart
)
    .interactive()
)


chart = chart.configure_axis(
    # labelFontSize=14,
    # titleFontSize=16,
    # labelAngle=45  # Rotates the labels by 45 degrees
    labelLimit=1000,
    # labelPadding=100
)

selected_data = st.altair_chart(chart,use_container_width=False,on_select="rerun",selection_mode=None,key="df_point")
# st.write(selected_data)

if selected_data.selection.param_1:
    @st.dialog("Summary",width="large")
    def on_selection():
        iteration_name = selected_data.selection.param_1[0]["iteration_name"]
        workflow_name = selected_data.selection.param_1[0]["workflow_name"]
        table_family = selected_data.selection.param_1[0]["table_family"]
        df_new = df[['user_name','iteration_name','workflow_name','table_family','src_warehouse','src_table','tgt_warehouse','tgt_table','exclude_fields_from_summary_array','duration','favorite']]
        df_new_filtered = df_new.loc[(df_new['iteration_name']==iteration_name) & (df_new['workflow_name']==workflow_name) & (df_new['table_family']==table_family)]

        summary_df_transpose = df_new_filtered.T
        summary_df_transpose.columns = ['Values']
        st.dataframe(summary_df_transpose, width=50000)
        st.page_link("validation_v3.py", label="More Details...", icon="📊")

        st.session_state['iteration_name']=iteration_name 
        st.session_state['workflow_name']=workflow_name
        st.session_state['table_family']=table_family
    on_selection()



if (spark.sql(f"select count(*) rec_count from {constants.VALIDATION_SUMMARY_TABLE}" ).collect()[0]["rec_count"] == 0):
    st.warning("Kindly initiate the first validation iteration from the Runner page. Upon successful completion, you will be able to access the summary details in this page.")
    st.stop()

