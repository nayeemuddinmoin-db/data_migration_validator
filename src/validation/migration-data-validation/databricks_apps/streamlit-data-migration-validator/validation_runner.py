##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# import streamlit as st
# import pandas as pd

# st.set_page_config(layout="wide")

# st.header("Hello world!!!")
# apps = st.slider("Number of apps", max_value=60, value=10)
# chart_data = pd.DataFrame({'y':[2 ** x for x in range(apps)]})
# st.bar_chart(chart_data, height=500, width=min(100+50*apps, 1000), 
#              use_container_width=False, x_label="Apps", y_label="Fun with data")

#https://discuss.streamlit.io/t/drill-up-drill-down-options/8780/4

import importlib
import pandas as pd
import streamlit as st
import conf.constants as constants
# PySpark and databricks-connect related imports.
from databricks.connect.session import DatabricksSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
# import time
# import datetime
# from st_aggrid import AgGrid
# from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from datetime import datetime
# import json
from pyspark.sql.functions import from_unixtime
import requests
from pyspark.sql.functions import expr
# from pyspark.sql.functions import lit
from streamlit.web.server.websocket_headers import _get_websocket_headers
from streamlit_dynamic_filters import DynamicFilters
import os


# def _get_user_info():
#     headers = _get_websocket_headers()
#     return dict(
#         user_name=headers.get("X-Forwarded-Preferred-Username"),
#         user_email=headers.get("X-Forwarded-Email"),
#         user_id=headers.get("X-Forwarded-User"),
#         access_token=headers.get("X-Forwarded-Access-Token")
#     )

# user_info = _get_user_info()
# st.write(user_info)


st.error("This is the dev version. Demo version is available at [go/dmv](https://go/dmv)")


st.header("Validation Runner", divider=True)
tab1, tab2 = st.tabs(["Databricks Job Details", "Validation Job Run Status"])


with tab1:
    st.subheader("Databricks Job Details")

    st.caption("The following table shows the validation runs that have been triggered with their current status. Please select the workflow and table family to trigger a new validation run. Please note that the table families are not necessarily the same as the table names in the data lake.")

    # st.write(datetime.now(), "StartedGetOrCreateSparkSession")
    # spark = DatabricksSession.builder.clusterId(constants.CLUSTER_ID).getOrCreate()
    # st.write(datetime.now(), "CompletedGetOrCreateSparkSession")
    # spark = SparkSession.getActiveSession()
    # spark = DatabricksSession.builder.clusterId("dummy").validateSession(False).getOrCreate()
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

    dbutils = DBUtils(spark)

    job_id =constants.JOB_ID
    table_families = None
    iteration_name_suffix = None
    workflow_name = None

    def initialize_widgets():
        global workflow_name
        # st.write(datetime.now(), "getting the workflow_name")
        workflow_name = st.sidebar.multiselect(
        "Workflow Name",
        [workflow_name[0] for workflow_name in spark.sql(f"SELECT distinct workflow_name from {constants.VALIDATION_MAPPING_TABLE} where validation_is_active =true").collect()],
        )

        workflow_where_condition = "and workflow_name IN ("+ ', '.join(['"{}"'.format(value) for value in workflow_name])+")" if workflow_name else "and true"
        # st.write(workflow_where_condition)
        # st.write(datetime.now(), "got the workflow_name")


        # st.write(datetime.now(), "getting the table_families")
        with st.sidebar.form("my_form"):
            global table_families

            table_families = st.multiselect(
            "Workflow:TableFamily",
            [workflow_table_family[0] for workflow_table_family in spark.sql(f"SELECT distinct concat_ws(':',workflow_name, table_family) workflow_table_family from {constants.VALIDATION_MAPPING_TABLE} where validation_is_active {workflow_where_condition} ").collect()],)

            table_families = (
                ",".join(
                    [
                        f'"{table_family[0]}"'
                        for table_family in spark.sql(
                            f"select distinct concat_ws(':',workflow_name, table_family) workflow_table_family from {constants.VALIDATION_MAPPING_TABLE} where validation_is_active {workflow_where_condition}"
                        ).collect()
                    ]
                )
                if (not table_families)
                else ", ".join(['"{}"'.format(value) for value in table_families])
            )

            global iteration_name_suffix 
            iteration_name_suffix = st.text_input("Iteration Name Suffix")


            # Every form must have a submit button.
            submitted = st.form_submit_button("Trigger Validation Run")
            # st.write(workflow_name, iteration_name_suffix, table_families)

        return submitted, iteration_name_suffix

    def run_databricks_job():
        import requests

        databricks_instance = constants.DATABRICKS_INSTANCE
        # access_token = dbutils.secrets.get(
        #     scope=constants.ACCESS_TOKEN_SCOPE, key=constants.ACCESS_TOKEN_KEY)
        token_url = f"{databricks_instance}/oidc/v1/token"
        client_id = os.getenv('DATABRICKS_CLIENT_ID')
        client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
       
        # Requesting the token
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "all-apis"
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        response = requests.post(token_url, data=payload, headers=headers)
        access_token = response.json().get("access_token")
 

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # st.write("The current table family is", table_families)
        job = {
        "job_id" : job_id,
        "notebook_params": {
            "03-workflow:table_family" : str(table_families),
            "02-workflow_name" : str(workflow_name),
            "01-iteration_name_suffix" : iteration_name_suffix,
            "00-triggered_from_workflow" : True,
            "04-user_name" : st.context.headers["X-Forwarded-Preferred-Username"],
            "05-user_email" : st.context.headers["X-Forwarded-Email"],
        },
        

        }

        response = requests.post(f"{databricks_instance}/api/2.0/jobs/run-now", json=job, headers=headers)

        if response.status_code == 200:
            print(f"Job run triggered successfully for Job ID: {job_id}")
            print(response.json().get('run_id'))
            st.success(f"Job run triggered for job_id: {job_id} and run_id: {response.json().get('run_id')}")
        else:
            print(f"Failed to trigger job run for Job ID: {job_id}")
            st.exception(f"Failed to trigger job run for Job ID: {job_id}, {response.text}")
            print(response.text)

    # st.write(datetime.now(), "Initializing Widgets")
    submitted, iteration_name_suffix = initialize_widgets()
    # st.write(datetime.now(), "Initialized Widgets")
    if submitted:
        # st.write(datetime.now(), "Invoking run_databricks_job")
        run_databricks_job()
        # st.write(datetime.now(), "Completed run_databricks_job")


    databricks_instance = constants.DATABRICKS_INSTANCE
    # st.write(datetime.now(), "Getting Access Token")
    # access_token = dbutils.secrets.get(
    #         scope=constants.ACCESS_TOKEN_SCOPE, key=constants.ACCESS_TOKEN_KEY)
    token_url = f"{databricks_instance}/oidc/v1/token"
    client_id = os.getenv('DATABRICKS_CLIENT_ID')
    client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
    
    # Requesting the token
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "all-apis"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(token_url, data=payload, headers=headers)
    access_token = response.json().get("access_token")
 

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    job = {
    "job_id" : constants.JOB_ID
    }

    # st.write(datetime.now(), "Getting Spark Job Details")
    response = requests.get(f"{databricks_instance}/api/2.1/jobs/runs/list", json=job, headers=headers)
    # st.write(datetime.now(), "Receieved Spark Job Details")
    # st.write(response)
    def color_rule(s):
        return ['background-color: #A8D1D1']*len(s) if s.result_state == "SUCCESS" else (['background-color: #F8C57C']*len(s) if s.life_cycle_state == "RUNNING" else (['background-color: #D7D3BF']*len(s) if s.life_cycle_state == "QUEUED" else ['background-color: #ff584f']*len(s)))

    dictionary = response.json().get("runs")
    # st.write(dictionary)
    # print(dictionary)
    df = pd.DataFrame.from_dict(dictionary)
    # print(df)
    df_2 = spark.createDataFrame(df) 
    df_3 = df_2.select('job_id',
    'run_id',
    'creator_user_name',
    'state.life_cycle_state',
    'state.result_state',
    from_unixtime(df_2['start_time']/1000).alias("start_time_utc"),
    from_unixtime(df_2['end_time']/1000).alias("end_time_utc"),
    expr("""case when state.life_cycle_state IN ("TERMINATED","INTERNAL_ERROR") then NULL else False end""").alias("KILL"),
    'run_page_url',
    # 'overriding_parameters',
    'state.state_message',
    ).toPandas().style.apply(color_rule, axis=1)
    # st.write(datetime.now(), "Printing Spark Job Details")
    df_4 = st.data_editor(
    df_3,
    column_config={
        "job_id": st.column_config.TextColumn(),
        "run_id":st.column_config.TextColumn(),
        "run_page_url": st.column_config.LinkColumn(
            display_text="Job Run URL"
            ),
        "KILL": st.column_config.CheckboxColumn("Kill Job",
                                                help ="Select the entries to kill the RUNNING job"),
    },
    hide_index=True,
    disabled=("job_id", "run_id","creator_user_name","life_cycle_state","result_state","state_message","start_time_utc","end_time_utc","run_duration","run_page_url",
            #   "overriding_parameters"
            ),
    ).where(lambda df_4: df_4['life_cycle_state']!='TERMINATED')
    kill_id = df_4.loc[df_4['KILL']==True, 'run_id'].values

    if len(kill_id)>0:
        job = {
        "run_id" : f"{kill_id[0]}"
        }

        response = requests.post(f"{databricks_instance}/api/2.1/jobs/runs/cancel", json=job, headers=headers)

        if response.status_code == 200:
            print(f"Job Cancel triggered successfully for Run ID: {kill_id[0]}")
            print(response.json().get('run_id'))
            # st.write(datetime.now(), "Printing Kill Job Details")
            st.error(f"Job KILL triggered successfully for Run ID: {kill_id[0]}",icon="✅")

        else:
            print(f"Failed to trigger job run for Run ID: {kill_id[0]}")
            print(response.text)

    df_5=df_2.select('job_id',
    'run_id',
    'creator_user_name',
    'state.life_cycle_state',
    'state.result_state',
    from_unixtime(df_2['start_time']/1000).alias("start_time_utc"),
    from_unixtime(df_2['end_time']/1000).alias("end_time_utc"),
    expr("""case when state.life_cycle_state IN ("TERMINATED","INTERNAL_ERROR") then NULL else False end""").alias("KILL"),
    'run_page_url',
    # 'overriding_parameters',
    'state.state_message',
    ).toPandas()

    # st.divider()
    with st.expander("More Usage Details"):
        st.markdown('''
                **Workflow Name:** The name of the workflow\n
                **Table Family:** The name of the table family\n
                **Iteration Name Suffix:** The suffix that would be appended to the default iteration name which is the timestamp of the validation tool run\n
                To kill a running job, click on the checkbox correspoding to the job run under the **Kill** box\n
                ''')

with tab2:
    st.subheader("Validation Job Run Status")
    def color_rule_2(s):
        return ['background-color: #A8D1D1']*len(s) if s.validation_run_status == "SUMMARY_SUCCESS" else (['background-color: #F8C57C']*len(s) if s.validation_run_status == "STARTED" else (['background-color: #E1F0DA']*len(s) if s.validation_run_status == "RUN_SUCCESS" else (['background-color: #ff584f']*len(s) if s.validation_run_status == "FAILED" else ['background-color: #FFF8DE']*len(s))))

    df_log = spark.sql(f"""
        select
        iteration_name,
        workflow_name,
        table_family,
        validation_run_status,
        streamlit_user_name user_name, 
        validation_run_start_time,
        validation_run_end_time,
        src_warehouse,
        src_table,
        tgt_warehouse,
        tgt_table,
        exception
        from
        {constants.VALIDATION_LOG_TABLE}
        order by (iteration_name, validation_run_start_time, validation_run_end_time) desc""").toPandas()
    dynamic_filters_1 = DynamicFilters(df=df_log, filters=['user_name','iteration_name','workflow_name','table_family','src_warehouse','validation_run_status'],filters_name='filters_1')
    dynamic_filters_1.display_filters(location='columns',num_columns=3)

    data_df = dynamic_filters_1.filter_df().style.apply(color_rule_2, axis=1)
    st.dataframe(data_df)




 