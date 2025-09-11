##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

# import pandas as pd
from databricks.connect.session import DatabricksSession
import conf.constants as constants



def retrieve_summary():
  spark = DatabricksSession.builder.clusterId(constants.CLUSTER_ID).getOrCreate()
  # spark = DatabricksSession.builder.serverless(True).getOrCreate()

  df = spark.sql(f'''select
    iteration_name,
    workflow_name,
    table_family,
    concat_ws(':',workflow_name, table_family) as workflow_table_family,
    duration,
    (
      IF(
        primary_key_compliance_status IS NULL OR primary_key_compliance_status == "FAILED",
        false,
        true
      )
      AND IF(
        ordinal_position_compliance_status IS NULL OR ordinal_position_compliance_status == "FAILED",
        false,
        true
      )
      AND IF(
        col_name_compare_status IS NULL OR col_name_compare_status == "FAILED",
        false,
        true
      )
      AND IF(
        data_type_compare_status IS NULL OR data_type_compare_status == "FAILED",
        false,
        true
      )
      AND IF(
        comment_compare_status IS NULL OR comment_compare_status == "FAILED",
        false,
        true
      )
      AND IF(
        data_type_compatibility_status IS NULL OR data_type_compatibility_status == "FAILED",
        false,
        true
      )
      AND IF(
        mismatches_after_exclusion_status IS NULL OR mismatches_after_exclusion_status == "FAILED",
        false,
        true
      )
      AND IF(
        extras_status IS NULL OR extras_status == "FAILED",
        false,
        true
      )
    ) final_validation_status,
    primary_key_compliance_status,
    ordinal_position_compliance_status,
    col_name_compare_status,
    data_type_compare_status,
    comment_compare_status,
    data_type_compatibility_status,
    mismatches_status,
    mismatches_after_exclusion_status,
    extras_status,
    src_warehouse,
    src_table,
    tgt_warehouse,
    tgt_table,
    col_mapping,
    tgt_primary_keys,
    --to prevent the pandas replace issue on an array
    array_join(exclude_fields_from_summary_array, ',') as exclude_fields_from_summary_array,
    date_bucket,
    mismatch_exclude_fields,
    src_sql_override,
    tgt_sql_override,
    src_data_load_filter,
    tgt_data_load_filter,
    validation_data_db,
    user_name,
    favorite,
    src_records,
    tgt_records,
    src_delta_tbl_bytes,
    tgt_delta_tbl_bytes,
    src_extras,
    tgt_extras,
    mismatches mismatches_before_exclusion,
    matches,
    quick_validation
    from
    (
      select
        iteration_name,
        workflow_name,
        table_family,
        exclude_fields_from_summary_array,
        duration,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "primary_key_compliance_status"
            )
          ),
          concat("BYPASS_", primary_key_compliance_status),
          primary_key_compliance_status
        ) primary_key_compliance_status,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "ordinal_position_compliance_status"
            )
          ),
          concat("BYPASS_", ordinal_position_compliance_status),
          ordinal_position_compliance_status
        ) ordinal_position_compliance_status,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "col_name_compare_status"
            )
          ),
          concat("BYPASS_", col_name_compare_status),
          col_name_compare_status
        ) col_name_compare_status,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "data_type_compare_status"
            )
          ),
          concat("BYPASS_", data_type_compare_status),
          data_type_compare_status
        ) data_type_compare_status,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "comment_compare_status"
            )
          ),
          concat("BYPASS_", comment_compare_status),
          comment_compare_status
        ) comment_compare_status,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "data_type_compatibility_status"
            )
          ),
          concat("BYPASS_", data_type_compatibility_status),
          data_type_compatibility_status
        ) data_type_compatibility_status,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "mismatches_status"
            )
          ),
          concat("BYPASS_", mismatches_status),
          mismatches_status
        ) mismatches_status,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "mismatches_after_exclusion_status"
            )
          ),
          concat("BYPASS_", mismatches_after_exclusion_status),
          mismatches_after_exclusion_status
        ) mismatches_after_exclusion_status,
        IF(
          (
            array_contains(
              exclude_fields_from_summary_array,
              "extras_status"
            )
          ),
          concat("BYPASS_", extras_status),
          extras_status
        ) extras_status,
        src_warehouse,
        src_table,
        tgt_warehouse,
        tgt_table,
        col_mapping,
        tgt_primary_keys,
        exclude_fields_from_summary_array,
        date_bucket,
        mismatch_exclude_fields,
        src_sql_override,
        tgt_sql_override,
        src_data_load_filter,
        tgt_data_load_filter,
        validation_data_db,
        user_name,
        favorite,
        src_records,
        tgt_records,
        src_delta_tbl_bytes,
        tgt_delta_tbl_bytes,
        src_extras,
        tgt_extras,
        mismatches,
        matches,
        quick_validation
      from(
          select
            a.iteration_name,
            a.workflow_name,
            a.table_family,
            split(d.exclude_fields_from_summary, ',') exclude_fields_from_summary_array,
            date_format(from_unixtime(TIMESTAMPDIFF(SECOND, c.validation_run_start_time, c.validation_run_end_time)), 'HH:mm:ss') as duration,
            primary_key_compliance_status,
            ordinal_position_compliance_status,
            col_name_compare_status,
            data_type_compare_status,
            comment_compare_status,
            data_type_compatibility_status,
            mismatches_status,
            mismatches_after_exclusion_status,
            extras_status,
            a.src_warehouse,
            a.src_table,
            a.tgt_warehouse,
            a.tgt_table,
            a.col_mapping,
            a.tgt_primary_keys,
            a.date_bucket,
            a.mismatch_exclude_fields,
            a.src_sql_override,
            a.tgt_sql_override,
            a.src_data_load_filter,
            a.tgt_data_load_filter,
            a.validation_data_db,
            a.streamlit_user_name as user_name,
            a.favorite,
            coalesce(a.hash_metrics.src_records, a.metrics.src_records) as src_records,
            coalesce(a.hash_metrics.tgt_records, a.metrics.tgt_records) as tgt_records,
            coalesce(a.hash_metrics.src_delta_table_size_bytes, a.metrics.src_delta_table_size_bytes) as src_delta_tbl_bytes,
            coalesce(a.hash_metrics.tgt_delta_table_size_bytes, a.metrics.tgt_delta_table_size_bytes) as tgt_delta_tbl_bytes,
            coalesce(a.hash_metrics.src_extras, a.metrics.src_extras) as src_extras,
            coalesce(a.hash_metrics.tgt_extras, a.metrics.tgt_extras) as tgt_extras,
            coalesce(a.hash_metrics.mismatches, a.metrics.mismatches) as mismatches,
            coalesce(a.hash_metrics.matches, a.metrics.matches) as matches,
            a.quick_validation
          from
            {constants.VALIDATION_SUMMARY_TABLE} a
            LEFT OUTER JOIN {constants.VALIDATION_LOG_TABLE}  c 
              on a.iteration_name = c.iteration_name
              and a.workflow_name = c.workflow_name
              and a.table_family = c.table_family
            LEFT OUTER JOIN {constants.VALIDATION_MAPPING_TABLE}  d
              on a.workflow_name = d.workflow_name
              and a.table_family = d.table_family
        ) x
    ) y
  order by
    iteration_name desc,
    table_family''').toPandas()
  # st.dataframe(df,width=10000)
  df['mismatches_status'].replace("SUCCESS", '✔️',inplace=True)
  df['mismatches_status'].replace("FAILED", '✖️',inplace=True)
  df.replace("SUCCESS", '✅',inplace=True)
  df.replace("FAILED", '❌',inplace=True)
  df['final_validation_status'].replace(True, 'SUCCESS',inplace=True)
  df['final_validation_status'].replace(False, 'FAILED',inplace=True)
  df.replace('BYPASS_SUCCESS', '✔️',inplace=True)
  df.replace('BYPASS_FAILED', '✖️',inplace=True)
  return df