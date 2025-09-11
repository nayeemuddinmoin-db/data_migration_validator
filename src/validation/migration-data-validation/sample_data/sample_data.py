# Databricks notebook source
# MAGIC %run "../conf/constants"

# COMMAND ----------

# DBTITLE 1,All match Example
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_all_match_src_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary INT)
''')
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_all_match_tgt_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary INT)
''')
spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_all_match_src_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')

spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_all_match_tgt_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')


# COMMAND ----------

# DBTITLE 1,Primary Key Violations Example
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_pk_violation_src_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary INT)
''')
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_pk_violation_extras_tgt_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary INT)
''')
spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_pk_violation_src_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')
spark.sql(f'''
Insert into table {VALIDATION_EXAMPLE_DB}.databricks_sample_pk_violation_src_tbl select * from {VALIDATION_EXAMPLE_DB}.databricks_sample_pk_violation_src_tbl limit 1
''')

spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_pk_violation_extras_tgt_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')



# COMMAND ----------

# DBTITLE 1,Schema and Data Mismatch Example
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_schema_data_mismatch_src_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  salary INT)
''')
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_schema_data_mismatch_tgt_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary DECIMAL(18,2))
''')
spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_schema_data_mismatch_src_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')
spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_schema_data_mismatch_tgt_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')

# COMMAND ----------

# DBTITLE 1,Data Mismatch Example
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_data_mismatch_src_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary INT)
''')
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_data_mismatch_tgt_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary INT)
''')
spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_data_mismatch_src_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')
spark.sql(f'''
Insert into table {VALIDATION_EXAMPLE_DB}.databricks_sample_data_mismatch_src_tbl VALUES (123456789, "Mahesh", NULL, "Pillai", "M", "1970-01-01", "1")
''')

spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_data_mismatch_tgt_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')

spark.sql(f'''
Insert into table {VALIDATION_EXAMPLE_DB}.databricks_sample_data_mismatch_tgt_tbl VALUES (123456789, "Mahesh", NULL, "M Pillai", "M", "1960-01-01", "1")
''')



# COMMAND ----------

# DBTITLE 1,Src and Tgt Extras Example
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_extras_src_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary INT)
''')
spark.sql(f'''
CREATE OR REPLACE TABLE {VALIDATION_EXAMPLE_DB}.databricks_sample_extras_tgt_tbl (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate DATE,
  salary INT)
''')
spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_extras_src_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')
spark.sql(f'''
Insert into table {VALIDATION_EXAMPLE_DB}.databricks_sample_extras_src_tbl VALUES (123456789, "Mahesh", NULL, "Pillai", "M", "1970-01-01", "1")
''')

spark.sql(f'''
Insert overwrite table {VALIDATION_EXAMPLE_DB}.databricks_sample_extras_tgt_tbl select id, firstName, middleName, lastName, gender, birthDate, salary from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
''')

spark.sql(f'''
Insert into table {VALIDATION_EXAMPLE_DB}.databricks_sample_extras_tgt_tbl VALUES (987654321, "Swetha", NULL, "Nandajan", "F", "1970-01-01", "1")
''')



# COMMAND ----------

import os
spark.read.option("header","true")\
    .option("multiLine", "true")\
    .option("escape","\"")\
    .option("inferSchema", "true")\
    .csv("file://"+os.path.abspath("../conf/validation_mapping_config.csv"))\
    .write.mode("overwrite").saveAsTable(f'''{VALIDATION_MAPPING_TABLE}''')

# COMMAND ----------

import os
spark.read.option("header","true")\
    .option("multiLine", "true")\
    .option("escape","\"")\
    .option("inferSchema", "true")\
    .csv("file://"+os.path.abspath("../conf/tables_config.csv"))\
    .write.mode("overwrite").saveAsTable(f'''{TABLE_CONFIG_TABLE}''')
