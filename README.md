# data-migration-validator

> **This doc is a WIP 

This tool helps to validate the tables between Databricks and the following warehouses:
- Databricks
- Netezza
- Snowflake
> ***The above warehouses are the ones currently supported and tested. It is possible to plugin additional warehouses as needed***

## Key Features

* _**Simple setup and usage**_ as the core logic resides in a Databricks notebook and the dasboarding in a Jupyter notebook.
* Supports **Netezza**, **Snowflake** and **Databricks** as Sources.
* Schema (DataType) Validation includes **compatibility** between the source and target warehouses.
* Data Validation returns **all the data** with mismatches or anomalies as the data resides in delta tables.
* Supports **columns mapping** in case the source and target have different column names.
* Supports multiple **primary/ join** keys.
* Supports **excluding** fields from mismatch report (windowed validation) to reduce the noise.
* Supports **source and target filters** to reduce the data being pulled to the databricks environment for validation
* Supports the usage of different databricks databases to store **sensitive** content.
* Supports the **retention** of intermediate data produced as part of the validation.
* The validation can be run either for a collection of tables (**workflow**) or **individually** for each table family from the notebooks leveraging the databricks widgets.
* Supports **parallel validation**, the limit decided by the number of cores that you decide for the driver. So can run multiple validations in parallel
* Processing runs **entirely on Databricks** so can scale up the resources per your cluster configuration.
* Stores all the **historical validation iterations** so that you can come back in time and go through a previous validation iteration.
* Configutaion made simple by the use of **csv files** which can be edited by a spredsheet software like excel.
* Primary Key violations report helps to identify if there are **duplicates** in the source or target data.
* Reports show if the columns in both the source and target are not following the same order/ **ordinal** positions.
* Datatype compatibility can be **customized** by updating the underlying table that holds the regex rules.
* Datatype compatibily also looks at the **scale and precision** of the correspoding warehouse systems.
* Another **warehouse can be plugged** in and is a one time setup and thus the tool can benefit other users needing this warehouse support.
* The **sankey report** gives a birds eye view of the fields and the datatypes and their flow between the source and target warehouses.
* The windowed validation report helps to visualizes the mismatches and other anomalies in a **timeline** helping you to detect and reduce the noise from the mismatches
* The validation report supports applying **filters** at the mismatched data and storing the results in the backend
* The detailed validation report shows the mimatches and the rows horizontally making it easier for a **visual comparison**

  
## Architecture

![image](https://github.com/databricks/data-migration-validator/assets/35385680/5e78049f-486e-4036-81b8-4f65e6fc90a9)

## Usage
### Copy this repo to the Databricks Workspace

This repo can be copied to the Databricks repo in either of the following 2 ways:
* Clone the repo to the Databricks workspace ([git-operations-with-repos.html](https://docs.databricks.com/en/repos/git-operations-with-repos.html))
* Download this repo and make it upload it to the Databricks workspace
  
  <img width="1526" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/edb0e1fb-9e00-4c72-bff1-38975dbf1209">

### Create a Compute Cluster
Create an all-purpose-cluster that would suit the workload. A example is shown below.
<img width="1525" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/94a477fe-0f4d-4ae6-9499-4eec2c209fec">


### Configuration

#### Choose a catalog and schema
Choose or create a catalog and schema where the configs and the metrics table ca reside. It is advised to use an empty schema, as it would help in the isolation, monitoring and cleanup of the validation related tables \
In the examples here we have used the catalog `mmp` and schema `mpillai`

  <img width="487" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/ce998343-810e-4f29-a48c-1ba26b7f7a62">


#### Update the validation metrics schema name
File: `data-migration-validator/validation/migration-data-validation/conf/constants` \
Param:  `VALIDATION_METRICS_DB` \
Update this param with the `catalog.schema` name where the validation metrics and config tables should reside. In this case we have used the name `mmp.mpillai`


  <img width="334" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/b688aafc-71d2-4235-98f8-b70d08d090ca">

#### Run the DDLs to create all the config and metrics tables required for the validation tool
File: `/data-migration-validator/validation/migration-data-validation/ddls/DDLs` \
Run the above notebook after connecting to the cluster was created earlier
<img width="1515" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/d8eace7c-f435-4178-8739-1c3cdea02850">
This would create the following 2 config tables
<img width="910" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/32a6a9d3-5a1b-4a41-af0f-c4772d0554d9">

#### Load the template configs
File: `/data-migration-validator/validation/migration-data-validation/sample_data/sample_data` \
This notebook loads the template configs for you to start with. Additionally it also creates sample tables for example validation.\
Run the above notebook after connecting to the cluster was created earlier
<img width="1508" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/f8c532c4-11da-4654-91d9-f4bbb9d0a5b3">

#### Optional - Sample Run
File: `/data-migration-validator/validation/migration-data-validation/sample_data/sample_data` \

If using this validation tool for the first time, it is encouraged to try out the sample validation and reviewing the dashboard to get familiarised with the process.
The previous step had alreadt loaded the necessary configs for the same.

File: `data-migration-validator/validation/migration-data-validation/migration-data-validation` \
<img width="1497" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/21511391-e5d4-4967-9940-9e2c810abd78">

Note: Being the very first run, the widgets may not be visible. After the first run the widgets would pop-up 

<img width="1520" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/f15d8e2d-fb29-4edc-b101-61c654a216e0">

The following are the tables that get created and loaded as part of this process

<img width="913" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/fcfbcaf9-b81e-47cf-ae88-498d9462cd1b">

#### Optional - Dashboarding
File: `data-migration-validator/validation/migration-data-validation/dashboards/dashboarding` \

<img width="1500" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/b82f665c-35e7-4a5f-ad07-2f9db865ee06">

Choose the right Iteration Name from the drop down

The above .ipynb file displays the validation report. This can be run either in 3 different views
  * Standard View
    <img width="1500" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/9886ebc9-55f5-4303-946d-a03c0945ae3b">
  * Dashboard View (Preferred one) \
    <img width="367" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/c78fd4d7-1620-455b-a55a-948ba1e39b42">
    <img width="1525" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/8815bdac-13f1-4ce6-80d0-3ca0758cce13">
  * Dashboard Presentation Mode \
    <img width="243" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/1dbdcc7f-e7b0-42ab-99cb-a72e24a1b5fa">
    <img width="1724" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/e0bc895e-7b04-4d6c-b7a3-39e3022c8ae9">


#### Config Tables
There are primarily 3 config tables. The first 2 tables needs to be configured for each validation. The 3rd one usually does not require any configuration.
These tables can be exported as csv files and edited in a spreadsheet softare like Microsoft Excel, making it very user friendly.

1. tables_config - Contains the warehouse connection details for each of the source tables used in the data validation
2. validation_mapping - Contains the source and target details of the tables used for validation
3. db_data_type_compatibility_matrix - Contains the metadata (datatype) comparison rules for different warehouses. This is a tool specific configuration and only need to be modified, in case a new warehouse is being added or an existing rule needs to be modified
   

#### tables_config

**warehouse** - The type of data warehouses. Currently supported ones are netezza, snowflake and databricks \
**table_name** - The fully qualified table name in the format as supported by the corresponding warehouse. For example:  netezza -> mpillai.ADMIN.TRAINING_FOOTBALL, snowflake -> mpillai_dev.public.MISC_TASK \
**jdbc_options** - The JDBC connection options required for each warehouse. 
  For **Netezza** it woule be would be
  ```json
  {
   "hostname":"999.99.99.999:5480",
   "user":{
      "secret_scope":"netezza_keys",
      "key":"username"
   },
   "password":{
      "secret_scope":"netezza_keys",
      "key":"password"
   }
}
  ```
  where: \
  hostname - hostname:port for the JDBC connection \
  secret_scope - corresponds to the databricks `secret scope name` that stores the username or password for the jdbc connection \
  key - corresponds to the databricks `secret name` that stores the username or password for the jdbc connection \

  For **Snowflake** it woule be would be
  ```json
{
   "user":{
      "secret_scope":"SNOWFLAKE_NP",
      "key":"username"
   },
   "private_key":{
      "secret_scope":"SNOWFLAKE_NP",
      "key":"pkey"
   },
   "sfRole":"role_databricks_nonprd",
   "url":"db.us-central1.gcp.snowflakecomputing.com",
   "warehouse":"WH"
}
  ```
  where: \
  secret_scope - corresponds to the databricks `secret scope name` that stores the username or private_key for the snowflake connection \
  key - corresponds to the databricks `secret name` that stores the username or private_key for the snowflake connection \
  sfRole - the default security role to use for the session after connecting \
  url - the hostname for your snowflake account in the format `<account_identifier>.snowflakecomputing.com` \
  warehouse - The default virtual warehouse to use for the session after connecting.


#### validation_mapping

**entry_id** - sequential number denoting the id for the entry \
**workflow_name** - arbitrary name given to the workflow the correspoding tables this entry belongs to. This could be something like the application name, workflow name, etc. which would help to group a set of tables and do a bulk validation against them \
**table_family** - arbitrary name given to the entry coreesponding to the table name. This could be same as the source/ target table name or anything else that helps to identify the corresponding table in the validation report \
**src_warehouse** - the warehouse that the source table belongs to.viz, databricks, snowfalke, netezza \
**src_table** - the fully qualified source table name. The following table shows example patterns
| Warehouse  | Format                                                                   |
| ---------- | ------------------------------------------------------------------------ |
| databricks | mmp.mpillai.databricks_sample_src_tbl                                    |
| netezza    | mpillai.ADMIN.TRAINING_FOOTBALL                                          |
| snowflake  | mpillai_dev.public.MISC_TASK                                             | 

**tgt_warehouse** - the warehouse that the target table belongs to.viz, databricks, snowfalke, netezza \
**tgt_table** - the fully qualified source table name. The following table shows example patterns
| Warehouse  | Format                                                                   |
| ---------- | ------------------------------------------------------------------------ |
| databricks | mmp.mpillai.databricks_sample_src_tbl                                    |
| netezza    | mpillai.ADMIN.TRAINING_FOOTBALL                                          |
| snowflake  | mpillai_dev.public.MISC_TASK                                             |                               | 


**col_mapping** - The mapping between source and target column names in case they are different. If they are same the vale should be `{}`. In case they are different it should follow the format
```
{ "src_col_1": "tgt_col_1", "src_col_2": "tgt_col_2" }
```
where : \
src_col - the source column name \
tgt_col - the corresponding target column name

**tgt_primary_keys** - the primary keys or joining keys that would help in the joining of the target and source tables for validation. These should be `|` separated in case of multiple keys. Ex: `LOCATION_ID|MISC_TASK_ID` \
**date_bucket** - There is a chart that shows the mismatches and other anomalies on a timeline. This report is called `windowed validation report` . The `date-bucket` would be used as the x-axis here. date_bucket column is a date column referring to the timestamp generated in the upstream src system; most desirable are the last update fields viz the last_update_timestamp , `last_modified_tstmp` . If that’s not available, you can use the `create_tstmp` or such. If there are no such dates, just leave it blank. Do not give a non-existent column name. If this entry is empty, we would only miss that windowed validation chart. \
**mismatch_exclude_fields** - for the `windowed validation report` chart to reduced the noise of mismatched record from the timeline chart, we can excludes specific fields from the report. To specify such fields for which mismatch is expected, provide those separated by `|` . Ex: `UPDATE_TSTMP|LOAD_TSTMP` \
**filter** - this is for the `windowed validation report`. This filter would be applied on the data after the data is pulled from the source/ target tables. This helps in getting the validation reports for each of the provided filter.
```
(LOCATION_ID = US OR LOCATION_ID = CA)
```
The above example shows the filter name in the sql format \
**addtnl_filters** - this filter is for all the validation report (except the windowed_validation). The following is the format
```json
[
   {
      "filter_name":"N/A",
      "filter":"N/A",
      "capture_mismatches":false
   },
   {
      "filter_name":"LOCATION_ID",
      "filter":"LOCATION_ID_tgt = US OR LOCATION_ID_tgt = CA",
      "capture_mismatches":false
   },
   {
      "filter_name":"LOCATION_ID and MISC_LAST_UPDATED_TSTMP",
      "filter":"(LOCATION_ID_tgt = US OR LOCATION_ID_tgt = CA) and (MISC_MOD_TSTMP_src<=(select max(MISC_MOD_TSTMP_tgt) from {full_outer_table}))",
      "capture_mismatches":true
   }
]
```
where:
filter_name - arbitrary name for the filter to identify in the dashboard drop-down
filter - the sql where condition that would depict the desired filter. Ex: `(LOCATION_ID_tgt = US OR LOCATION_ID_tgt = CA) and (MISC_MOD_TSTMP_src<=(select max(MISC_MOD_TSTMP_tgt) from {full_outer_table}))`. \
Note that the column names should have suffixes `_src` and `-tgt` to distingusih between them. In case we need to refer to the internal table that is being used, refer it by `{full_outer_table}` \
In case ther are no additional filters required just put the following entry
```json
[
   {
      "filter_name":"N/A",
      "filter":"N/A",
      "capture_mismatches":true
   }
]
```
**src_data_load_filter** - these filters are applied before pulling the data from the `source` table into databricks for validation. This is to reduce the data being passed through the n/w and compared to boost the  performance. \
**tgt_data_load_filter** - these filters are applied before pulling the data from the `target` table into databricks for validation. This is to reduce the data being passed through the n/w and compared to boost the  performance.
**validation_data_db** - this is the validation database/ schema nam in databricks where the validation reports corresponding to the respective src/ tgt tables need to be stored. In case there are sensitive tables, the validation results of these could be in a schema created for sesitive tables. For instance, there could be a databricks schema `mmp.mpillai_normal` for non-sensitive tables and `mmp.mpillai_sensitive` for the sensitive tables. The assumption is the access to the `mmp.mpillai_sensitive` is restricted to specif user/ groups. \
**retain_tables_list** - by default, temporary tables created as part of the validation process like the the source table copy, the target table copy, the full outer join table of source and targets are deleted after the completion of the data validation job run. But in case any of these needs to be retained, for instace like debugging, the following entries can be provided `src_tbl, tgt_tbl, full_outer` \
**validation_is_active** - to disable this entry from being considered for validation, provide `false`


## Integration with new DataWareHouses

The code tool is made generic to add a new warehouse besides netezza, snowflake, databricks.
Let's assume Hive as an example. For this:
* create a new folder `hive` under `data-migration-validator/validation/migration-data-validation/integrations/`
* follow the template of the other warehouses
* There should be 3 methods
  * readHive - this method basically reads using the jdbc or other mechanism from the warehouse leveraging the settings from the `table_configs` table
  * captureHiveSchema - this method reads the metadata from the warehouse, if possible by issuing a `desc` statement through the `readHive` method
  * captureHiveTable - this method reads the data from the warehouse, if possible by issuing a `desc` statement through the `readHive` method
* Update the `captureSchema` method in the `data-migration-validator/validation/migration-data-validation/migration-data-validation` notebook to include the `captureHiveSchema` method
* Update the `captureTable` method in the `data-migration-validator/validation/migration-data-validation/migration-data-validation` notebook to include the `captureHiveTable` method
* Update the `/Repos/mahesh.pillai@databricks.com/data-migration-validator/validation/migration-data-validation/conf/data_type_compatibility_matrix.csv` by opening up in a spreadsheet to add an additional column for the new warehouse, which would have the regex rules that would be used by the Data type compatibility report (sql) in `data-migration-validator/validation/migration-data-validation/dashboards/dashboarding` 














## Sample Reports

### Summary

<img width="477" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/a4aadbf7-91d6-42f2-8250-72c96d22322d">

### Primary Key Violations

<img width="1048" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/32ee28a9-00a3-4118-81b6-90056bbbbb3b">

## Schema Validation

### Ordinal position Validation

<img width="233" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/640f02c1-7051-40f9-917e-966e13e498c3">

### Columns Comment Comparison

<img width="224" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/522ab7c4-afdd-4456-973f-0444a13848ba">

### Datatype Comparison

<img width="303" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/5fe8fa8f-eab8-417f-8e3e-8c035548b658">

### Datatype Compatibility

<img width="790" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/4b42cc87-a951-4593-acd1-04b7a7d82ad0">

### Schema Validation Summary

<img width="1580" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/80d73635-e161-4a8f-b629-c3739978aeaf">

### Schema Sankey

<img width="1582" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/8001d412-268e-489f-a408-93b8a2423fea">

### Windowed Validation

<img width="1583" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/0d1f56e6-f64b-4616-8594-ac2afe2fb612">

### Column Mismatch Status per Table across filters applied

<img width="789" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/f298ed91-fb41-4d26-8535-4a94260354d6">


### Mismatches between ovelapping records - Unfiltered

<img width="1582" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/5fb77aef-a517-4a54-aa8a-605fad331763">

### Mismatches between overlapping records (Can be filtered using the dashboard filter)

<img width="1579" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/b1a78dde-8475-495b-baec-c4156212a346">


### Mismatched Records between Source and Target tables, at the Column Level

<img width="1578" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/ffa9f265-3b76-487b-8664-226618f7277a">

### Anomalies Metrics

<img width="783" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/5feaba71-f5cd-4349-92f5-d464b3e40701">

### Anomalies Summary

<img width="1428" alt="image" src="https://github.com/databricks/data-migration-validator/assets/35385680/801b5d28-db4d-4201-88e5-7983e39eafb3">










