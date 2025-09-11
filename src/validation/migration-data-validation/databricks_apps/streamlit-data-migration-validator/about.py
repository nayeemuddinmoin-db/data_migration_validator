##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

import streamlit as st

tab1, tab2, tab3, tab4, tab5 = st.tabs(["TL;DR", "Supported Source Systems", "Architecture", "Technical Doc", "Author"])


with tab1:
    st.markdown("""
    # TL;DR

    This interactive validation tool, built entirely on the **Databricks** stack, provides a comprehensive solution for large-scale data migrations. It generates detailed reports at multiple levels—high-level summaries, itemized breakdowns, and data-level insights—by seamlessly comparing both data and metadata between source systems such as Netezza, Snowflake, MSSQL, Oracle, Teradata, Hive (EMR) and Databricks. The tool enables quick identification of discrepancies, ensures data integrity, and enhances migration efficiency with real-time, actionable reporting, all while leveraging Databricks' powerful processing capabilities for scalability and performance.

    ## Issue:
    During large-scale data migrations to Databricks, field teams rely on opensource or custom tools for validation. While effective for smaller migrations, these tools lack scalability and efficiency for large migrations as the team would need to develop a solution from scratch. They do not provide an integrated solution for managing thousands of tables, spotting and triaging discrepancies, or offering actionable reporting. A more streamlined, user-friendly approach is needed to reduce manual intervention and improve reporting accuracy.

    ## Scope:
    In Scope: Validation during large-scale migrations (involving thousands of tables) to Databricks or similar platforms, including UI, reporting, discrepancy analysis, and configuration.

    ## Impact:
    The manual validation process usually poses the following challenges:

    **Delays in Migration**: Manual validation leads to significant time delays.                
    **Increased Errors**: Lack of detailed, actionable reports increases the risk of missing data issues.                
    **Re-work**: Validation delays at later stages cause unnecessary re-conversion of code, reducing confidence in the migration tool.                
    **Limited Data Integrity Confidence**: Absence of robust validation affects trust in the migration’s accuracy and completeness.                
    **Triaging Issues**: Identifying and resolving discrepancies becomes cumbersome and inefficient.                
    **Customer Satisfaction**: Delayed validation results in slower sign-off, potentially frustrating customers.

    ## Who It Impacts:
    * **Field Teams**: Data engineers and QA personnel affected by inefficiencies and manual tasks.                
    * **Project Managers**: Face difficulties in meeting deadlines due to slow and error-prone validation.                
    * **Customers/Clients**: Affected by delays and concerns over data quality.                
    * **Executives & Stakeholders**: Delayed reporting affects decision-making and timelines.                
    * **IT/Support Teams**: Increased burden due to manual issue triage and troubleshooting.
    ## Key features:

    * **Unified User Interface**: A single platform that integrates all validation tasks, reducing the need for multiple tools.                
    * **Comprehensive Reporting/Dashboard**: Real-time, visually intuitive reports to track migration health and progress.                
    * **Detailed Discrepancy Analysis**: Actionable insights into discrepancies, enabling quick resolution.                
    * **Automated Triage**: Mechanisms to prioritize and resolve issues, reducing manual troubleshooting.                
    * **Scalability**: Easy configuration of validation tasks across thousands of tables, ensuring consistency.                
    ## Benefits:
    * **Faster Migration**: Streamlining validation reduces delays, accelerating project timelines and customer sign-off.
    * **Improved Data Quality**: Better tools for discrepancy analysis enhance data accuracy and completeness.
    * **Higher Customer Satisfaction**: Faster and more reliable validation builds trust and improves customer experience.
    * **Enhanced Reporting**: Real-time dashboards improve transparency and decision-making for executives and stakeholders.
    * **Reduced Operational Load**: Automating validation processes reduces manual effort for field teams and IT support.


    ## Architecture HighlightsApproach to Address the Problem/Opportunity Statement
    The current solution leverage Databricks Apps is designed to streamline and scale the data validation process for large migrations. This solution will leverage Databricks' ecosystem for processing, ensuring high performance, scalability, and efficiency.

    ## Key Features & Deliverables:

    * Centralized User Interface (UI):
    * The Databricks App will offer an intuitive UI for both technical and non-technical users, with a dashboard providing high-level insights into migration health, data completeness, discrepancies, and validation results.
    * Backend on Databricks:
    * Data validation and comparison will use Spark-based processing on Databricks, ensuring scalability, real-time processing, and parallel validation of large datasets.
    * Reporting & Analytics:
    * High-Level Summary: Migration overview with validation criteria breakdown.
    * Detailed Discrepancy Reports: Visualizations pinpointing exact discrepancies (tables, columns, rows) for faster resolution.
    * Data Type Compatibility Matrix: Ensures compatibility between source and target systems, highlighting potential data type conversion issues. Customizable via a config table.
    * Scalability & Configuration:
    * Supports multi-source comparisons (Snowflake, Databricks, Netezza, MSSQL, EMR Hive).
    * Simplified configuration for thousands of tables with minimal manual intervention.
    * Embedded Delta Table Editor: Facilitates interactive updates on Delta tables directly from the UI, reducing manual configuration cycles.


    ## Key Differentiators:

    * **Unified Web UI**: A single, integrated interface for the entire validation process, improving the user experience compared to current tools.
    * **Comprehensive Reporting**: Actionable, high-level summaries and detailed reports for quicker troubleshooting.
    * **Data Type Compatibility Matrix**: Ensures schema compatibility between source and target systems.
    * **Delta Table Editor**: Facilitates direct updates to backend Delta tables from the UI, streamlining configuration.
    * **Scalability**: Easily handles large datasets and thousands of tables, addressing the limitations of current solutions.
                
    Key Features
    ------------

    *   _**Simple setup and usage**_ as the core logic resides in Databricks notebooks     
    *   The package comes with **sample datasets** and their validation reports depicting different scenarios making it suitable for demos out of the box.
    *   Supports **Netezza**, **Snowflake, MSSql, Oracle, Teradata, Hive** and **Databricks** as Sources.
    *   Schema (DataType) Validation includes **compatibility** between the source and target warehouses.
    *   Data Validation returns **all the data** with mismatches or anomalies as the data resides in delta tables.
    *   Supports **columns mapping** in case the source and target have different column names.
    *   Supports multiple **primary/ join** keys.
    *   Supports **excluding** fields from mismatch report (windowed validation) to reduce the noise.
    *   Supports **source and target filters** to reduce the data being pulled to the Databricks environment for validation
    *   Supports the usage of different Databricks databases to store **sensitive** content.
    *   Supports the **retention** of intermediate data produced as part of the validation.
    *   The validation can be run either for a collection of tables (**workflow**) or **individually** for each table family from the notebooks leveraging the Databricks widgets.
    *   Supports **parallel validation**, the limit decided by the number of cores that you decide for the driver. So can run multiple validations in parallel
    *   Processing runs **entirely on Databricks** so can scale up the resources per your cluster configuration.
    *   Stores all the **historical validation iterations** so that you can come back in time and go through a previous validation iteration.
    *   Configuration made simple by the use of **Embedded Delta Table Editor in the UI** which can be edited realtime.
    *   Primary Key violations report helps to identify if there are **duplicates** in the source or target data.
    *   Reports show if the columns in both the source and target are not following the same order/ **ordinal** positions.
    *   Datatype compatibility can be **customized** by updating the underlying table that holds the regex rules.
    *   Datatype compatibility also looks at the **scale and precision** of the corresponding warehouse systems.
    *   Another **warehouse can be plugged** in and is a one time setup and thus the tool can benefit other users needing this warehouse support.
    *   The windowed validation report helps to visualizes the mismatches and other anomalies in a **timeline** helping you to detect and reduce the noise from the mismatches
    *   The validation report supports applying **filters** at the mismatched data and storing the results in the backend
    *   The detailed validation report shows the mismatches and the rows horizontally making it easier for a **visual comparison**
        

    Validation Configuration
    ------------------------

    ### Config Tables

    There are primarily 3 config tables. The first 2 tables needs to be configured for each validation. The 3rd one usually does not require any configuration. These tables can be updated realtime from the UI or exported as csv files and edited in a spreadsheet software like Microsoft Excel, making it very user friendly.

    1.  **tables\_config** - Contains the warehouse connection details for each of the source tables used in the data validation
        
    2.  **validation\_mapping** - Contains the source and target details of the tables used for validation
        
    3.  **db\_data\_type\_compatibility\_matrix** - Contains the metadata (datatype) comparison rules for different warehouses. This is a tool specific configuration and only need to be modified, in case a new warehouse is being added or an existing rule needs to be modified
        

    #### tables\_config

    *   **warehouse** - The type of data warehouses. Currently supported ones are Netezza, Snowflake and Databricks 
        
    *   **table\_name** - The fully qualified table name in the format as supported by the corresponding warehouse. For example: Netezza -> mpillai.ADMIN.TRAINING\_FOOTBALL, Snowflake -> mpillai\_dev.public.MISC\_TASK 
        
    *   **jdbc\_options** - The JDBC connection options required for each warehouse. For **Netezza** it woule be would be
        

    { "hostname":"999.99.99.999:5480", "user":{ "secret\_scope":"netezza\_keys", "key":"username" }, "password":{ "secret\_scope":"netezza\_keys", "key":"password" }}

    where: hostname - hostname:port for the JDBC connection secret\_scope - corresponds to the Databricks secret scope name that stores the username or password for the jdbc connection key - corresponds to the databricks secret name that stores the username or password for the jdbc connection \\

    For **Snowflake** it would be

    { "user":{ "secret\_scope":"SNOWFLAKE\_NP", "key":"username" }, "private\_key":{ "secret\_scope":"SNOWFLAKE\_NP", "key":"pkey" }, "sfRole":"role\_databricks\_nonprd", "url":"db.us-central1.gcp.snowflakecomputing.com", "warehouse":"WH"}

    where: secret\_scope - corresponds to the Databricks secret scope name that stores the username or private\_key for the snowflake connection key - corresponds to the databricks secret name that stores the username or private\_key for the snowflake connection sfRole - the default security role to use for the session after connecting url - the hostname for your snowfl       
                """)

with tab2:
    st.markdown("# Supported Source Systems")
    st.image("./logos/Source Systems.png", width = 700)
    st.markdown("""
    This tool helps to validate the tables between Databricks and the following warehouses:

*   Databricks
*   Netezza
*   Snowflake
*   MSSQL
*   Oracle
*   Teradata
*   Hive (EMR)
    """)


with tab3:
    st.image("./logos/Data Migration Validator HL Arch.png",)

with tab5:
    st.html("""<p>&nbsp;</p>
    <table style="width: 59.3312%; border-collapse: collapse; height: 36px;" border="0">
    <tbody>
    <tr style="height: 18px;">
    <td style="width: 10%; height: 18px;">Author</td>
    <td style="width: 90%; height: 18px;"><strong><em>Mahesh Madhusoodanan Pillai</em></strong></td>
    </tr>
    <tr style="height: 18px;">
    <td style="width: 10%; height: 18px;">Email</td>
    <td style="width: 90%; height: 18px;"><span style="color: #339966;"><strong><em>mahesh.pillai@databricks.com</em></strong></span></td>
    </tr>
    </tbody>
    </table>
    """)
    


