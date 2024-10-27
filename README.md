# Development-of-a-hive-application-for-data-loading-and-processing
**Description:** Development of a hive application for data loading and processing, using hadoop and spark clusters for efficient data analysis.
<br>
<br>
**Technologies used:** Hadoop, Hive, HDFS, Shell Scripting, Parquet, Cron
<br>
<br>
**Project Steps:**
- Creation of tables in the data warehouse:
   - Creation of a staging table.
   - Creation of a partitioned Parquet table to optimize performance.
- Loading Data/HiveQL Script Development:
   - Loading data into the staging table from a local file source.
   - Loading data into the partitioned table from the staging table.
   - Developing HiveQL scripts to automate the loading and processing of data.
- Infrastructure Improvement:
   - Redefining the solution to use the HDFS environment as the location for the file source instead of the local.
- Validation and Deployment:
   - Validating the Hive application to ensure the correct conversion of data.
   - Deploying the Hive application in HDFS for further access and processing.
- Automation:
   - Developing a shell wrapper to execute the Hive application.
   - Scheduling the execution of Hive applications using Cron.
