# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/pos-dlt. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/real-time-point-of-sale-analytics.

# COMMAND ----------

# MAGIC %md ## Step 1: Schedule the DLT Pipeline
# MAGIC 
# MAGIC To run the workflow defined in the *POS 03* and *POS 04* notebooks, we need to schedule them as a [DLT pipeline](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-ui.html). Using the Jobs UI within the Databricks workspace, we can click on the Delta Live Tables tab and click *Create Pipeline*.</p>
# MAGIC 
# MAGIC  <img src='https://brysmiwasb.blob.core.windows.net/demos/images/pos_dlt_scheduling_2.png' width=500> 
# MAGIC  
# MAGIC In the *Create Pipeline* dialog, we select *POS 03* and then click *Add Notebook Library* to select *POS 04*, indicating these two notebooks should be orchestrated as one workflow.
# MAGIC 
# MAGIC Because we are using the *apply_changes()* functionality in *POS 03*, we need to click *Add Configuration* and set a key and value of *pipelines.applyChangesPreviewEnabled* and *true*, respectively.  (This requirement will likely change at a future date.)
# MAGIC 
# MAGIC Under *Target*, we specify the name of the database within which DLT objects created in these workflows should reside. If we accept the defaults associated with this demo, that database is *pos_dlt*. 
# MAGIC 
# MAGIC Under *Storage Location*, we specify the storage location where object data and metadata will be placed.  Again, our *POS 01* notebook expects this to be */mnt/pos/dlt_pipeline*. 
# MAGIC 
# MAGIC Under *Pipeline Mode*, we specify how the cluster that runs our job will be managed.  If we select *Triggered*, the cluster shuts down with each cycle.  As several of our DLT objects are configured to run continuously, we should select *Continuous* mode. In our DLT object definitions, we leveraged some throttling techniques to ensure our workflows do not become overwhelmed with data.  Still, there will be some variability in terms of data moving through our pipelines so we might specify a minimum and maximum number of workers within a reasonable range based on our expectations for the data.  Once deployed, we might monitor resource utilization to determine if this range should be adjusted.
# MAGIC 
# MAGIC **NOTE** Continuous jobs will run indefinitely until explicitly stopped.  Please be aware of this as you manage your DLT pipelines.
# MAGIC 
# MAGIC Clicking *Create* we now have defined the jobs for our DLT workflow. And are presented with a UI with which we can monitor our jobs.|

# COMMAND ----------

# MAGIC %md ## Step 2: Monitor the Job
# MAGIC 
# MAGIC 
# MAGIC Initially, the UI will display *Waiting for Resources* as it waits for the jobs cluster to spin up.  Once a job cluster has been provisioned, the logic in the notebooks assigned to the DLT pipeline is executed and a graph representing the end-to-end workflow is presented:
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/pos_DLT_scheduling.png'>
# MAGIC 
# MAGIC The connections between the items indicate the dependencies between objects.  Color coding indicates the status of the tables in the pipeline. Should an error be encountered, event information at the bottom of the UI would reflect this.  Clicking on the error event would then expose error messages with which the problem could be diagnosed.

# COMMAND ----------

# MAGIC %md When the job initially runs, it will run in *Development* mode as indicated at the top of the UI.  In Development mode, any errors will cause the job to be stopped so that they may be corrected. By clicking *Production*, the job is moved into a state where jobs are restarted upon error.
# MAGIC 
# MAGIC To stop a job, click the *Stop* button at the top of the UI. **If you do not explicitly stop this job, it will run indefinitely.**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | azure-iot-device                                     | Microsoft Azure IoT Device Library | MIT    | https://pypi.org/project/azure-iot-device/                       |
# MAGIC | azure-storage-blob                                | Microsoft Azure Blob Storage Client Library for Python| MIT        | https://pypi.org/project/azure-storage-blob/      |
