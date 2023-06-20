# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to illustrate the order of execution. Happy exploring! 
# MAGIC 🎉
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Simply attach this notebook to a cluster and hit Run-All for this notebook. A multi-step job and the clusters used in the job will be created for you and hyperlinks are printed on the last block of the notebook. 
# MAGIC
# MAGIC 2. Run the accelerator notebooks: Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** with the cluster to see how this solution accelerator executes. 
# MAGIC
# MAGIC     2a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     2b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the `job_json['tasks']` below.
# MAGIC
# MAGIC **Prerequisites** 
# MAGIC 1. You need to have cluster creation permissions in this workspace.
# MAGIC
# MAGIC 2. In case the environment has cluster-policies that interfere with automated deployment, you may need to manually create the cluster in accordance with the workspace cluster policy. The `job_json` definition below still provides valuable information about the configuration these series of notebooks should run with. 
# MAGIC
# MAGIC **Notes**
# MAGIC 1. The pipelines, workflows and clusters created in this script are not user-specific. Keep in mind that rerunning this script again after modification resets them for other users too.
# MAGIC
# MAGIC 2. If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators may require the user to set up additional cloud infra or secrets to manage credentials. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

# MAGIC %md
# MAGIC Before setting up the rest of the accelerator, we need set up a few credentials in order to access the source dataset and other cloud infrastructure dependencies. Here we demonstrate using the [Databricks Secret Scope](https://docs.databricks.com/security/secrets/secret-scopes.html) for credential management. 
# MAGIC
# MAGIC #### Step 1a: Setup the Azure IOT Hub
# MAGIC
# MAGIC To setup and configure the Azure IOT Hub, you will need to:</p>
# MAGIC
# MAGIC 1. [Create an Azure IOT Hub](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-create-through-portal#create-an-iot-hub). We used an S1-sized IOT Hub for a 10x playback of event data as described in the next notebook.
# MAGIC 2. [Add a Device and retrieve the Device Connection String](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-create-through-portal#register-a-new-device-in-the-iot-hub). We used a device with Symmetric key authentication and auto-generated keys enabled to connect to the IOT Hub.
# MAGIC 3. [Retrieve the Azure IOT Hub's Event Hub Endpoint Compatible Endpoint property](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-messages-read-builtin#read-from-the-built-in-endpoint).
# MAGIC 4. Set Azure IOT Hub relevant configuration values in the block below. You can set up a [secret scope](https://docs.databricks.com/security/secrets/secret-scopes.html) to manage credentials used in notebooks. For the block below, we have manually set up the `solution-accelerator-cicd` secret scope and saved our credentials there for internal testing purposes.
# MAGIC
# MAGIC **NOTE** Details on the Kafka configurations associated with an Azure IOT Hub's event hub endpoint are found [here](https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/tutorials/spark). 
# MAGIC
# MAGIC ####Step 1b: Setup the Azure Storage Account
# MAGIC
# MAGIC To setup and configure the Azure Storage account, you will need to:</p>
# MAGIC
# MAGIC 1. [Create an Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal).
# MAGIC 2. [Create a Blob Storage container](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container).
# MAGIC 3. [Retrieve an Account Access Key & Connection String](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys).
# MAGIC 4. Set Azure Storage Account relevant configuration values in the cell below. You can set up a [secret scope](https://docs.databricks.com/security/secrets/secret-scopes.html) to manage credentials used in notebooks. For the block below, we have manually set up the `solution-accelerator-cicd` secret scope and saved our credentials there for internal testing purposes.
# MAGIC
# MAGIC Copy the block of code below, replace the name the secret scope and fill in the credentials and execute the block. After executing the code, The accelerator notebook will be able to access the credentials it needs.
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC client = NotebookSolutionCompanion().client
# MAGIC try:
# MAGIC   client.execute_post_json(f"{client.endpoint}/api/2.0/secrets/scopes/create", {"scope": "solution-accelerator-cicd"})
# MAGIC except:
# MAGIC   pass
# MAGIC
# MAGIC client.execute_post_json(f"{client.endpoint}/api/2.0/secrets/put", {
# MAGIC   "scope": "solution-accelerator-cicd",
# MAGIC   "key": "rcg_pos_iot_hub_conn_string",
# MAGIC   "string_value": '____' 
# MAGIC })
# MAGIC
# MAGIC client.execute_post_json(f"{client.endpoint}/api/2.0/secrets/put", {
# MAGIC   "scope": "solution-accelerator-cicd",
# MAGIC   "key": "rcg_pos_iot_hub_endpoint",
# MAGIC   "string_value": '____'
# MAGIC })
# MAGIC
# MAGIC try:
# MAGIC   client.execute_post_json(f"{client.endpoint}/api/2.0/secrets/scopes/create", {"scope": "clickstream-readonly"})
# MAGIC except:
# MAGIC   pass
# MAGIC client.execute_post_json(f"{client.endpoint}/api/2.0/secrets/put", {
# MAGIC   "scope": "clickstream-readonly", 
# MAGIC   "key": "rcg_pos_storage_account_name",
# MAGIC   "string_value": "____"
# MAGIC })
# MAGIC
# MAGIC try:
# MAGIC   client.execute_post_json(f"{client.endpoint}/api/2.0/secrets/scopes/create", {"scope": "clickstream-readwrite"})
# MAGIC except:
# MAGIC   pass
# MAGIC client.execute_post_json(f"{client.endpoint}/api/2.0/secrets/put", {
# MAGIC   "scope": "clickstream-readwrite", 
# MAGIC   "key": "rcg_pos_storage_account_key",
# MAGIC   "string_value": "____"
# MAGIC })
# MAGIC
# MAGIC ```

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS databricks_solacc LOCATION '/databricks_solacc/'")
spark.sql(f"CREATE TABLE IF NOT EXISTS databricks_solacc.dlt (path STRING, pipeline_id STRING, solacc STRING)")
dlt_config_table = "databricks_solacc.dlt"

# COMMAND ----------

pipeline_json = {
          "clusters": [
              {
                  "label": "default",
                  "autoscale": {
                      "min_workers": 1,
                      "max_workers": 3
                  }
              }
          ],
          "libraries": [
              {
                  "notebook": {
                      "path": "03_Bronze-to-Silver ETL"
                  }
              },
              {
                  "notebook": {
                      "path": "04_Silver-to-Gold ETL"
                  }
              }
          ],
          "name": "POS_DLT",
          "storage": f"/databricks_solacc/pos/dlt_pipeline",
          "target": f"solacc_pos_dlt",
          "allow_duplicate_names": True
      }

# COMMAND ----------

pipeline_id = NotebookSolutionCompanion().deploy_pipeline(pipeline_json, dlt_config_table, spark)

# COMMAND ----------

job_json = {
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "RCG_solacc_automation"
        },
        "tasks": [
            {
                "job_cluster_key": "POS_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": "02_Data Generation",
                    "base_parameters": {
                        "mode": "test"
                    }
                },
                "task_key": "POS_01",
                "description": ""
            },
            {
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
                "task_key": "POS_02",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "POS_01"
                    }
                ]
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "POS_cluster",
                "new_cluster": {
                    "spark_version": "12.2.x-cpu-ml-scala2.12",
                    "num_workers": 2,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"},
                    "custom_tags": {
                        "usage": "solacc_testing",
                        "group": "RCG_solacc_automation"
                    },
                }
            }
        ]
    }

# COMMAND ----------

# DBTITLE 1,Companion job and cluster(s) definition
dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)

# COMMAND ----------


