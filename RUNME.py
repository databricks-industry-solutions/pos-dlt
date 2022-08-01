# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator with. It also creates the Workflow to create a Workflow DAG and illustrate the order of execution. Feel free to interactively run notebooks with the cluster or to run the Workflow to see how this solution accelerator executes. Happy exploring!
# MAGIC 
# MAGIC The pipelines, workflows and clusters created in this script are user-specific, so you can alter the workflow and cluster via UI without affecting other users. Running this script again after modification resets them.
# MAGIC 
# MAGIC **Note**: If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators sometimes require the user to set up additional cloud infra or data access, for instance. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-industry-solutions/notebook-solution-companion git+https://github.com/databricks-academy/dbacademy-rest git+https://github.com/databricks-academy/dbacademy-gems 

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

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
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                    "num_workers": 2,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"},
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
NotebookSolutionCompanion().deploy_compute(job_json)

# COMMAND ----------


