from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.dbgems import get_username
from dbacademy.dbgems import get_cloud
def get_job_param_json(env, solacc_path, job_name, node_type_id, spark_version, spark):
    
    # This job is not environment specific, so `env` is not used
    num_workers = 2
    solacc_id = "/RCG/POS DLT"
    user = get_username().split("@")[0].replace(".", "_")
    pipeline_name = f"POS_DLT_{user}"
    dlt_id_pdf = spark.read.format("delta").load("s3://db-gtm-industry-solutions/cicd/history/dlt/").filter(f"solacc = '{solacc_id}'").toPandas()
    dlt_definition_dict = {
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
                      "path": f"{solacc_path}/03_Bronze-to-Silver ETL"
                  }
              },
              {
                  "notebook": {
                      "path": f"{solacc_path}/04_Silver-to-Gold ETL"
                  }
              }
          ],
          "name": pipeline_name,
          "configuration": {
            "pipeline.user": user
          },
          "storage": f"/mnt/pos/dlt_pipeline_{user}",
          "target": f"pos_dlt_user_{user}",
          "allow_duplicate_names": True
      }
    pipeline_id = dlt_id_pdf['pipeline_id'][0] if len(dlt_id_pdf) > 0 else None
    if pipeline_id:
        dlt_definition_dict['id'] = pipeline_id
        print(f"Found dlt at '{pipeline_id}'; updating it with latest config if there is any change")
        client = DBAcademyRestClient()
        client.execute_put_json(f"{client.endpoint}/api/2.0/pipelines/{pipeline_id}", dlt_definition_dict)
    else:
        response = DBAcademyRestClient().pipelines().create_from_dict(dlt_definition_dict)
        pipeline_id = response["pipeline_id"]
        # log pipeline id to the cicd dlt table: we use this delta table to store pipeline id information because looking up pipeline id via API can sometimes bring back a lot of data into memory and cause OOM error, and because we use delta table to capture solacc cicd-related infra and history consistently. 
        # Reusing the DLT pipeline allows for DLT run history to accumulate over time rather than to be wiped out after each deployment. DLT has some UI components that only show up after the pipeline is executed at least twice. 
        spark.createDataFrame([{"solacc": solacc_id, "pipeline_id": pipeline_id}]).write.option("mergeSchema", "True").save("s3://db-gtm-industry-solutions/cicd/history/dlt/")
      
    job_json = {
        "timeout_seconds": 7200,
        "name": job_name,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "RCG"
        },
        "tasks": [
            {
                "job_cluster_key": "POS_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/02_Data Generation",
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
                    "spark_version": spark_version,
                "spark_conf": {
                    "spark.databricks.delta.formatCheck.enabled": "false"
                    },
                    "num_workers": num_workers,
                    "node_type_id": node_type_id,
                    "custom_tags": {
                        "usage": "solacc_testing"
                    },
                }
            }
        ]
    }
    cloud = get_cloud()
    if cloud == "AWS": 
      job_json["job_clusters"][0]["new_cluster"]["aws_attributes"] = {
                        "ebs_volume_count": 0,
                        "availability": "ON_DEMAND",
                        "instance_profile_arn": "arn:aws:iam::997819012307:instance-profile/shard-demo-s3-access",
                        "first_on_demand": 1
                    }
    if cloud == "MSA": 
      job_json["job_clusters"][0]["new_cluster"]["azure_attributes"] = {
                        "availability": "ON_DEMAND_AZURE",
                        "first_on_demand": 1
                    }
    if cloud == "GCP": 
      job_json["job_clusters"][0]["new_cluster"]["gcp_attributes"] = {
                        "use_preemptible_executors": False
                    }
    return job_json
    

    