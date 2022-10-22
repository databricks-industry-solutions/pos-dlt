# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/pos-dlt. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/real-time-point-of-sale-analytics.

# COMMAND ----------

# MAGIC %md This notebook provides instructions for setting up the cloud services on which subsequent notebooks depend.  It also captures various configuration settings required by those notebooks. You do not need to run this notebook separately as each notebook in the set calls this notebook to retrieve configuration data. **However**, you should read the environment setup instructions in this notebook carefully and update all configuration values as appropriate.
# MAGIC 
# MAGIC **NOTE** For environments with sensitive data, it is advised that you take advantage of the Databricks [secret management](https://docs.databricks.com/security/secrets/index.html) capability to secure these values.

# COMMAND ----------

# DBTITLE 1,Initialize Config Settings
if 'config' not in locals():
  config = {}

# COMMAND ----------

# MAGIC %md ## Step 1: Setup the Azure Environment
# MAGIC 
# MAGIC While Databricks is a cloud-agnostic platform, this demonstration will leverage several external technologies made available by the cloud provider. This will require us to supply cloud-specific environment setup guidance. For this demonstration, we've elected to make use of technologies provided by the Microsoft Azure cloud though this scenario is supportable using similar technologies made available by AWS and GCP.
# MAGIC 
# MAGIC The Azure-specific technologies we will use are:</p>
# MAGIC 
# MAGIC * [Azure IOT Hub](https://docs.microsoft.com/en-us/azure/iot-hub/about-iot-hub)
# MAGIC * [Azure Storage](https://docs.microsoft.com/en-us/azure/storage/common/storage-introduction)
# MAGIC 
# MAGIC To set these up, you will need to have access to an [Azure subscription](https://azure.microsoft.com/en-us/account/).  

# COMMAND ----------

# MAGIC %md #### Step 1a: Setup the Azure IOT Hub
# MAGIC 
# MAGIC To setup and configure the Azure IOT Hub, you will need to:</p>
# MAGIC 
# MAGIC 1. [Create an Azure IOT Hub](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-create-through-portal#create-an-iot-hub). We used an S1-sized IOT Hub for a 10x playback of event data as described in the next notebook.
# MAGIC 2. [Add a Device and retrieve the Device Connection String](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-create-through-portal#register-a-new-device-in-the-iot-hub). We used a device with Symmetric key authentication and auto-generated keys enabled to connect to the IOT Hub.
# MAGIC 3. [Retrieve the Azure IOT Hub's Event Hub Endpoint Compatible Endpoint property](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-messages-read-builtin#read-from-the-built-in-endpoint).
# MAGIC 4. Set Azure IOT Hub relevant configuration values in the block below. You can set up a [secret scope](https://docs.databricks.com/security/secrets/secret-scopes.html) to manage credentials used in notebooks. For the block below, we have manually set up the `solution-accelerator-cicd` secret scope and saved our credentials there for internal testing purposes.
# MAGIC 
# MAGIC **NOTE** Details on the Kafka configurations associated with an Azure IOT Hub's event hub endpoint are found [here](https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/tutorials/spark). 

# COMMAND ----------

# DBTITLE 1,Config Settings for Azure IOT Hub 
# config['iot_device_connection_string'] = 'YOUR IOT HUB DEVICE CONNECTION STRING HERE' # replace with your own credential here temporarily or set up a secret scope with your credential
config['iot_device_connection_string'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_iot_hub_conn_string") 

# config['event_hub_compatible_endpoint'] = 'YOUR IOT HUB EVENT HUB COMPATIBLE ENDPOINT PROPERTY HERE' # replace with your own credential here temporarily or set up a secret scope with your credential
config['event_hub_compatible_endpoint'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_iot_hub_endpoint") 

# helper function to convert strings above into dictionaries
def split_connstring(connstring):
  conn_dict = {}
  for kv in connstring.split(';'):
    k,v = kv.split('=',1)
    conn_dict[k]=v
  return conn_dict
  
# split conn strings
iothub_conn = split_connstring(config['iot_device_connection_string'])
eventhub_conn = split_connstring(config['event_hub_compatible_endpoint'])


# configure kafka endpoint settings
config['eh_namespace'] = eventhub_conn['Endpoint'].split('.')[0].split('://')[1] 
config['eh_kafka_topic'] = iothub_conn['HostName'].split('.')[0]
config['eh_listen_key_name'] = 'ehListen{0}AccessKey'.format(config['eh_namespace'])
config['eh_bootstrap_servers'] = '{0}.servicebus.windows.net:9093'.format(config['eh_namespace'])
config['eh_sasl'] = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint={0};SharedAccessKeyName={1};SharedAccessKey={2}\";'.format(eventhub_conn['Endpoint'], eventhub_conn['SharedAccessKeyName'], eventhub_conn['SharedAccessKey'])

# COMMAND ----------

# MAGIC %md ####Step 1b: Setup the Azure Storage Account
# MAGIC 
# MAGIC To setup and configure the Azure Storage account, you will need to:</p>
# MAGIC 
# MAGIC 1. [Create an Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal).
# MAGIC 2. [Create a Blob Storage container](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container).
# MAGIC 3. [Retrieve an Account Access Key & Connection String](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys).
# MAGIC 4. Set Azure Storage Account relevant configuration values in the cell below. You can set up a [secret scope](https://docs.databricks.com/security/secrets/secret-scopes.html) to manage credentials used in notebooks. For the block below, we have manually set up the `solution-accelerator-cicd` secret scope and saved our credentials there for internal testing purposes.

# COMMAND ----------

# DBTITLE 1,Config Settings for Azure Storage Account
# config['storage_account_name'] = 'YOUR STORAGE ACCOUNT NAME STRING HERE' # replace with your own credential here temporarily or set up a secret scope with your credential
config['storage_account_name'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_storage_account_name") 

config['storage_container_name'] = 'pos'

# config['storage_account_access_key'] = 'YOUR STORAGE ACCOUNT ACCESS KEY HERE' # replace with your own credential here temporarily or set up a secret scope with your credential
config['storage_account_access_key'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_storage_account_key") 

config['storage_connection_string'] = 'DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};EndpointSuffix=core.windows.net'.format(config['storage_account_name'], config['storage_account_access_key'])

# COMMAND ----------

# MAGIC %md ## Step 2: Mount the Azure Storage to Databricks
# MAGIC 
# MAGIC The Azure Storage account created in the previous step will now be [mounted](https://docs.databricks.com/data/data-sources/azure/azure-storage.html#mount-azure-blob-storage-containers-to-dbfs) to the Databricks environment.  To do this, you will need access to various storage account details captured in Step 1b:

# COMMAND ----------

# DBTITLE 1,Config Settings for DBFS Mount Point
config['dbfs_mount_name'] = f'/mnt/pos' 

# COMMAND ----------

# DBTITLE 1,Create DBFS Mount Point
conf_key_name = "fs.azure.account.key.{0}.blob.core.windows.net".format(config['storage_account_name'])
conf_key_value = config['storage_account_access_key']

# determine if not already mounted
for m in dbutils.fs.mounts():
  mount_exists = (m.mountPoint==config['dbfs_mount_name'])
  if mount_exists: break

# create mount if not exists
if not mount_exists:
  
  print('creating mount point {0}'.format(config['dbfs_mount_name']))
  
  # create mount
  dbutils.fs.mount(
    source = "wasbs://{0}@{1}.blob.core.windows.net".format(
      config['storage_container_name'], 
      config['storage_account_name']
      ),
    mount_point = config['dbfs_mount_name'],
    extra_configs = {conf_key_name:conf_key_value}
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 3: Upload Data Files to Azure Storage
# MAGIC 
# MAGIC This accelerator makes use of simulated data that can be downloaded to your desktop system from [here](https://github.com/databricks/tech-talks/blob/master/datasets/point_of_sale_simulated.zip). To make this data available to the notebooks, the various files must be uploaded to the DBFS mount point created in the last step as follows:
# MAGIC 
# MAGIC **NOTE** In the table below, it is assumed the mount point is defined as */mnt/pos*. If you used an alternative mount point name above, be sure you updated the *dbfs_mount_name* configuration parameter and deposit the files into the appropriate location.
# MAGIC 
# MAGIC | File Type | File | Path |
# MAGIC | -----|------|------|
# MAGIC | Change Event| inventory_change_store001.txt |  /mnt/pos/generator/inventory_change_store001.txt |
# MAGIC |Change Event| inventory_change_online.txt | /mnt/pos/generator/inventory_change_online.txt  |
# MAGIC |Snapshot| inventory_snapshot_store001.txt | /mnt/pos/generator/inventory_snapshot_store001.txt  |
# MAGIC |Snapshot| inventory_snapshot_online.txt | /mnt/pos/generator/inventory_snapshot_online.txt  |
# MAGIC |Static |      stores.txt                         |         /mnt/pos/static_data/stores.txt                                         |
# MAGIC |Static|      items.txt                 |           /mnt/pos/static_data/items.txt                                       |
# MAGIC |Static|      inventory_change_type.txt                      |           /mnt/pos/static_data/inventory_change_type.txt                                       |
# MAGIC 
# MAGIC To upload these files from your desktop system, please feel free to use either of the following techniques:</p>
# MAGIC 
# MAGIC * [Upload files via the Azure Portal](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal)
# MAGIC * [Upload files via Azure Storage Explorer](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-storage-explorer)

# COMMAND ----------

# DBTITLE 1,Config Settings for Data Files
# change event data files
config['inventory_change_store001_filename'] = config['dbfs_mount_name'] + '/generator/inventory_change_store001.txt'
config['inventory_change_online_filename'] = config['dbfs_mount_name'] + '/generator/inventory_change_online.txt'

# snapshot data files
config['inventory_snapshot_store001_filename'] = config['dbfs_mount_name'] + '/generator/inventory_snapshot_store001.txt'
config['inventory_snapshot_online_filename'] = config['dbfs_mount_name'] + '/generator/inventory_snapshot_online.txt'

# static data files
config['stores_filename'] = config['dbfs_mount_name'] + '/static_data/store.txt'
config['items_filename'] = config['dbfs_mount_name'] + '/static_data/item.txt'
config['change_types_filename'] = config['dbfs_mount_name'] + '/static_data/inventory_change_type.txt'

# COMMAND ----------

# MAGIC %md ## Step 4: Configure Misc. Items
# MAGIC 
# MAGIC In this last step, we will provide the paths to a few items our accelerator will need to access.  First amongst these is the location of the inventory snapshot files our simulated stores will deposit into our streaming infrastructure.  This path should be dedicated to this one purpose and not shared with other file types:

# COMMAND ----------

# DBTITLE 1,Config Settings for Checkpoint Files
config['inventory_snapshot_path'] = config['dbfs_mount_name'] + '/inventory_snapshots/'

# COMMAND ----------

# MAGIC %md Next, we will configure the default storage location for our DLT objects and metadata:

# COMMAND ----------

# DBTITLE 1,Config Settings for DLT Data
config['dlt_pipeline'] = config['dbfs_mount_name'] + '/dlt_pipeline'

# COMMAND ----------

# MAGIC %md Finally, we will set the name of the database within which persisted data objects will be housed:

# COMMAND ----------

# DBTITLE 1,Identify Database for Data Objects and initialize it
database_name = f'pos_dlt'
config['database'] = database_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | azure-iot-device                                     | Microsoft Azure IoT Device Library | MIT    | https://pypi.org/project/azure-iot-device/                       |
# MAGIC | azure-storage-blob                                | Microsoft Azure Blob Storage Client Library for Python| MIT        | https://pypi.org/project/azure-storage-blob/      |
