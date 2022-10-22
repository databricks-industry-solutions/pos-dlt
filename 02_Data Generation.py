# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/pos-dlt. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/real-time-point-of-sale-analytics.

# COMMAND ----------

# MAGIC %md This notebook was developed to run on a minimal (one-worker node) cluster. You should leave this notebook running on this cluster while you run the remaining notebooks as a Delta Live Tables job.

# COMMAND ----------

# MAGIC %md The purpose of this notebook is to generate a stream of inventory-relevant data originating from two simulated stores, one physical and the other online.  These data are transmitted to various data ingest services configured with the cloud provider as indicated in the *POS 01* notebook.
# MAGIC 
# MAGIC The first of the two streams of data being generated represents inventory change events captured by a store's point-of-sale system. The second of these streams represents the point-in-time quantities (snapshot) of products in a store as determined through a manual count of inventory. Due to differences in the frequency with which each type of data is generated and the volume of data associated with each, they are transmitted in different formats using different delivery mechanisms.
# MAGIC </p>
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/pos_data_generation.png' width=400>
# MAGIC 
# MAGIC **IMPORTANT NOTE** The Azure IOT Hub is not reset between runs of this notebook.  As a result, it is possible that the messages delivered from prior runs could still be read by downstream streaming jobs.  To ensure a clean environment between runs, you may wish to first delete your Azure IOT Hub deployment and then recreate it.  This will require you to repeat the steps outlined in *POS 01* and update appropriate configuration values in that notebook.

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install azure-iot-device==2.7.1 --use-feature=2020-resolver
# MAGIC %pip install azure-storage-blob==12.8.1

# COMMAND ----------

dbutils.widgets.text("mode", "prod")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import *
import pyspark.sql.functions as f

import datetime, time

from azure.iot.device import IoTHubDeviceClient
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

# DBTITLE 1,Notebook Configuration
# MAGIC %run "./01_Environment Setup"

# COMMAND ----------

# MAGIC %md This notebook is typically run to simulate a new stream of POS data. To ensure data from prior runs are not used in downstream calculations, you should reset the database and DLT engine environment between runs of this notebook:
# MAGIC 
# MAGIC **NOTE** These actions are not typical of a real-world workflow but instead are here to ensure the proper calculation of values in a simulation.

# COMMAND ----------

# DBTITLE 1,Reset the Target Database
_ = spark.sql("DROP DATABASE IF EXISTS {0} CASCADE".format(config['database']))
_ = spark.sql("CREATE DATABASE IF NOT EXISTS {0}".format(config['database']))

# COMMAND ----------

# MAGIC %md **NOTE** This next step should be run before any DLT jobs are launched.

# COMMAND ----------

# DBTITLE 1,Reset the DLT Environment
dbutils.fs.rm(config['dlt_pipeline'],True)

# COMMAND ----------

# MAGIC %md ## Step 1: Assemble Inventory Change Records
# MAGIC 
# MAGIC The inventory change records represent events taking place in a store location which impact inventory.  These may be sales transactions, recorded loss, damage or theft, or replenishment events. In addition, Buy-Online, Pickup In-Store (BOPIS) events originating in the online store and fulfilled by the physical store are captured in the data. While each of these events might have some different attributes associated with them, they have been consolidated into a single stream of inventory change event records. Each event type is distinguished through a change type identifier.
# MAGIC 
# MAGIC A given event may involve multiple products (items).  By grouping the data for items associated with an event around the transaction ID that uniquely identifies that event, the multiple items associated with a sales transaction or other event type can be efficiently transmitted:

# COMMAND ----------

# DBTITLE 1,Combine & Reformat Inventory Change Records
# format of inventory change records
inventory_change_schema = StructType([
  StructField('trans_id', StringType()),  # transaction event ID
  StructField('item_id', IntegerType()),  
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('quantity', IntegerType()),
  StructField('change_type_id', IntegerType())
  ])

# inventory change record data files (one from each store)
inventory_change_files = [
  config['inventory_change_store001_filename'],
  config['inventory_change_online_filename']
  ]

# read inventory change records and group items associated with each transaction so that one output record represents one complete transaction
inventory_change = (
  spark
    .read
    .csv(
      inventory_change_files, 
      header=True, 
      schema=inventory_change_schema, 
      timestampFormat='yyyy-MM-dd HH:mm:ss'
      )
    .withColumn('trans_id', f.expr('substring(trans_id, 2, length(trans_id)-2)')) # remove surrounding curly braces from trans id
    .withColumn('item', f.struct('item_id', 'quantity')) # combine items and quantities into structures from which we can build a list
    .groupBy('date_time','trans_id')
      .agg(
        f.first('store_id').alias('store_id'),
        f.first('change_type_id').alias('change_type_id'),
        f.collect_list('item').alias('items')  # organize item info as a list
        )
    .orderBy('date_time','trans_id')
    .toJSON()
    .collect()
  )

# print a single transaction record to illustrate data structure
eval(inventory_change[0])


# COMMAND ----------

# MAGIC %md ##Step 2: Assemble Inventory Snapshots
# MAGIC 
# MAGIC Inventory snapshots represent the physical counts taken of products sitting in inventory.  
# MAGIC Such counts are periodically taken to capture the true state of a store's inventory and are necessary given the challenges most retailers encounter in inventory management.
# MAGIC 
# MAGIC With each snapshot, we capture basic information about the store, item, and quantity on-hand along with the date time and employee associated with the count.  So that the impact of inventory snapshots may be more rapidly reflected in our streams, we simulate a complete recount of products in a store every 5-days.  This is far more aggressive than would occur in the real world but again helps to demonstrate the streaming logic:

# COMMAND ----------

# DBTITLE 1,Access Inventory Snapshots
# format of inventory snapshot records
inventory_snapshot_schema = StructType([
  StructField('item_id', IntegerType()),
  StructField('employee_id', IntegerType()),
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('quantity', IntegerType())
  ])

# inventory snapshot files
inventory_snapshot_files = [ 
  config['inventory_snapshot_store001_filename'],
  config['inventory_snapshot_online_filename']
  ]

# read inventory snapshot data
inventory_snapshots = (
  spark
    .read
    .csv(
      inventory_snapshot_files, 
      header=True, 
      timestampFormat='yyyy-MM-dd HH:mm:ss', 
      schema=inventory_snapshot_schema
      )
  )

display(inventory_snapshots)

# COMMAND ----------

# MAGIC %md To coordinate the transmission of change event data with periodic snapshots, the unique dates and times for which snapshots were taken within a given store location are extracted to a list.  This list will be used in the section of code that follows:
# MAGIC 
# MAGIC **NOTE** It is critical for the logic below that this list of dates is sorted in chronological order.

# COMMAND ----------

# DBTITLE 1,Assemble Set of Snapshot DateTimes by Store
# get date_time of each inventory snapshot by store
inventory_snapshot_times = (
  inventory_snapshots
    .select('date_time','store_id')
    .distinct()
    .orderBy('date_time')  # sorting of list is essential for logic below
  ).collect()

# display snapshot times
inventory_snapshot_times

# COMMAND ----------

# MAGIC %md ## Step 3: Transmit Store Data to the Cloud
# MAGIC 
# MAGIC In this step, we will send the event change JSON documents to an Azure IOT Hub.  Using the time differences between transactions, we will delay the transmittal of each document by a calculated number of seconds. That number of seconds, derived by calculating the seconds between the current transaction and the previous transaction, is adjusted by the *event_speed_factor* variable, allowing you to speed up or slow down the replay for your needs.
# MAGIC 
# MAGIC **NOTE** Please note that the Azure IOT Hub enforces a limit on the number of events which can be transmitted to it on a per second basis. If this limit is exceeded, you may experience *429 errors* from the IOT Hub client below. For more information this, please refer to [this document](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-quotas-throttling).
# MAGIC 
# MAGIC As events are transmitted, the data is checked to see if any snapshot files should be generated and sent into the Azure Storage account. In order to ensure the snapshot data is received in-sequence, any old snapshot files in the storage account are deleted before data transmission begins:

# COMMAND ----------

# DBTITLE 1,Delete Any Old Snapshot Files
# connect to container holding old snapshots
blob_service_client = BlobServiceClient.from_connection_string(config['storage_connection_string'])
container_client = blob_service_client.get_container_client(container=config['storage_container_name'])

# for each blob in specified "path"
for blob in container_client.list_blobs(name_starts_with=config['inventory_snapshot_path'].replace(config['dbfs_mount_name'],'')[1:]):
  blob_client = container_client.get_blob_client(blob)
  blob_client.delete_blob()

# close clients
container_client.close()
blob_service_client.close()

# COMMAND ----------

# DBTITLE 1,Connect to IOT Hub
# make sure to disconnect if this is a re-run of the notebook
if 'client' in locals():
  try:
    client.disconnect()
  except:
    pass

# connect to iot hub
client = IoTHubDeviceClient.create_from_connection_string( config['iot_device_connection_string'] )
client.connect()

# COMMAND ----------

# DBTITLE 1,Send Transactions
if mode != "prod":
  inventory_change = inventory_change[:100] # limit the number of transactions here to shorten execution duration; this notebook is engineered to run for approx. 3 days without this limit
event_speed_factor = 10 # Send records to iot hub at <event_speed_factor> X real-time speed
max_msg_size = 256 * 1024 # event message to iot hub cannot exceed 256KB
last_dt = None

for event in inventory_change:
  # extract datetime from transaction document
  d = eval(event) # evaluate json as a dictionary
  dt = datetime.datetime.strptime( d['date_time'], '%Y-%m-%dT%H:%M:%S.000Z')
  
  # inventory snapshot transmission
  # -----------------------------------------------------------------------
  snapshot_start = time.time()
  
  inventory_snapshot_times_for_loop = inventory_snapshot_times # copy snapshot times list as this may be modified in loop
  for snapshot_dt, store_id in inventory_snapshot_times_for_loop: # for each snapshot
    
    # if event date time is before next snapshot date time
    if dt < snapshot_dt: # (snapshot times are ordered by date)
      break              #   nothing to transmit
      
    else: # event date time exceeds a snapshot datetime
      
      # extract snaphot data for this dt
      snapshot_pd = (
        inventory_snapshots
          .filter(f.expr("store_id={0} AND date_time='{1}'".format(store_id, snapshot_dt)))
          .withColumn('date_time', f.expr("date_format(date_time, 'yyyy-MM-dd HH:mm:ss')")) # force timestamp conversion to include 
          .toPandas()  
           )
        
      # transmit to storage blob as csv
      blob_service_client = BlobServiceClient.from_connection_string(config['storage_connection_string'])
      blob_client = blob_service_client.get_blob_client(
        container=config['storage_container_name'], 
        blob=(config['inventory_snapshot_path'].replace(config['dbfs_mount_name'],'')[1:]+
              'inventory_snapshot_{0}_{1}'.format(store_id,snapshot_dt.strftime('%Y-%m-%d %H:%M:%S')))
        )
      blob_client.upload_blob(str(snapshot_pd.to_csv()), overwrite=True)
      blob_client.close()
      
      # remove snapshot date from inventory_snapshot_times
      inventory_snapshot_times.pop(0)
      print('Loaded inventory snapshot for {0}'.format(snapshot_dt.strftime('%Y-%m-%d %H:%M:%S')))
      
  snapshot_seconds = time.time() - snapshot_start
  # -----------------------------------------------------------------------
  
  # inventory change event transmission
  # -----------------------------------------------------------------------
  # calculate delay (in seconds) between this event and prior event (account for snapshot)
  if last_dt is None: last_dt = dt
  delay = (dt - last_dt).seconds - snapshot_seconds
  delay = int(delay/event_speed_factor) # adjust delay by event speed factor
  if delay < 0: delay = 0
  
  # sleep for delay duration
  #print('Sleep for {0} seconds'.format(delay))
  #time.sleep(delay)
  
  # send items individually if json document too large
  # change log feb 28 2022 - added `temp = [item]` to convert dictionary to list.
  if len(event) > max_msg_size:
    items = d.pop('items') # retrieve items collection
    for i, item in enumerate(items): # for each item
      temp = [item]
      d['items'] = temp   # add a one-item items-collection
      client.send_message(str(d)) # send message

      if (i+1)%25==0: # pause transmission every Xth item to avoid overwhelming IOT hub
        time.sleep(1)
  else:  # send whole transaction document
    client.send_message(event)
    
  # -----------------------------------------------------------------------
  
  # set last_dt for next cycle
  last_dt = dt

# COMMAND ----------

# DBTITLE 1,Disconnect from IOT Hub
client.disconnect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | azure-iot-device                                     | Microsoft Azure IoT Device Library | MIT    | https://pypi.org/project/azure-iot-device/                       |
# MAGIC | azure-storage-blob                                | Microsoft Azure Blob Storage Client Library for Python| MIT        | https://pypi.org/project/azure-storage-blob/      |

# COMMAND ----------


