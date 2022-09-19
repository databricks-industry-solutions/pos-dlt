## Overview

The Point-of-Sale Streaming with Delta Live Tables (DLT) solution accelerator demonstrates how Delta Live Tables may be used to construct a near real-time lakehouse architecture calculating current inventories for various products across multiple store locations. Instead for moving directly from raw ingested data to the calculated inventory, the solution separates the logic into two stages.</p>

<img src='https://brysmiwasb.blob.core.windows.net/demos/images/pos_dlt_pipeline_UPDATED.png' width=800>

In the first stage (denoted in the figure as *Bronze-to-Silver ETL*), ingested data is transformed for greater accessibility.  Actions taken against the data in this stage, *e.g.* the decomposing of nested arrays, the deduplication of records, *etc.*, are not intended to apply any business-driven interpretations to the data.  The tables written to in this phase represent the *Silver* layer of our lakehouse [medallion architecture](https://databricks.com/glossary/medallion-architecture).

In the second stage (denoted in the figure as *Silver-to-Gold ETL*, the Silver tables are used to derive our business-aligned output, calculated current-state inventory.  These data are written to a table representing the *Gold* layer of our architecture.

Across this two-staged workflow, [Delta Live Tables (DLT)](https://databricks.com/product/delta-live-tables) is used as an orchestration and monitoring utility.


## The Notebooks

Excluding this one, the solution accelerator is comprised of 5 notebooks. These notebooks are:</p>

* *POS 01: Environment Setup*
* *POS 02: Data Generation*
* *POS 03: Bronze-to-Silver ETL*
* *POS 04: Silver-to-Gold ETL*
* *POS 05: DLT Job Scheduling*

The *POS 01: Environment Setup* notebook provides instructions on how to setup the environment for data ingest. This setup includes the provisioning of cloud-resources outside of the Databricks workspace.  While every cloud-provider on which Databricks runs provides access to storage and event ingest mechanisms, we've elected to demonstrate this accelerator using Azure cloud services. Organizations leveraging other clouds or different services on the Azure cloud should be able to follow the logic in our approach and convert various elements in the *POS 01*, *POS 02* and *POS 03* notebooks to work with those alternative capabilities.

The *POS 02: Data Generation* notebook reads data from a 30-day simulation of POS data from two fictional stores to generate a stream of event data targeted at the data ingest mechanisms provisioned in *POS 01*. The simulated data is played back at 10x speed to balance the needs of those who wish to examine the streaming process over sustained intervals while not running for a full 30-day span of time. The logic in this notebook has only limited dependencies on the Databricks environment.  We elected to write this code within a Databricks notebook instead of a stand-alone Python script for the sake of consistency.  This notebook should be running on a minimally sized Databricks cluster before launching the remaining notebooks in the accelerator.

The *POS 03: Bronze-to-Silver ETL* notebook represents the first stage of the workflow described above. In it, tables representing POS transactions and inventory count events are created from the ingested data.  These data are captured in near real-time.  In addition, three static tables representing data changing very infrequently are created.  A key aspect of this notebook is that different tables with different frequencies of data updates can be supported using similar mechanisms.

The *POS 04: Silver-to-Gold ETL* notebook represents the second stage of the workflow described above. The logic in this notebook builds off the Silver-layer tables created in the *POS 03* notebook to create the business-aligned output required for this scenario. It's important to note that because the Silver tables are built using the Delta Lake storage format, updates to those tables taking place through the workflows defined in *POS 03* are immediately to this workflow, retaining an end-to-end streaming experience.

The *POS 05: DLT Job Scheduling* notebook addresses the scheduling of the workflows defined in the *POS 03* and *POS 04* notebooks. It's important to understand that the *POS 03* and *POS 04* notebooks defining the first and second stages of the workflow **are not** run interactively and instead must be scheduled with the DLT engine in order to access the functionality it provides.

## Structured Streaming vs. Delta Live Tables

These notebooks represent a revision of a solution accelerator published in 2021.  In [that accelerator](https://databricks.com/blog/2021/09/09/real-time-point-of-sale-analytics-with-a-data-lakehouse.html), the overall workflow is implemented using [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) in a manner consistent with what is described above.

**NOTE** In the original solution accelerator on which this version is based, *POS 03* and *POS 04* had slightly different names.  The purpose of these notebooks between the two accelerators is the same regardless of the naming changes.

By introducing Delta Live Tables (DLT), we don't radically change the implementation of the streaming workflows. Instead, the use of DLT provides a wrapper on our workflows that enables orchestration, monitoring and many other enhancements we'd otherwise need to implement ourselves. In this regard, DLT complements Spark Structured Streaming and doesn't replace it.  If you compare the relevant notebooks between this and the previous solution accelerator, you'll be able to see how these two technologies work with each other.

That said, DLT does provide some features which can simplify the implementation of our logic.  In the original accelerator notebooks, we spent quite a bit of time describing a work-around that would enable us to perform a complex join between static and streaming objects.  DLT simplifies that aspect of this workflow and allows us to remove a bit of code from notebooks *POS 03* and *POS 04*.

The simplification of the logic is a welcome feature of DLT.  Still, DLT, as a new technology, does impose some constraints that have required us to revise some aspects of our code.  First, DLT notebooks must be self-contained, *i.e.* they cannot call other notebooks.  This has required us to copy some configuration logic from *POS 01* into *POS 03*.  Second, DLT does not yet support Azure IOT Hub Event Hub endpoints.  This has required us to reconfigure the streaming dataset reading data from the IOT Hub to leverage that service's Kafka endpoint.  Finally, notebooks defining DLT workflows are not run interactively and instead must be scheduled.  We've introduced an additional notebook, *POS 05*, to address the scheduling aspects for notebooks *POS 03* and *POS 04*.

----------------------

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.


| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| azure-iot-device                                     | Microsoft Azure IoT Device Library | MIT    | https://pypi.org/project/azure-iot-device/                       |
| azure-storage-blob                                | Microsoft Azure Blob Storage Client Library for Python| MIT        | https://pypi.org/project/azure-storage-blob/      |

To run this accelerator, clone this repo into a Databricks workspace. Attach the RUNME notebook to any cluster running a DBR 11.0 or later runtime, and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. Execute the multi-step-job to see how the pipeline runs.

The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.
