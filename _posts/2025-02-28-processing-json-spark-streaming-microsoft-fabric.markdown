---
layout: post
title:  "Processing JSON with Spark Streaming in Microsoft Fabric"
date:   2025-02-07 19:18:46 -0500
categories: tutorials 
tags: fabric spark-streaming
---
[Spark Structured Streaming](https://spark.apache.org/streaming/) is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine, which allows users to process real-time data streams using the same high-level APIs as batch processing. When working with streaming ingestion of complex JSON datasets, using notebooks in Microsoft Fabric allows for leveraging the rich Python ecosystem and also uses the power of Apache Spark to efficiently handle massive JSON datasets in a distributed compute environment.

In this blog, we will walk through a scenario to harness Fabric Spark for processing a stream of nested JSON which will then be loaded as a Delta parquet table in the Fabric Lakehouse.

### Working with nested JSON

<div style="text-align: center;">
  <img src="\assets\json.png" alt="Alt text" width="200">
</div>

JSON can be processed very efficiently with Spark Streaming but the variability in the JSON's structure can introduce significant challenges that impact performance and the overall viability of streaming ETL flows. The flexibility of JSON, allowing for nested structures and varying schemas, can complicate processing. Nested arrays and objects require additional steps like flattening and denormalization to be analyzed effectively. This can introduce computational overhead and impact performance which can slow down the ETL pipeline. Also, unlike tabular formats, JSON does not enforce a fixed schema. This flexibility can lead to challenges in schema inference and validation, making it harder to ensure data quality and consistency.

A simple JSON object with a **flat structure** where each record is a JSON object with key-value pairs representing columns poses less of a challenge when processing given its straight-forward structrue. A JSON containing an **array of objects** with columns represented as objects inside an array, for example, poses more of a challenge in terms of parsing it.

The more nesting inside a JSON dataset, the more complexity it introduces which can lead to more latency in a real-time ingestion flow. No JSON is too complex to handle though provided that you utilize the rich set of JSON functions in Spark SQL for parsing the JSON efficiently and accurately.

For our scenario, we will work with a sample JSON dataset in which the majority of columns are represented in an array of objects:

{% highlight ruby %}
[
  {
      "columns": [
          {"operation": "INSERT"},
          {"make": "Ford"},
          {"model": "Mustang"},
          {"vehicleType": 1},
          {"state": "TX"},
          {"tollAmount": 4},
          {"tag": 634148038},
          {"licensePlate": "ZSY 7044"}
      ],
      "entryTime": "2023-05-09T04:49:15.0189703Z",
      "eventProcessedUtcTime": "2023-05-09T04:52:54.3513112Z"
  }
]
{% endhighlight %}

## Scenario

![Architecture](\assets\Architecture-JSON-Spark-Streaming-Fabric.jpg)

In this scenario, you will learn how to ingest and parse streaming JSON from Azure Event Hubs into a Microsoft Fabric Lakehouse using the power of Spark Structured Streaming.

Follow the steps below to implement this scenario:

1. [Set up a stream with Azure Event Hubs](#set-up-a-stream-with-azure-event-hubs)
2. [Ingest into the Fabric Lakehouse with Spark Structured Streaming](#ingest-into-the-fabric-lakehouse-with-spark-structured-streaming)

### Set up a stream with Azure Event Hubs

Before we parse our JSON, we need to ingest it. In order to do that, we will setup Azure Event Hubs and begin receiving streaming JSON using the [Events Hubs Data Explorer](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-data-explorer) which offers a great way for debugging and reviewing data in Event Hubs with minimal effort. Alternatively, use the python script below to send events from your local machine.

#### Send events using Event Hubs Data Explorer

1. [Create an Azure Event Hubs namespace and event hub](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-create) in Azure.
2. Use the [Event Hubs Data Explorer to send events.](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-data-explorer#use-the-event-hubs-data-explorer)
   ![Event Hubs Data Explorer](\assets\eventhubs-data-explorer-send-events.png)
3. To send events with a custom payload, select the `Custom payload` dataset and select `JSON` as the `Content-Type`. Enter the sample JSON (above) as the payload and check the **Repeat send** box, and specify the **Repeat send count** and the interval between each payload. This will ensure you have a steady stream of events to work with.
   ![Send events from Data Explorer](\assets\eventhubs-send-events-custom-payload.png)

#### Send events from local machine

Alternatively, instead of using Azure Event Hubs Data Explorer to simulate a stream, you can send send events directly from your local machine by using a script.

1. Download `send_json.py` from the [GitHub repository](https://github.com/abdale/send-json-to-eventhubs).
2. Download the sample JSON `sample.json`.
3. Replace the **connection string**, **event hub name** and **json file path** in `send_json.py` before running it.
4. Modify the `max_sends` and `time.sleep(1)` (currently set to 1s) in `send_json.py` as desired.
5. Run `send_json.py` locally to send events to an event hub. Make sure you have [Python installed](https://www.python.org/downloads/) on your machine beforehand.

### Ingest into the Fabric Lakehouse with Spark Structured Streaming

1 - After setting up our Azure Event Hubs streaming, we are ready to ingest it into the Fabric Lakehouse. In a **Fabric PySpark notebook**, setup the connection string containing the Event Hubs namespace and shared access key.

{% highlight ruby %}
connectionString = "Endpoint=sb://<EVENT_HUB_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=<SHARED_ACCESS_KEY_NAME>;SharedAccessKey=<SHARED_ACCESS_KEY>;EntityPath=<EVENT_HUB_NAME>"
ehConf['eventhubs.connectionString'] = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
{% endhighlight %}

> Note: As a security best practice, it is recommended to keep your shared access key in Azure Key Vault. Use [Credentials utilities](https://learn.microsoft.com/en-ca/fabric/data-engineering/notebook-utilities#credentials-utilities) to access Azure Key Vault secrets in a Fabric notebook.

2 - Next, import the necassary libraries required to perform transformations on your dataset:

{% highlight ruby %}
import pyspark.sql.functions as f
from pyspark.sql.functions import col, explode, expr, first
from pyspark.sql.types import *
{% endhighlight %}

3   Read stream from Event Hubs in a DataFrame:

{% highlight ruby %}
df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()
{% endhighlight %}

5 - Select and cast the body column from the DataFrame as a string to standardize the downstream processing:

{% highlight ruby %}
raw_data = df.selectExpr("CAST(body AS STRING) as message")
{% endhighlight %}

6 - Process the JSON from the DataFrame, add a unique ID, structure the data, and write it to a Delta table:

{% highlight ruby %}
def process_json(df, epoch_id):
    messages = df.collect()
    for message in messages:
        raw_message = message["message"]
        json_df = spark.read.json(spark.sparkContext.parallelize([raw_message]))
        
        # Add a unique ID column using uuid
        json_df = json_df.withColumn("id", expr("uuid()"))
        
        # Explode the JSON column
        exploded_df = json_df.withColumn("json_col", explode(col("columns")))
        
        # Select the necessary columns dynamically
        columns = exploded_df.select("json_col.*").columns
        selected_df = exploded_df.select("id", "json_col.*")
        
        # Aggregate the columns to combine fields into a single row
        aggregated_df = selected_df.groupBy("id").agg(
            *[first(c, ignorenulls=True).alias(c) for c in columns]
        )
        
        # Cast all columns to string
        final_df = aggregated_df.select([col(c).cast(StringType()).alias(c) for c in aggregated_df.columns if c != "id"])
        
        # Write to Delta table
        final_df.write.format("delta").mode("append").saveAsTable("events")
{% endhighlight %}

This function processes incoming JSON data with an array structure, flattening and normalizing it into a tabular format. It addresses common issues like null values and varying data types to ensure data integrity for further processing. The function avoids the need to define a schema beforehand, sidestepping schema mismatches often encountered with the `from_json` function, for example. However, it uses the `collect()` method, which centralizes data on the driver node, which could lead to scalability challenges.

6 - Start streaming query to process incoming data in batches using the `process_json` function with checkpoints for fault tolerance:

{% highlight ruby %}
query = raw_data \
  .writeStream \
  .foreachBatch(process_json) \
  .option("checkpointLocation", "Files/checkpoint") \
  .trigger(processingTime='5 seconds') \
  .start()

# Await termination
query.awaitTermination()
{% endhighlight %}


> **Note**: You can find the Notebook to execute the steps above in Microsoft Fabric [here](/assets/process-json-into-fabric-lakehouse.ipynb). Simply download and import it into your Fabric workspace to get started.