---
layout: post
title:  "Processing JSON with Spark Streaming in Microsoft Fabric"
date:   2025-02-07 19:18:46 -0500
categories: update
---
Spark Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine, which allows users to process real-time data streams using the same high-level APIs as batch processing. When working with streaming ingestion of complex JSON datasets, using notebooks in Microsoft Fabric allows for leveraging the rich Python ecosystem and also uses the power of Apache Spark to efficiently handle massive JSON datasets in a distributed compute environment.

## Working with JSON

JSON can be processed very efficiently with **Spark** Streaming but the variability in tje JSON's structure can introduce significant challenges that impact performance and the overall viability of streaming ETL flows. Not all JSON datasets are created equal though, a JSON dataset can be as simple as containing the columns as key-value pairs inside curly brackets wrapped around an array or as complex as a nested array of objects representing columns.

The flexibility of JSON, allowing for nested structures and varying schemas, can complicate processing. Nested arrays and objects require additional steps like flattening and denormalization to be analyzed effectively. This can introduce computational overhead and impact performance. In streaming ETL flows, the variability in JSON structure can affect the consistency and speed of data processing. Complex JSON structures may require more extensive parsing and transformation, which can slow down the ETL pipeline. Also, unlike tabular formats, JSON does not enforce a fixed schema. This flexibility can lead to challenges in schema inference and validation, making it harder to ensure data quality and consistency.

Before we dive into our scenario, let's understand JSON complexity further with a few examples.

Simple **JSON object with a flat structure** where each record is a JSON object with key-value pairs representing columns:

{% highlight ruby %}
[
  {"orderId": "12345", "customerName": "Michael", "totalAmount": 150.75},
  {"orderId": "12346", "customerName": "Sarah", "totalAmount": 200.00}
]
{% endhighlight %}

**Nested objects** with columns nested within other objects to represent hierarchical data:

{% highlight ruby %}
[
  {
    "orderId": "12345",
    "customerName": "Michael",
    "shippingAddress": {
      "street": "123 Maple St",
      "city": "Toronto",
      "postalCode": "M5H 2N2"
    }
  },
  {
    "orderId": "12346",
    "customerName": "Sarah",
    "shippingAddress": {
      "street": "456 Oak St",
      "city": "Vancouver",
      "postalCode": "V5K 0A1"
    }
  }
]
{% endhighlight %}

**Array of objects** with columns represented as objects inside an array:

{% highlight ruby %}
[
  {
    "orderId": "12345",
    "customerName": "Michael",
    "items": [
      {"itemName": "Laptop", "quantity": 1, "price": 1000.00},
      {"itemName": "Mouse", "quantity": 2, "price": 25.00}
    ]
  },
  {
    "orderId": "12346",
    "customerName": "Sarah",
    "items": [
      {"itemName": "Keyboard", "quantity": 1, "price": 50.00}
    ]
  }
]
{% endhighlight %}

The more nesting inside a JSON dataset, the more complexity it introduces, and introduces more latency in a real-time or near real-time ingestion flow. No JSON is too complex to handle though provided that you utilize the rich set of JSON functions in Spark SQL for parsing the JSON efficiently and accurately.

For our scenario, we will work with a JSON dataset in which the majority of columns are represented in an **array of objects**.

## Scenario

In this scenario, you will learn how to ingest and parse streaming JSON from Azure Event Hubs into a Microsoft Fabric Lakehouse using the power of Spark Structured Streaming.

Follow the steps below to implement this scenario:

1. Analyze the [sample JSON payload](#sample-json-payload)
1. [Set up a stream with Azure Event Hubs](#set-up-a-stream-with-azure-event-hubs)
1. [Ingest into the Fabric Lakehouse with Spark Structured Streaming]()

### Sample JSON payload

Let's start with a JSON which contains most of its columns as an array of objects.

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

## Set up a stream with Azure Event Hubs

Before we parse our JSON, we need to ingest it. In order to do that, we will setup Azure Event Hubs and begin receiving streaming JSON using the [Events Hubs Data Explorer](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-data-explorer) which offers a great way for debugging and reviewing data in Event Hubs with minimal effort. Alternatively, use the python script below to send events from your local machine.

### Send events using Event Hubs Data Explorer

1. [Create an Azure Event Hubs namespace and event hub](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-create) in Azure.
1. Use the [Event Hubs Data Explorer](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-data-explorer#use-the-event-hubs-data-explorer) to [send events with a custom payload](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-data-explorer#sending-custom-payload) by selecting the `Custom payload` dataset and selecting `JSON` as the `Content-Type`.
1. Enter the sample JSON (above) as the payload and check the **Repeat send** box, and specify the **Repeat send count** and the interval between each payload. This will ensure you have a steady stream of events to work with.

### Send events from local machine

{% highlight ruby %}
import time
import json
from azure.eventhub import EventHubProducerClient, EventData

# Replace with your EventHub connection string and name
CONNECTION_STR = 'Endpoint=sb://<eventhub_namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<shared_access_key>'
EVENTHUB_NAME = 'your_eventhub_name'

def send_json_to_eventhub(file_path):
    try:
        producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(json_data)))
            producer.send_batch(event_data_batch)
        producer.close()
        print("Data sent to EventHub successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    json_file_path = 'your_json_file_path'
    max_sends = 500  # Set the number of sends before termination
    send_count = 0

    while send_count < max_sends:
        send_json_to_eventhub(json_file_path)
        send_count += 1
        time.sleep(1)  # Wait for 1 second

    print(f"Terminated after {max_sends} sends.")
{% endhighlight %}

1. Copy the code snippet above in a file called `send.py`.
1. Copy the sample JSON in a file called `sample.json`.
1. Replace the connection string, event hub name and json file path in the script before running it.
1. Modify the `max_sends` and `time.sleep(1)` (currently set to 1s) in the script as desired.  
1. Run `send.py` locally to send events to an event hub. Make sure you have [Python installed](https://www.python.org/downloads/) on your machine.

## Ingest into the Fabric Lakehouse with Spark Structured Streaming

### 1 - Setup

In a Fabric PySpark notebook, setup the connection string containing the Event Hubs namespace and shared access key, and encrypt it.

{% highlight ruby %}
connectionString = "Endpoint=sb://<EVENT_HUB_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=<SHARED_ACCESS_KEY_NAME>;SharedAccessKey=<SHARED_ACCESS_KEY>;EntityPath=<EVENT_HUB_NAME>"

ehConf['eventhubs.connectionString'] = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
{% endhighlight %}

> Note about managing secrets in Azure Key Vault

### 2 - Import the necassary libraries

{% highlight ruby %}
import pyspark.sql.functions as f
from pyspark.sql.functions import col, explode, expr, first
from pyspark.sql.types import *
{% endhighlight %}

### 3 - Define the JSON schema

{% highlight ruby %}
# Define the schema for the columns array
columns_schema = ArrayType(StructType([
    StructField("operation", StringType(), True),
    StructField("make", StringType(), True),
    StructField("model", StringType(), True),
    StructField("vehicleType", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("tollAmount", IntegerType(), True),
    StructField("tag", LongType(), True),
    StructField("licensePlate", StringType(), True)
]))

# Define the schema for the main JSON structure
schema = StructType([
    StructField("columns", columns_schema, True),
    StructField("entryTime", TimestampType(), True),
    StructField("eventProcessedUtcTime", TimestampType(), True)
])
{% endhighlight %}

This is based on our sample JSON above. Since the columns in our JSON payload are structured as an array of objects, the above schema definition for the columns is an `ArrayType`. You may have a different JSON structure. It is important to properly define the schema to avoid complications.

### 4 - Read stream from Event Hubs

{% highlight ruby %}
df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

raw_data = df.selectExpr("CAST(body AS STRING) as message")
{% endhighlight %}

### 5 - Process the JSON

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

Process each batch of data, flatten the JSON structure, cast all columns to strings, and write the data to a Delta table.

Steps:

- Collect Messages: Collect messages from the DataFrame.
- Parse JSON: Parse the JSON data.
- Add Unique ID: Add a unique ID column using uuid.
- Explode JSON: Explode the "columns" array.
- Select Columns: Select the necessary columns dynamically.
- Aggregate Columns: Aggregate the columns to combine fields into a single row.
- Cast to String: Cast all columns to strings.
- Write to Delta Table: Write the processed data to a Delta table named events9.

### 6 - Start streaming

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