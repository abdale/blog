---
layout: post
title:  "Processing JSON with Spark Streaming in Microsoft Fabric"
date:   2025-02-07 19:18:46 -0500
categories: update
---
Spark Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine, which allows users to process real-time data streams using the same high-level APIs as batch processing. When working with streaming ingestion of complex JSON datasets, using notebooks in Microsoft Fabric allows for leveraging the rich Python ecosystem and also uses the power of Apache Spark to efficiently handle massive JSON datasets in a distributed compute environment.

# Scenario
In this scenario, you will learn how to ingest streaming JSON from Azure Event Hubs into a Microsoft Fabric Lakehouse using the power of Spark Structured Streaming.

# Working with JSON

JSON can be processed very efficiently with Spark Streaming but the variability in its structure can introduce significant challenges that impact performance and the overall viability of streaming ETL flows. Not all JSON datasets are created equal, a JSON dataset can be as simple as containing the columns as key-value pairs inside curly brackets wrapped around an array or as complex as a nested array of objects representing columns. 

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

## Sample JSON

{% highlight ruby %}
[
  {
    "operation": "INSERT",
    "columns": [
      {
        "make": "Ford"
      },
      {
        "model": "Mustang"
      },
      {
        "vehicleType": 1
      },
      {
        "vehicleWeight": 0
      },
      {
        "state": "TX"
      },
      {
        "tollAmount": 4
      },
      {
        "tag": 634148038
      },
      {
        "tollId": 2
      },
      {
        "licensePlate": "ZSY 7044"
      }
    ],
    "entryTime": "2023-05-09T04:49:15.0189703Z",
    "eventProcessedUtcTime": "2023-05-09T04:52:54.3513112Z",
    "partitionId": 0,
    "eventEnqueuedUtcTime": "2023-05-09T04:49:16.0750000Z"
  }
]
{% endhighlight %}

For our scenario, we will consider a JSON which contains most of its columns as an array of objects. The `columns` array contains `make`, `model`, `vehicleType`, `vehicleWeight`, `state`, `tollAmount`, `tag`, `tollId`, and `licensePlate`.
 In addition to these nested columns, the `operation`, `entryTime`, `eventProcessedUtcTime`, `partitionId` and `eventEnqueuedUtcTime` columns can also be included in the target dataset.


# Setting up a stream with Azure Event Hubs

First, we will create an Azure Event Hubs resource inside of Azure. 

# Ingesting streaming data into the Fabric Lakehouse

# Ingesting streaming data into Fabric SQL Database