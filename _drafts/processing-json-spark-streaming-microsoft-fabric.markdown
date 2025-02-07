---
layout: post
title:  "Processing JSON with Spark Streaming in Microsoft Fabric"
date:   2025-02-07 19:18:46 -0500
categories: update
---
Spark Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine, which allows users to process real-time data streams using the same high-level APIs as batch processing. Its main benefits include ease of use through familiar Spark APIs, unified batch and streaming processing, low-latency performance, and robust fault tolerance with features like checkpointing and watermarks. This makes it ideal for building complex streaming applications and pipelines efficiently.

# Understanding JSON complexity

JSON can be processed very efficiently with Spark Streaming but the variability in its structure can introduce significant challenges that impact performance and the overall viability of streaming ETL flows. Not all JSON datasets are created equal, a JSON dataset can be as simple as containing the columns as key-value pairs inside curly brackets wrapped around an array or as complex as a nested array of objects representing columns. 

The flexibility of JSON, allowing for nested structures and varying schemas, can complicate processing. Nested arrays and objects require additional steps like flattening and denormalization to be analyzed effectively. This can introduce computational overhead and impact performance. In streaming ETL flows, the variability in JSON structure can affect the consistency and speed of data processing. Complex JSON structures may require more extensive parsing and transformation, which can slow down the ETL pipeline. Also, unlike tabular formats, JSON does not enforce a fixed schema. This flexibility can lead to challenges in schema inference and validation, making it harder to ensure data quality and consistency.

Let's understand this with a few examples.

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

No JSON too complex to handle as long as you know how to parse it.

# Setting up a stream with Azure Event Hubs

# Ingesting streaming data into the Fabric Lakehouse

# Ingesting streaming data into Fabric SQL Database