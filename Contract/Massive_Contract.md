
# Massive Historical Stock Prices Data Contract

## 📚 Table of contents

1. 👋 [Summary](#1--summary)
2. 🔍 [Dependencies](#2--dependencies)
3. 📊 [Massive Historical Stock Prices Schema](#3--massive-historical-stock-prices-schema)
   1. 🌍 [Overall View](#i--overall-view)
   2. 💾 [Table Details](#ii--table-details)
   3. 📒 [Contract Definitions](#iii--contract-definitions-ddl-and-json)

## 1. 👋 Summary

This document defines the data contract for historical
stock market prices sourced from Massive, formerly Polygon.io.

The proposed ingestion process retrieves Microsoft (MSFT)
daily aggregate stock prices and stores the mapped records
in Databricks Delta tables.

**Source:** Massive API (formerly Polygon.io)

**API Endpoint:** https://api.massive.com/v2/aggs/ticker/MSFT/range/1/day/{from}/{to}

**Stock Symbol:** MSFT

**Data Format:** JSON

**Storage Format:** Delta Lake

**Data Frequency:** Daily

## 2. 🔍 Dependencies

| Layer | Tables / Sources |
|---|---|
| External Source | Massive Aggregates API |
| Raw Data | Massive Unity Catalog Volume (to be confirmed) |
| Bronze | workspace.bronze.massive_stock_prices |
| Silver | workspace.silver.massive_stock_prices |

## 3. 📊 Massive Historical Stock Prices Schema

### I. 🌍 Overall View

| Field Name | Type | Description |
|---|---|---|
| symbol | string | Stock ticker symbol |
| trade_date | date | Date of stock trading |
| open_price | double | Opening stock price |
| high_price | double | Highest stock price |
| low_price | double | Lowest stock price |
| close_price | double | Closing stock price |
| volume | double | Trading volume |
| vwap | double | Volume-weighted average price |
| transactions | long | Number of transactions |
| aggregate_timestamp | timestamp | Aggregate window start timestamp |
| source | string | API data provider |
| batch_id | string | Ingestion batch identifier |
| ingested_at | timestamp | Original ingestion timestamp |
| bronze_loaded_at | timestamp | Bronze processing timestamp |

### II. 💾 Table Details

- **Table Name:** `workspace.bronze.massive_stock_prices`
- **Storage Format:** Managed Delta Table
- **Location:** Managed by Databricks
- **Primary Key:** None
- **Logical Unique Key:** symbol, aggregate_timestamp, source
- **Cluster Key:** None
- **Source Format:** JSON
- **Stock Symbol:** MSFT
- **Data Provider:** Massive
- **Data Frequency:** Daily

### III. 📒 Contract Definitions (DDL and JSON)

#### DDL Definition

```sql
CREATE TABLE IF NOT EXISTS workspace.bronze.massive_stock_prices (
    symbol STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume DOUBLE,
    vwap DOUBLE,
    transactions BIGINT,
    aggregate_timestamp TIMESTAMP,
    source STRING,
    batch_id STRING,
    ingested_at TIMESTAMP,
    bronze_loaded_at TIMESTAMP
)
USING DELTA;
```

#### JSON Definition

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Massive Historical Stock Prices Contract",
  "description": "Data contract for Microsoft (MSFT) daily aggregate stock prices sourced from Massive.",
  "type": "object",
  "properties": {
    "symbol": {
      "type": "string",
      "const": "MSFT",
      "description": "Stock ticker symbol"
    },
    "trade_date": {
      "type": "string",
      "format": "date",
      "description": "Trading date"
    },
    "open_price": {
      "type": "number",
      "minimum": 0
    },
    "high_price": {
      "type": "number",
      "minimum": 0
    },
    "low_price": {
      "type": "number",
      "minimum": 0
    },
    "close_price": {
      "type": "number",
      "minimum": 0
    },
    "volume": {
      "type": "number",
      "minimum": 0
    },
    "vwap": {
      "type": ["number", "null"],
      "minimum": 0
    },
    "transactions": {
      "type": ["integer", "null"],
      "minimum": 0
    },
    "aggregate_timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "source": {
      "type": "string",
      "const": "massive"
    },
    "batch_id": {
      "type": "string"
    },
    "ingested_at": {
      "type": "string",
      "format": "date-time"
    },
    "bronze_loaded_at": {
      "type": "string",
      "format": "date-time"
    }
  },
  "required": [
    "symbol",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "aggregate_timestamp",
    "source",
    "batch_id",
    "ingested_at",
    "bronze_loaded_at"
  ],
  "additionalProperties": false
}
```
