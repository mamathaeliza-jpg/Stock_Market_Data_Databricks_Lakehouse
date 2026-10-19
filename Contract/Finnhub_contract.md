
# Finnhub Stock Quote Data Contract

## 📚 Table of contents

1. 👋 [Summary](#1--summary)
2. 🔍 [Dependencies](#2--dependencies)
3. 📊 [Finnhub Stock Quote Data Schema](#3--finnhub-stock-quote-data-schema)
   1. 🌍 [Overall View](#i--overall-view)
   2. 💾 [Table Details](#ii--table-details)
   3. 📒 [Contract Definitions](#iii--contract-definitions-ddl-and-json)

## 1. 👋 Summary

This file defines the data contract for Finnhub stock quote
data sourced from the Finnhub Quote API.

The current implementation retrieves Microsoft (MSFT) stock
quotes and stores the processed records in Databricks Delta tables.

**Source:** Finnhub API

**API Endpoint:** https://finnhub.io/api/v1/quote

**Stock Symbol:** MSFT

**Data Format:** JSON

**Storage Format:** Delta Lake

## 2. 🔍 Dependencies

| Layer | Tables / Sources |
|---|---|
| External Source | Finnhub Quote API |
| Raw Data | workspace.default.finnhub Volume |
| Bronze | workspace.bronze.finnhub_quotes |
| Silver | workspace.silver.finnhub_quotes |

## 3. 📊 Finnhub Stock Quote Data Schema

### I. 🌍 Overall View

| Field Name | Type | Description |
|---|---|---|
| symbol | string | Stock ticker symbol |
| source | string | Data provider |
| batch_id | string | Ingestion batch identifier |
| ingested_at | string | Original ingestion timestamp |
| quote_timestamp | timestamp | Stock quote timestamp |
| current_price | double | Current stock price |
| open_price | double | Opening stock price |
| high_price | double | Highest price of the trading day |
| low_price | double | Lowest price of the trading day |
| previous_close | double | Previous closing stock price |
| price_change | double | Absolute stock price change |
| percent_change | double | Percentage stock price change |
| bronze_loaded_at | timestamp | Bronze processing timestamp |

### II. 💾 Table Details

- **Table Name:** `workspace.bronze.finnhub_quotes`
- **Storage Format:** Managed Delta Table
- **Location:** Managed by Databricks
- **Primary Key:** None
- **Logical Unique Key:** symbol, source, quote_timestamp
- **Cluster Key:** None
- **Source Format:** JSON
- **Stock Symbol:** MSFT

### III. 📒 Contract Definitions (DDL and JSON)

#### DDL Definition

```sql
CREATE TABLE IF NOT EXISTS workspace.bronze.finnhub_quotes (
    symbol STRING,
    source STRING,
    batch_id STRING,
    ingested_at STRING,
    quote_timestamp TIMESTAMP,
    current_price DOUBLE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    previous_close DOUBLE,
    price_change DOUBLE,
    percent_change DOUBLE,
    bronze_loaded_at TIMESTAMP
)
USING DELTA;
```

#### JSON Definition

```json
{
  "type": "object",
  "properties": {
    "symbol": {
      "type": "string"
    },
    "source": {
      "type": "string"
    },
    "batch_id": {
      "type": "string"
    },
    "ingested_at": {
      "type": "string",
      "format": "date-time"
    },
    "quote_timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "current_price": {
      "type": "number"
    },
    "open_price": {
      "type": "number"
    },
    "high_price": {
      "type": "number"
    },
    "low_price": {
      "type": "number"
    },
    "previous_close": {
      "type": "number"
    },
    "price_change": {
      "type": "number"
    },
    "percent_change": {
      "type": "number"
    },
    "bronze_loaded_at": {
      "type": "string",
      "format": "date-time"
    }
  },
  "required": [
    "symbol",
    "source",
    "batch_id",
    "quote_timestamp",
    "current_price"
  ]
}
```
