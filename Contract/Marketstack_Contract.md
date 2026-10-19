
# Marketstack Historical Stock Prices Data Contract

## 📚 Table of contents

1. 👋 [Summary](#1--summary)
2. 🔍 [Dependencies](#2--dependencies)
3. 📊 [Marketstack Historical Stock Prices Schema](#3--marketstack-historical-stock-prices-schema)
   1. 🌍 [Overall View](#i--overall-view)
   2. 💾 [Table Details](#ii--table-details)
   3. 📒 [Contract Definitions](#iii--contract-definitions-ddl-and-json)

## 1. 👋 Summary

This file defines the data contract for historical
end-of-day stock market prices sourced from the
Marketstack API.

The proposed ingestion process retrieves Microsoft (MSFT)
daily stock prices and stores the structured records
in Databricks Delta tables.

**Source:** Marketstack API

**API Endpoint:** https://api.marketstack.com/v1/eod

**Stock Symbol:** MSFT

**Data Format:** JSON

**Storage Format:** Delta Lake

**Data Frequency:** Daily

## 2. 🔍 Dependencies

| Layer | Tables / Sources |
|---|---|
| External Source | Marketstack End-of-Day API |
| Raw Data | Marketstack Unity Catalog Volume (to be confirmed) |
| Bronze | workspace.bronze.marketstack_stock_prices |
| Silver | workspace.silver.marketstack_stock_prices |

## 3. 📊 Marketstack Historical Stock Prices Schema

### I. 🌍 Overall View

| Field Name | Type | Description |
|---|---|---|
| symbol | string | Stock ticker symbol |
| exchange | string | Stock exchange identifier |
| trade_date | date | Date of stock trading |
| open_price | double | Opening stock price |
| high_price | double | Highest stock price |
| low_price | double | Lowest stock price |
| close_price | double | Closing stock price |
| adjusted_close | double | Adjusted closing stock price |
| volume | long | Number of shares traded |
| source | string | API data provider |
| batch_id | string | Ingestion batch identifier |
| ingested_at | timestamp | Original ingestion timestamp |
| bronze_loaded_at | timestamp | Bronze processing timestamp |

### II. 💾 Table Details

- **Table Name:** `workspace.bronze.marketstack_stock_prices`
- **Storage Format:** Managed Delta Table
- **Location:** Managed by Databricks
- **Primary Key:** None
- **Logical Unique Key:** symbol, trade_date, source
- **Cluster Key:** None
- **Source Format:** JSON
- **Stock Symbol:** MSFT
- **Data Provider:** Marketstack
- **Data Frequency:** Daily

### III. 📒 Contract Definitions (DDL and JSON)

#### DDL Definition

```sql
CREATE TABLE IF NOT EXISTS workspace.bronze.marketstack_stock_prices (
    symbol STRING,
    exchange STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    adjusted_close DOUBLE,
    volume BIGINT,
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
  "title": "Marketstack Historical Stock Prices Contract",
  "description": "Data contract for Microsoft (MSFT) end-of-day stock prices sourced from Marketstack.",
  "type": "object",
  "properties": {
    "symbol": {
      "type": "string",
      "const": "MSFT",
      "description": "Stock ticker symbol"
    },
    "exchange": {
      "type": ["string", "null"],
      "description": "Stock exchange identifier"
    },
    "trade_date": {
      "type": "string",
      "format": "date",
      "description": "Stock trading date"
    },
    "open_price": {
      "type": ["number", "null"],
      "minimum": 0,
      "description": "Opening stock price"
    },
    "high_price": {
      "type": ["number", "null"],
      "minimum": 0,
      "description": "Highest trading price"
    },
    "low_price": {
      "type": ["number", "null"],
      "minimum": 0,
      "description": "Lowest trading price"
    },
    "close_price": {
      "type": "number",
      "minimum": 0,
      "description": "Closing stock price"
    },
    "adjusted_close": {
      "type": ["number", "null"],
      "minimum": 0,
      "description": "Adjusted closing stock price"
    },
    "volume": {
      "type": ["integer", "null"],
      "minimum": 0,
      "description": "Number of shares traded"
    },
    "source": {
      "type": "string",
      "const": "marketstack",
      "description": "API data provider"
    },
    "batch_id": {
      "type": "string",
      "description": "Ingestion batch identifier"
    },
    "ingested_at": {
      "type": "string",
      "format": "date-time",
      "description": "Original ingestion timestamp"
    },
    "bronze_loaded_at": {
      "type": "string",
      "format": "date-time",
      "description": "Bronze processing timestamp"
    }
  },
  "required": [
    "symbol",
    "trade_date",
    "close_price",
    "source",
    "batch_id",
    "ingested_at",
    "bronze_loaded_at"
  ],
  "additionalProperties": false
}
```
