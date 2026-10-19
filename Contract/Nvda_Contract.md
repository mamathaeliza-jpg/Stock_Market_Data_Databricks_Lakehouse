
# NVDA Historical Stock Prices Data Contract

## 📚 Table of contents

1. 👋 [Summary](#1--summary)
2. 🔍 [Dependencies](#2--dependencies)
3. 📊 [NVDA Historical Stock Prices Schema](#3--nvda-historical-stock-prices-schema)
   1. 🌍 [Overall View](#i--overall-view)
   2. 💾 [Table Details](#ii--table-details)
   3. 📒 [Contract Definitions](#iii--contract-definitions-ddl-and-json)

## 1. 👋 Summary

This document defines the data contract for NVIDIA
Corporation (NVDA) historical daily stock market prices.

The proposed ingestion process retrieves NVIDIA stock
prices from a configured market data API and stores
the structured records in Databricks Delta tables.

**Data Provider:** To be confirmed from ingestion code

**Stock Name:** NVIDIA Corporation

**Stock Symbol:** NVDA

**Exchange:** NASDAQ

**API Endpoint:** To be confirmed

**Data Format:** JSON

**Storage Format:** Delta Lake

**Data Frequency:** Daily

## 2. 🔍 Dependencies

| Layer | Tables / Sources |
|---|---|
| External Source | Configured NVDA stock market API |
| Raw Data | NVDA Unity Catalog Volume (to be confirmed) |
| Bronze | workspace.bronze.nvda_stock_prices |
| Silver | workspace.silver.nvda_stock_prices |

## 3. 📊 NVDA Historical Stock Prices Schema

### I. 🌍 Overall View

| Field Name | Type | Description |
|---|---|---|
| symbol | string | Stock ticker symbol |
| trade_date | date | Stock trading date |
| open_price | double | Opening stock price |
| high_price | double | Highest stock price |
| low_price | double | Lowest stock price |
| close_price | double | Closing stock price |
| adjusted_close | double | Adjusted closing stock price |
| volume | long | Number of shares traded |
| source | string | Market data provider |
| batch_id | string | Ingestion batch identifier |
| ingested_at | timestamp | Original ingestion timestamp |
| bronze_loaded_at | timestamp | Bronze processing timestamp |

### II. 💾 Table Details

- **Table Name:** `workspace.bronze.nvda_stock_prices`
- **Storage Format:** Managed Delta Table
- **Location:** Managed by Databricks
- **Primary Key:** None
- **Logical Unique Key:** symbol, trade_date, source
- **Cluster Key:** None
- **Source Format:** JSON
- **Stock Symbol:** NVDA
- **Stock Exchange:** NASDAQ
- **Data Frequency:** Daily

### III. 📒 Contract Definitions (DDL and JSON)

#### DDL Definition

```sql
CREATE TABLE IF NOT EXISTS workspace.bronze.nvda_stock_prices (
    symbol STRING,
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
  "title": "NVDA Historical Stock Prices Contract",
  "description": "Data contract for NVIDIA Corporation (NVDA) daily historical stock prices.",
  "type": "object",
  "properties": {
    "symbol": {
      "type": "string",
      "const": "NVDA",
      "description": "Stock ticker symbol"
    },
    "trade_date": {
      "type": "string",
      "format": "date",
      "description": "Stock trading date"
    },
    "open_price": {
      "type": "number",
      "minimum": 0,
      "description": "Opening stock price"
    },
    "high_price": {
      "type": "number",
      "minimum": 0,
      "description": "Highest stock price"
    },
    "low_price": {
      "type": "number",
      "minimum": 0,
      "description": "Lowest stock price"
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
      "type": "integer",
      "minimum": 0,
      "description": "Number of shares traded"
    },
    "source": {
      "type": "string",
      "description": "Market data provider"
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
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "source",
    "batch_id",
    "ingested_at",
    "bronze_loaded_at"
  ],
  "additionalProperties": false
}
```
