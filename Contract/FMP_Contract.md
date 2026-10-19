
# FMP Historical Stock Prices Data Contract

## 📚 Table of contents

1. 👋 [Summary](#1--summary)
2. 🔍 [Dependencies](#2--dependencies)
3. 📊 [FMP Historical Stock Prices Schema](#3--fmp-historical-stock-prices-schema)
   1. 🌍 [Overall View](#i--overall-view)
   2. 💾 [Table Details](#ii--table-details)
   3. 📒 [Contract Definitions](#iii--contract-definitions-ddl-and-json)

## 1. 👋 Summary

This file defines the data contract for historical
stock market prices sourced from the
Financial Modeling Prep (FMP) API.

The proposed ingestion process retrieves Microsoft (MSFT)
daily stock prices and stores the structured records
in Databricks Delta tables.

**Source:** Financial Modeling Prep (FMP) API

**API Endpoint:** https://financialmodelingprep.com/stable/historical-price-eod/full

**Stock Symbol:** MSFT

**Data Format:** JSON

**Storage Format:** Delta Lake

**Data Frequency:** Daily

## 2. 🔍 Dependencies

| Layer | Tables / Sources |
|---|---|
| External Source | Financial Modeling Prep API |
| Raw Data | Proposed FMP Unity Catalog Volume |
| Bronze | workspace.bronze.fmp_stock_prices |
| Silver | workspace.silver.fmp_stock_prices |

## 3. 📊 FMP Historical Stock Prices Schema

### I. 🌍 Overall View

| Field Name | Type | Description |
|---|---|---|
| symbol | string | Stock ticker symbol |
| trade_date | date | Date of stock trading |
| open_price | double | Opening stock price |
| high_price | double | Highest stock price |
| low_price | double | Lowest stock price |
| close_price | double | Closing stock price |
| volume | long | Number of shares traded |
| change | double | Absolute price change |
| change_percent | double | Percentage price change |
| vwap | double | Volume-weighted average price |
| source | string | API data provider |
| batch_id | string | Ingestion batch identifier |
| ingested_at | timestamp | Original ingestion timestamp |
| bronze_loaded_at | timestamp | Bronze processing timestamp |

### II. 💾 Table Details

- **Table Name:** `workspace.bronze.fmp_stock_prices`
- **Storage Format:** Managed Delta Table
- **Location:** Managed by Databricks
- **Primary Key:** None
- **Logical Unique Key:** symbol, trade_date, source
- **Cluster Key:** None
- **Source Format:** JSON
- **Stock Symbol:** MSFT
- **Data Provider:** Financial Modeling Prep
- **Data Frequency:** Daily

### III. 📒 Contract Definitions (DDL and JSON)

#### DDL Definition

```sql
CREATE TABLE IF NOT EXISTS workspace.bronze.fmp_stock_prices (
    symbol STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT,
    change DOUBLE,
    change_percent DOUBLE,
    vwap DOUBLE,
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
  "title": "FMP Historical Stock Prices Contract",
  "description": "Data contract for Microsoft (MSFT) daily historical stock prices sourced from Financial Modeling Prep.",
  "type": "object",
  "properties": {
    "symbol": {
      "type": "string",
      "description": "Stock ticker symbol"
    },
    "trade_date": {
      "type": "string",
      "format": "date",
      "description": "Trading date"
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
    "volume": {
      "type": "integer",
      "minimum": 0,
      "description": "Number of shares traded"
    },
    "change": {
      "type": ["number", "null"],
      "description": "Absolute stock price change"
    },
    "change_percent": {
      "type": ["number", "null"],
      "description": "Percentage stock price change"
    },
    "vwap": {
      "type": ["number", "null"],
      "minimum": 0,
      "description": "Volume-weighted average price"
    },
    "source": {
      "type": "string",
      "const": "fmp",
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
