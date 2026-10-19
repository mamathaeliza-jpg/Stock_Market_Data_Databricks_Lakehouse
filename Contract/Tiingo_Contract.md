
# Tiingo Historical Stock Prices Data Contract

## 📚 Table of contents

1. 👋 [Summary](#1--summary)
2. 🔍 [Dependencies](#2--dependencies)
3. 📊 [Tiingo Historical Stock Prices Schema](#3--tiingo-historical-stock-prices-schema)
   1. 🌍 [Overall View](#i--overall-view)
   2. 💾 [Table Details](#ii--table-details)
   3. 📒 [Contract Definitions](#iii--contract-definitions-ddl-and-json)

## 1. 👋 Summary

This document defines the data contract for historical
daily stock market prices sourced from the Tiingo API.

The proposed ingestion process retrieves Microsoft (MSFT)
stock prices and stores the structured records
in Databricks Delta tables.

**Source:** Tiingo API

**API Endpoint:** https://api.tiingo.com/tiingo/daily/MSFT/prices

**Stock Symbol:** MSFT

**Exchange:** NASDAQ

**Data Format:** JSON

**Storage Format:** Delta Lake

**Data Frequency:** Daily

## 2. 🔍 Dependencies

| Layer | Tables / Sources |
|---|---|
| External Source | Tiingo Daily Prices API |
| Raw Data | Tiingo Unity Catalog Volume (to be confirmed) |
| Bronze | workspace.bronze.tiingo_stock_prices |
| Silver | workspace.silver.tiingo_stock_prices |

## 3. 📊 Tiingo Historical Stock Prices Schema

### I. 🌍 Overall View

| Field Name | Type | Description |
|---|---|---|
| symbol | string | Stock ticker symbol |
| trade_date | date | Stock trading date |
| open_price | double | Opening stock price |
| high_price | double | Highest stock price |
| low_price | double | Lowest stock price |
| close_price | double | Closing stock price |
| volume | long | Number of shares traded |
| adjusted_open | double | Adjusted opening stock price |
| adjusted_high | double | Adjusted highest stock price |
| adjusted_low | double | Adjusted lowest stock price |
| adjusted_close | double | Adjusted closing stock price |
| adjusted_volume | long | Adjusted trading volume |
| dividend_cash | double | Cash dividend amount |
| split_factor | double | Stock split adjustment factor |
| source | string | API data provider |
| batch_id | string | Ingestion batch identifier |
| ingested_at | timestamp | Original ingestion timestamp |
| bronze_loaded_at | timestamp | Bronze processing timestamp |

### II. 💾 Table Details

- **Table Name:** `workspace.bronze.tiingo_stock_prices`
- **Storage Format:** Managed Delta Table
- **Location:** Managed by Databricks
- **Primary Key:** None
- **Logical Unique Key:** symbol, trade_date, source
- **Cluster Key:** None
- **Source Format:** JSON
- **Stock Symbol:** MSFT
- **Data Provider:** Tiingo
- **Data Frequency:** Daily

### III. 📒 Contract Definitions (DDL and JSON)

#### DDL Definition

```sql
CREATE TABLE IF NOT EXISTS workspace.bronze.tiingo_stock_prices (
    symbol STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT,
    adjusted_open DOUBLE,
    adjusted_high DOUBLE,
    adjusted_low DOUBLE,
    adjusted_close DOUBLE,
    adjusted_volume BIGINT,
    dividend_cash DOUBLE,
    split_factor DOUBLE,
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
  "title": "Tiingo Historical Stock Prices Contract",
  "description": "Data contract for Microsoft (MSFT) daily historical stock prices sourced from Tiingo.",
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
      "description": "Stock trading date"
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
      "type": "integer",
      "minimum": 0
    },
    "adjusted_open": {
      "type": ["number", "null"],
      "minimum": 0
    },
    "adjusted_high": {
      "type": ["number", "null"],
      "minimum": 0
    },
    "adjusted_low": {
      "type": ["number", "null"],
      "minimum": 0
    },
    "adjusted_close": {
      "type": ["number", "null"],
      "minimum": 0
    },
    "adjusted_volume": {
      "type": ["integer", "null"],
      "minimum": 0
    },
    "dividend_cash": {
      "type": ["number", "null"],
      "minimum": 0
    },
    "split_factor": {
      "type": ["number", "null"],
      "exclusiveMinimum": 0
    },
    "source": {
      "type": "string",
      "const": "tiingo"
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
    "source",
    "batch_id",
    "ingested_at",
    "bronze_loaded_at"
  ],
  "additionalProperties": false
}
```
