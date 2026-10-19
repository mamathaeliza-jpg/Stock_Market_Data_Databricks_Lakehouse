
# CoinGecko Cryptocurrency Price Data Contract

## 📚 Table of contents

1. 👋 [Summary](#1--summary)
2. 🔍 [Dependencies](#2--dependencies)
3. 📊 [CoinGecko Cryptocurrency Price Schema](#3--coingecko-cryptocurrency-price-schema)
   1. 🌍 [Overall View](#i--overall-view)
   2. 💾 [Table Details](#ii--table-details)
   3. 📒 [Contract Definitions](#iii--contract-definitions-ddl-and-json)

## 1. 👋 Summary

This file defines the data contract for cryptocurrency
price data sourced from the CoinGecko API.

The proposed ingestion process retrieves Bitcoin (BTC)
and Ethereum (ETH) market prices in US dollars and stores
the structured records in Databricks Delta tables.

**Source:** CoinGecko API

**API Endpoint:** https://api.coingecko.com/api/v3/simple/price

**Cryptocurrencies:** Bitcoin (BTC), Ethereum (ETH)

**Currency:** USD

**Data Format:** JSON

**Storage Format:** Delta Lake

## 2. 🔍 Dependencies

| Layer | Tables / Sources |
|---|---|
| External Source | CoinGecko API |
| Raw Data | Proposed CoinGecko Unity Catalog Volume |
| Bronze | workspace.bronze.coingecko_prices |
| Silver | workspace.silver.coingecko_prices |

## 3. 📊 CoinGecko Cryptocurrency Price Schema

### I. 🌍 Overall View

| Field Name | Type | Description |
|---|---|---|
| coin_id | string | CoinGecko cryptocurrency identifier |
| symbol | string | Cryptocurrency ticker symbol |
| currency | string | Quote currency, USD |
| current_price | double | Latest cryptocurrency price in USD |
| market_cap | double | Cryptocurrency market capitalization |
| volume_24h | double | Total trading volume over 24 hours |
| price_change_24h | double | Price change percentage over 24 hours |
| last_updated_at | timestamp | Time of last price update |
| source | string | API data provider |
| batch_id | string | Ingestion batch identifier |
| ingested_at | timestamp | Time the API response was ingested |
| bronze_loaded_at | timestamp | Bronze processing timestamp |

### II. 💾 Table Details

- **Table Name:** `workspace.bronze.coingecko_prices`
- **Storage Format:** Managed Delta Table
- **Location:** Managed by Databricks
- **Primary Key:** None
- **Logical Unique Key:** coin_id, currency, last_updated_at
- **Cluster Key:** None
- **Source Format:** JSON
- **Data Provider:** CoinGecko
- **Cryptocurrencies:** BTC, ETH

### III. 📒 Contract Definitions (DDL and JSON)

#### DDL Definition

```sql
CREATE TABLE IF NOT EXISTS workspace.bronze.coingecko_prices (
    coin_id STRING,
    symbol STRING,
    currency STRING,
    current_price DOUBLE,
    market_cap DOUBLE,
    volume_24h DOUBLE,
    price_change_24h DOUBLE,
    last_updated_at TIMESTAMP,
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
  "title": "CoinGecko Cryptocurrency Price Contract",
  "description": "Data contract for cryptocurrency prices sourced from CoinGecko.",
  "type": "object",
  "properties": {
    "coin_id": {
      "type": "string",
      "description": "CoinGecko cryptocurrency identifier"
    },
    "symbol": {
      "type": "string",
      "description": "Cryptocurrency ticker symbol"
    },
    "currency": {
      "type": "string",
      "const": "USD"
    },
    "current_price": {
      "type": "number",
      "minimum": 0
    },
    "market_cap": {
      "type": ["number", "null"]
    },
    "volume_24h": {
      "type": ["number", "null"]
    },
    "price_change_24h": {
      "type": ["number", "null"]
    },
    "last_updated_at": {
      "type": ["string", "null"],
      "format": "date-time"
    },
    "source": {
      "type": "string",
      "const": "coingecko"
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
    "coin_id",
    "symbol",
    "currency",
    "current_price",
    "source",
    "batch_id",
    "ingested_at",
    "bronze_loaded_at"
  ],
  "additionalProperties": false
}
```
