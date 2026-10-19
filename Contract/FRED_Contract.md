
# FRED Economic Data Contract

## 📚 Table of contents

1. 👋 [Summary](#1--summary)
2. 🔍 [Dependencies](#2--dependencies)
3. 📊 [FRED Economic Data Schema](#3--fred-economic-data-schema)
   1. 🌍 [Overall View](#i--overall-view)
   2. 💾 [Table Details](#ii--table-details)
   3. 📒 [Contract Definitions](#iii--contract-definitions-ddl-and-json)

## 1. 👋 Summary

This file defines the data contract for economic
time-series observations sourced from the
Federal Reserve Economic Data (FRED) API.

The proposed ingestion process retrieves the
Federal Funds Effective Rate (DFF) and stores
the structured observations in Databricks Delta tables.

**Source:** Federal Reserve Economic Data (FRED)

**API Endpoint:** https://api.stlouisfed.org/fred/series/observations

**Series ID:** DFF

**Series Name:** Federal Funds Effective Rate

**Frequency:** Daily

**Unit:** Percent

**Data Format:** JSON

**Storage Format:** Delta Lake

## 2. 🔍 Dependencies

| Layer | Tables / Sources |
|---|---|
| External Source | FRED Series Observations API |
| Raw Data | FRED Unity Catalog Volume (to be confirmed) |
| Bronze | workspace.bronze.fred_observations |
| Silver | workspace.silver.fred_observations |

## 3. 📊 FRED Economic Data Schema

### I. 🌍 Overall View

| Field Name | Type | Description |
|---|---|---|
| series_id | string | FRED economic series identifier |
| observation_date | date | Date of the economic observation |
| value | double | Economic indicator value |
| realtime_start | date | Start date of the real-time observation period |
| realtime_end | date | End date of the real-time observation period |
| units | string | Measurement unit |
| frequency | string | Observation frequency |
| source | string | API data provider |
| batch_id | string | Ingestion batch identifier |
| ingested_at | timestamp | Original ingestion timestamp |
| bronze_loaded_at | timestamp | Bronze processing timestamp |

### II. 💾 Table Details

- **Table Name:** `workspace.bronze.fred_observations`
- **Storage Format:** Managed Delta Table
- **Location:** Managed by Databricks
- **Primary Key:** None
- **Logical Unique Key:** series_id, observation_date, realtime_start, realtime_end
- **Cluster Key:** None
- **Source Format:** JSON
- **Series ID:** DFF
- **Data Provider:** FRED
- **Data Frequency:** Daily
- **Measurement Unit:** Percent

### III. 📒 Contract Definitions (DDL and JSON)

#### DDL Definition

```sql
CREATE TABLE IF NOT EXISTS workspace.bronze.fred_observations (
    series_id STRING,
    observation_date DATE,
    value DOUBLE,
    realtime_start DATE,
    realtime_end DATE,
    units STRING,
    frequency STRING,
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
  "title": "FRED Economic Data Contract",
  "description": "Data contract for Federal Funds Effective Rate (DFF) observations sourced from the FRED API.",
  "type": "object",
  "properties": {
    "series_id": {
      "type": "string",
      "const": "DFF",
      "description": "FRED economic series identifier"
    },
    "observation_date": {
      "type": "string",
      "format": "date",
      "description": "Economic observation date"
    },
    "value": {
      "type": ["number", "null"],
      "description": "Economic indicator value; null for missing observations"
    },
    "realtime_start": {
      "type": "string",
      "format": "date",
      "description": "Start date of the real-time observation period"
    },
    "realtime_end": {
      "type": "string",
      "format": "date",
      "description": "End date of the real-time observation period"
    },
    "units": {
      "type": "string",
      "const": "Percent",
      "description": "Measurement unit"
    },
    "frequency": {
      "type": "string",
      "const": "Daily",
      "description": "Observation frequency"
    },
    "source": {
      "type": "string",
      "const": "fred",
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
    "series_id",
    "observation_date",
    "value",
    "realtime_start",
    "realtime_end",
    "units",
    "frequency",
    "source",
    "batch_id",
    "ingested_at",
    "bronze_loaded_at"
  ],
  "additionalProperties": false
}
```
