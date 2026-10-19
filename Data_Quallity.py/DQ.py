
"""
Centralized Data Quality Monitoring
Stock Market Databricks Lakehouse

Coverage:
- Bronze
- Silver
- Gold

Designed to run inside Databricks after pipeline tasks.
"""

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

spark = SparkSession.builder.getOrCreate()

CATALOG = "workspace"
LAYERS = ["bronze", "silver", "gold"]

# Set to True after confirming expected table inventory.
# Otherwise this script checks all tables it discovers.
EXPECTED_TABLES = {
    "bronze": [
        "stock_prices",
        "finnhub_quotes",
    ],
    "silver": [
        "stock_prices",
        "finnhub_quotes",
        "dim_market_assets",
        "fact_market_observations",
        "fact_market_intelligence",
    ],
    "gold": [
        "market_overview",
    ],
}

ENFORCE_EXPECTED_TABLES = False

# Use a schedule appropriate to each source before
# enabling strict freshness thresholds.
MAX_AGE_HOURS = None

NONNEGATIVE_COLUMNS = {
    "open", "high", "low", "close",
    "open_price", "high_price", "low_price",
    "close_price", "current_price",
    "volume", "adjusted_volume",
    "market_cap", "volume_24h",
    "total_assets", "total_observations",
    "positive_movements", "negative_movements",
    "total_trading_volume",
}

# These fields should never be null when present.
# Add project-specific keys after inspecting schemas.
IMPORTANT_FIELDS = {
    "observation_date",
    "trade_date",
    "symbol",
    "asset_id",
}

# Some fields can be NULL legitimately, such as
# average_interest_rate for stock records.

results = []
failures = []


def record(layer, table, check, passed, detail):
    status = "PASS" if passed else "FAIL"

    message = (
        f"{status} | {layer} | {table} | "
        f"{check} | {detail}"
    )
    print(message)

    results.append({
        "layer": layer,
        "table": table,
        "check": check,
        "status": status,
        "detail": str(detail),
    })

    if not passed:
        failures.append(message)


def list_tables(layer):
    return [
        row.tableName
        for row in spark.sql(
            f"SHOW TABLES IN {CATALOG}.{layer}"
        ).collect()
        if not row.isTemporary
    ]


def check_table(layer, table):
    full_name = f"{CATALOG}.{layer}.{table}"

    print(f"\nChecking {full_name}")

    df = spark.table(full_name)
    columns = set(df.columns)

    # 1. Nonempty table
    has_data = df.limit(1).count() > 0

    record(
        layer, table, "Nonempty",
        has_data,
        "Contains rows" if has_data else "No records",
    )

    if not has_data:
        return

    # 2. Null checks for important columns
    for column in IMPORTANT_FIELDS & columns:
        null_count = df.filter(
            F.col(column).isNull()
        ).count()

        record(
            layer, table, f"Null check: {column}",
            null_count == 0,
            f"{null_count} null rows",
        )

    # 3. Negative numeric values
    numeric_columns = {
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
    }

    for column in (
        NONNEGATIVE_COLUMNS & columns & numeric_columns
    ):
        negative_count = df.filter(
            F.col(column) < 0
        ).count()

        record(
            layer, table,
            f"Nonnegative: {column}",
            negative_count == 0,
            f"{negative_count} invalid rows",
        )

    # 4. OHLC consistency
    price_cols = {
        "open_price",
        "high_price",
        "low_price",
        "close_price",
    }

    if price_cols.issubset(columns):
        invalid_count = df.filter(
            (F.col("high_price") < F.col("low_price"))
            | (F.col("high_price") < F.col("open_price"))
            | (F.col("high_price") < F.col("close_price"))
            | (F.col("low_price") > F.col("open_price"))
            | (F.col("low_price") > F.col("close_price"))
        ).count()

        record(
            layer, table,
            "OHLC consistency",
            invalid_count == 0,
            f"{invalid_count} invalid rows",
        )

    # 5. Timestamp freshness (optional)
    if MAX_AGE_HOURS is not None:
        timestamp_column = next(
            (
                name for name in [
                    "bronze_loaded_at",
                    "ingested_at",
                    "processed_at",
                    "updated_at",
                ]
                if name in columns
            ),
            None,
        )

        if timestamp_column:
            latest = df.select(
                F.max(
                    F.col(timestamp_column).cast("timestamp")
                ).alias("latest")
            ).first()["latest"]

            if latest is None:
                record(
                    layer, table, "Freshness",
                    False, "No timestamp values",
                )
            else:
                if latest.tzinfo is None:
                    latest = latest.replace(
                        tzinfo=timezone.utc
                    )

                age = (
                    datetime.now(timezone.utc) - latest
                ).total_seconds() / 3600

                record(
                    layer, table, "Freshness",
                    age <= MAX_AGE_HOURS,
                    f"{age:.1f} hours old",
                )


def main():
    for layer in LAYERS:
        print(f"\n{'=' * 60}")
        print(f"DATA QUALITY: {layer.upper()}")
        print(f"{'=' * 60}")

        try:
            tables = list_tables(layer)
        except Exception as error:
            record(
                layer, "(schema)", "Schema access",
                False, str(error),
            )
            continue

        if not tables:
            record(
                layer, "(schema)", "Tables present",
                False, "No tables found",
            )
            continue

        record(
            layer, "(schema)", "Tables present",
            True, f"{len(tables)} tables discovered",
        )

        if ENFORCE_EXPECTED_TABLES:
            for expected in EXPECTED_TABLES.get(layer, []):
                record(
                    layer, expected, "Expected table exists",
                    expected in tables,
                    "Present" if expected in tables
                    else "Missing",
                )

        for table in tables:
            try:
                check_table(layer, table)
            except Exception as error:
                record(
                    layer, table, "Unexpected error",
                    False, str(error),
                )

    print(f"\n{'=' * 60}")
    print("FULL LAKEHOUSE DATA QUALITY SUMMARY")
    print(f"{'=' * 60}")

    passed = sum(
        r["status"] == "PASS" for r in results
    )
    failed = len(failures)

    print(f"Total checks: {len(results)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failures:
        print("\nFAILED CHECKS:")
        for failure in failures:
            print(failure)

        raise RuntimeError(
            f"Lakehouse data quality failed: "
            f"{failed} check(s)"
        )

    print("\nALL LAKEHOUSE CHECKS PASSED")


if __name__ == "__main__":
    main()
