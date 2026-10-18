import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests


# ==================================================
# STEP 1: CONFIGURATION
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

SYMBOL = "AMZN"

OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==================================================
# STEP 2: LOAD API KEY
# ==================================================

def load_env():

    if ENV_FILE.exists():

        for line in ENV_FILE.read_text(
            encoding="utf-8"
        ).splitlines():

            line = line.strip()

            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)

            os.environ.setdefault(
                key.strip(),
                value.strip().strip('"').strip("'")
            )


# ==================================================
# STEP 3: FETCH AMAZON STOCK DATA
# ==================================================

def fetch_fmp_data():

    api_key = os.getenv("FMP_API_KEY")

    if not api_key:
        raise ValueError(
            "FMP_API_KEY is missing from .env"
        )

    print("Fetching Amazon (AMZN) stock data from FMP...")

    response = requests.get(
        "https://financialmodelingprep.com/stable/historical-price-eod/full",
        params={
            "symbol": SYMBOL,
            "apikey": api_key
        },
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, list) or not payload:
        raise RuntimeError(
            f"FMP returned no historical prices: {payload}"
        )

    return payload


# ==================================================
# STEP 4: SAVE RAW JSON
# ==================================================

def save_json(payload):

    now = datetime.now(timezone.utc)

    batch_id = now.strftime("%Y%m%dT%H%M%SZ")

    record = {
        "symbol": SYMBOL,
        "source": "financial_modeling_prep",
        "batch_id": batch_id,
        "ingested_at": now.isoformat(),
        "payload": {
            "historical_prices": payload
        }
    }

    filename = f"FMP_{SYMBOL}_{batch_id}.json"

    filepath = OUTPUT_DIR / filename

    with filepath.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(record, file, indent=2)

    print("JSON saved successfully!")
    print("File:", filepath)


# ==================================================
# STEP 5: MAIN
# ==================================================

def main():

    print("========== FMP INGESTION ==========")

    load_env()

    payload = fetch_fmp_data()

    save_json(payload)

    print("FMP AMAZON INGESTION COMPLETED!")


if __name__ == "__main__":
    main()
