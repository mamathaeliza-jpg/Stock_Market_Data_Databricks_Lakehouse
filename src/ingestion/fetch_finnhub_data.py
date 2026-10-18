
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests


# ==========================================
# STEP 1: CONFIGURATION
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

SYMBOL = "MSFT"

OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================
# STEP 2: READ API KEYS
# ==========================================

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

        if key.strip() in (
            "ALPHA_VANTAGE_API_KEY",
            "FINNHUB_API_KEY"
        ):
            os.environ.setdefault(
                key.strip(),
                value.strip().strip('"').strip("'")
            )


ALPHA_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")


# ==========================================
# STEP 3: SAVE JSON
# ==========================================

def save_json(source, payload):

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    record = {
        "symbol": SYMBOL,
        "source": source,
        "batch_id": timestamp,
        "ingested_at": now.isoformat(),
        "payload": payload
    }

    filename = (
        f"{source.upper()}_{SYMBOL}_{timestamp}.json"
    )

    filepath = OUTPUT_DIR / filename

    with filepath.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(record, file, indent=2)

    print("Saved:", filepath)


# ==========================================
# STEP 4: ALPHA VANTAGE
# ==========================================

def fetch_alpha_vantage():

    if not ALPHA_KEY:
        raise ValueError(
            "ALPHA_VANTAGE_API_KEY is missing."
        )

    print("\nFetching Microsoft from Alpha Vantage")

    response = requests.get(
        "https://www.alphavantage.co/query",
        params={
            "function": "TIME_SERIES_DAILY",
            "symbol": SYMBOL,
            "apikey": ALPHA_KEY
        },
        timeout=30
    )

    response.raise_for_status()
    payload = response.json()

    if "Time Series (Daily)" not in payload:
        raise RuntimeError(
            "Alpha Vantage did not return daily prices. "
            f"Response fields: {list(payload.keys())}"
        )

    save_json("alpha_vantage", payload)


# ==========================================
# STEP 5: FINNHUB
# ==========================================

def fetch_finnhub():

    if not FINNHUB_KEY:
        raise ValueError(
            "FINNHUB_API_KEY is missing."
        )

    print("\nFetching Microsoft from Finnhub")

    response = requests.get(
        "https://finnhub.io/api/v1/quote",
        params={
            "symbol": SYMBOL,
            "token": FINNHUB_KEY
        },
        timeout=30
    )

    response.raise_for_status()
    payload = response.json()

    if (
        not isinstance(payload, dict)
        or not payload.get("t")
        or not payload.get("c")
    ):
        raise RuntimeError(
            f"Invalid Finnhub response: {payload}"
        )

    save_json("finnhub", payload)


# ==========================================
# STEP 6: RUN
# ==========================================

def main():

    print("========== MICROSOFT INGESTION ==========")

    fetch_alpha_vantage()
    fetch_finnhub()

    print("\nMICROSOFT INGESTION COMPLETED!")


if __name__ == "__main__":
    main()
