
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

SYMBOL = "NFLX"

OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================
# STEP 2: READ API KEY
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

        if key.strip() == "MARKETSTACK_API_KEY":
            os.environ.setdefault(
                key.strip(),
                value.strip().strip('"').strip("'")
            )


MARKETSTACK_KEY = os.getenv("MARKETSTACK_API_KEY")


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
        "payload": {
            "historical_prices": payload
        }
    }

    filename = (
        f"MARKETSTACK_{SYMBOL}_{timestamp}.json"
    )

    filepath = OUTPUT_DIR / filename

    with filepath.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(record, file, indent=2)

    print("Saved:", filepath)


# ==========================================
# STEP 4: MARKETSTACK API
# ==========================================

def fetch_marketstack():

    if not MARKETSTACK_KEY:
        raise ValueError(
            "MARKETSTACK_API_KEY is missing."
        )

    print("\nFetching NFLX stock prices from Marketstack")

    response = requests.get(
        "https://api.marketstack.com/v1/eod",
        params={
            "access_key": MARKETSTACK_KEY,
            "symbols": SYMBOL,
            "limit": 100
        },
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("data"), list)
        or not payload["data"]
    ):
        raise RuntimeError(
            f"Invalid or empty Marketstack response: {payload}"
        )

    save_json("marketstack", payload)


# ==========================================
# STEP 5: RUN
# ==========================================

def main():

    print("========== MARKETSTACK INGESTION ==========")

    fetch_marketstack()

    print("\nMARKETSTACK INGESTION COMPLETED!")


if __name__ == "__main__":
    main()
