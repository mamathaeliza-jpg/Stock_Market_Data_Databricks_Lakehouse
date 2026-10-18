
import json
import os
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

import requests


# ==========================================
# STEP 1: CONFIGURATION
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

SYMBOL = "JPM.US"

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

        if key.strip() == "EODHD_API_KEY":
            os.environ.setdefault(
                key.strip(),
                value.strip().strip('"').strip("'")
            )


EODHD_KEY = os.getenv("EODHD_API_KEY")


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
        f"EODHD_JPM_{timestamp}.json"
    )

    filepath = OUTPUT_DIR / filename

    with filepath.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(record, file, indent=2)

    print("Saved:", filepath)


# ==========================================
# STEP 4: EODHD API
# ==========================================

def fetch_eodhd():

    if not EODHD_KEY:
        raise ValueError(
            "EODHD_API_KEY is missing."
        )

    print("\nFetching JPM stock prices from EODHD")

    end_date = date.today()
    start_date = end_date - timedelta(days=365)

    response = requests.get(
        f"https://eodhd.com/api/eod/{SYMBOL}",
        params={
            "api_token": EODHD_KEY,
            "fmt": "json",
            "period": "d",
            "from": start_date.isoformat(),
            "to": end_date.isoformat()
        },
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    if (
        not isinstance(payload, list)
        or not payload
        or not isinstance(payload[0], dict)
        or not all(
            field in payload[0]
            for field in (
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume"
            )
        )
    ):
        raise RuntimeError(
            f"Invalid or empty EODHD response: {payload}"
        )

    save_json("eodhd", payload)


# ==========================================
# STEP 5: RUN
# ==========================================

def main():

    print("========== EODHD INGESTION ==========")

    fetch_eodhd()

    print("\nEODHD INGESTION COMPLETED!")


if __name__ == "__main__":
    main()
