
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

SYMBOL = "TSLA"

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

        if key.strip() == "MASSIVE_API_KEY":
            os.environ.setdefault(
                key.strip(),
                value.strip().strip('"').strip("'")
            )


MASSIVE_KEY = os.getenv("MASSIVE_API_KEY")


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
            "daily_prices": payload
        }
    }

    filename = (
        f"MASSIVE_{SYMBOL}_{timestamp}.json"
    )

    filepath = OUTPUT_DIR / filename

    with filepath.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(record, file, indent=2)

    print("Saved:", filepath)


# ==========================================
# STEP 4: MASSIVE API
# ==========================================

def fetch_massive():

    if not MASSIVE_KEY:
        raise ValueError(
            "MASSIVE_API_KEY is missing."
        )

    print("\nFetching AAPL stock prices from Massive")

    end_date = date.today()
    start_date = end_date - timedelta(days=30)

    url = (
        "https://api.massive.com/v2/aggs/ticker/"
        f"{SYMBOL}/range/1/day/{start_date}/{end_date}"
    )

    response = requests.get(
        url,
        params={
            "apiKey": MASSIVE_KEY,
            "adjusted": "true",
            "sort": "asc",
            "limit": 500
        },
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("results"), list)
        or not payload["results"]
    ):
        raise RuntimeError(
            f"Invalid or empty Massive response: {payload}"
        )

    save_json("massive", payload)


# ==========================================
# STEP 5: RUN
# ==========================================

def main():

    print("========== MASSIVE INGESTION ==========")

    fetch_massive()

    print("\nMASSIVE INGESTION COMPLETED!")


if __name__ == "__main__":
    main()
