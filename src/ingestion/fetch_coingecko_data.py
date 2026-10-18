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

COINS = "bitcoin,ethereum"

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

        if key.strip() == "COINGECKO_API_KEY":
            os.environ.setdefault(
                key.strip(),
                value.strip().strip('"').strip("'")
            )


COINGECKO_KEY = os.getenv("COINGECKO_API_KEY")


# ==========================================
# STEP 3: SAVE JSON
# ==========================================

def save_json(source, payload):

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    record = {
        "source": source,
        "batch_id": timestamp,
        "ingested_at": now.isoformat(),
        "currency": "usd",
        "payload": {
            "markets": payload
        }
    }

    filename = (
        f"COINGECKO_MARKETS_{timestamp}.json"
    )

    filepath = OUTPUT_DIR / filename

    with filepath.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(record, file, indent=2)

    print("Saved:", filepath)


# ==========================================
# STEP 4: COINGECKO API
# ==========================================

def fetch_coingecko():

    if not COINGECKO_KEY:
        raise ValueError(
            "COINGECKO_API_KEY is missing."
        )

    print("\nFetching BTC and ETH from CoinGecko")

    response = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        headers={
            "x-cg-demo-api-key": COINGECKO_KEY
        },
        params={
            "vs_currency": "usd",
            "ids": COINS,
            "order": "market_cap_desc",
            "per_page": 2,
            "page": 1,
            "sparkline": "false"
        },
        timeout=30
    )

    response.raise_for_status()
    payload = response.json()

    if (
        not isinstance(payload, list)
        or not payload
    ):
        raise RuntimeError(
            f"Invalid CoinGecko response: {payload}"
        )

    save_json("coingecko", payload)


# ==========================================
# STEP 5: RUN
# ==========================================

def main():

    print("========== COINGECKO INGESTION ==========")

    fetch_coingecko()

    print("\nCOINGECKO INGESTION COMPLETED!")


if __name__ == "__main__":
    main()
