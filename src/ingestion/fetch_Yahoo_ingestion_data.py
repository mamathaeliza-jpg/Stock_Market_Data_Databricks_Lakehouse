
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

SYMBOL = "NVDA"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"


def load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)

            os.environ.setdefault(
                key.strip(),
                value.strip().strip('"').strip("'")
            )


def fetch_twelve_data():
    api_key = os.getenv("TWELVE_DATA_API_KEY")

    if not api_key or api_key == "paste_your_new_api_key_here":
        raise ValueError("TWELVE_DATA_API_KEY is missing from .env")

    response = requests.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": SYMBOL,
            "interval": "1day",
            "outputsize": 30,
            "apikey": api_key
        },
        timeout=30
    )

    response.raise_for_status()
    payload = response.json()

    if payload.get("status") == "error" or not payload.get("values"):
        raise RuntimeError(
            f"Twelve Data returned an error: {payload}"
        )

    return payload


def save_json(payload):
    now = datetime.now(timezone.utc)
    batch_id = now.strftime("%Y%m%dT%H%M%SZ")

    record = {
        "symbol": SYMBOL,
        "source": "twelve_data",
        "batch_id": batch_id,
        "ingested_at": now.isoformat(),
        "payload": payload
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    filepath = OUTPUT_DIR / f"TWELVE_DATA_{SYMBOL}_{batch_id}.json"

    with filepath.open("w", encoding="utf-8") as file:
        json.dump(record, file, indent=2)

    print("Saved:", filepath)


def main():
    load_env()

    print("Fetching Microsoft stock prices from Twelve Data...")

    payload = fetch_twelve_data()
    save_json(payload)

    print("Twelve Data ingestion completed!")


if __name__ == "__main__":
    main()
