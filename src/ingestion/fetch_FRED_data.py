
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

SERIES_ID = "DFF"

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

        if key.strip() == "FRED_API_KEY":
            os.environ.setdefault(
                key.strip(),
                value.strip().strip('"').strip("'")
            )


FRED_KEY = os.getenv("FRED_API_KEY")


# ==========================================
# STEP 3: SAVE JSON
# ==========================================

def save_json(source, payload):

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    record = {
        "series_id": SERIES_ID,
        "source": source,
        "batch_id": timestamp,
        "ingested_at": now.isoformat(),
        "payload": payload
    }

    filename = (
        f"{source.upper()}_{SERIES_ID}_{timestamp}.json"
    )

    filepath = OUTPUT_DIR / filename

    with filepath.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(record, file, indent=2)

    print("Saved:", filepath)


# ==========================================
# STEP 4: FRED API
# ==========================================

def fetch_fred():

    if not FRED_KEY:
        raise ValueError(
            "FRED_API_KEY is missing."
        )

    print("\nFetching Federal Funds Rate from FRED")

    response = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": SERIES_ID,
            "api_key": FRED_KEY,
            "file_type": "json",
            "observation_start": "2025-01-01",
            "sort_order": "desc",
            "limit": 500
        },
        timeout=30
    )

    response.raise_for_status()
    payload = response.json()

    if (
        not isinstance(payload, dict)
        or not isinstance(
            payload.get("observations"), list
        )
        or not payload["observations"]
    ):
        raise RuntimeError(
            f"Invalid FRED response: {payload}"
        )

    save_json("fred", payload)


# ==========================================
# STEP 5: RUN
# ==========================================

def main():

    print("========== FRED INGESTION ==========")

    fetch_fred()

    print("\nFRED INGESTION COMPLETED!")


if __name__ == "__main__":
    main()
