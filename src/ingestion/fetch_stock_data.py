
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

# Read the local .env file without printing the API key.
if ENV_FILE.exists():
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("ALPHA_VANTAGE_API_KEY="):
            os.environ.setdefault(
                "ALPHA_VANTAGE_API_KEY",
                line.split("=", 1)[1].strip()
            )

api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

if not api_key or api_key == "PASTE_YOUR_API_KEY_HERE":
    raise ValueError("Set ALPHA_VANTAGE_API_KEY in your .env file.")

symbol = "AAPL"

response = requests.get(
    "https://www.alphavantage.co/query",
    params={
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
    },
    timeout=30,
)
response.raise_for_status()

payload = response.json()

if "Time Series (Daily)" not in payload:
    raise RuntimeError(
        "Alpha Vantage did not return daily prices. "
        "Check your API key, API access, or rate limit. "
        f"Response fields: {list(payload.keys())}"
    )

batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

output_dir = PROJECT_ROOT / "data" / "raw"
output_dir.mkdir(parents=True, exist_ok=True)

output_file = output_dir / f"{symbol}_{batch_id}.json"

with output_file.open("w", encoding="utf-8") as file:
    json.dump(
        {
            "symbol": symbol,
            "source": "alpha_vantage",
            "batch_id": batch_id,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        },
        file,
        indent=2,
    )

print(f"Successfully fetched {symbol} stock data.")
print(f"Saved raw data to: {output_file}")
print(f"Trading dates received: {len(payload['Time Series (Daily)'])}")
