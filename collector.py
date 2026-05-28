"""
API Latency Collector
Pings a set of public APIs at regular intervals and logs metrics to MongoDB Atlas.

Setup:
    pip install requests pymongo python-dotenv
    Create a .env file with: MONGO_URI=<your Atlas connection string>

Usage:
    python collector.py                  # runs indefinitely
    python collector.py --limit 500      # stops after 500 samples
"""

import time
import json
import argparse
import logging
import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Target APIs
# All are free, require no auth key, and return JSON.
# Vary them to sample different geographies, payload sizes, and providers.
# ---------------------------------------------------------------------------
ENDPOINTS = [
    {
        "name": "open-meteo",
        "category": "weather",
        "url": "https://api.open-meteo.com/v1/forecast",
        "params": {"latitude": 52.52, "longitude": 13.41, "current_weather": True},
        "provider": "open-meteo.com",
        "region_hint": "EU",
    },
    {
        "name": "exchangerate-usd",
        "category": "finance",
        "url": "https://open.er-api.com/v6/latest/USD",
        "params": {},
        "provider": "exchangerate-api.com",
        "region_hint": "US",
    },
    {
        "name": "ipapi",
        "category": "network",
        "url": "https://ipapi.co/json/",
        "params": {},
        "provider": "ipapi.co",
        "region_hint": "EU",
    },
    {
        "name": "rest-countries",
        "category": "reference",
        "url": "https://restcountries.com/v3.1/name/germany",
        "params": {"fields": "name,capital,population"},
        "provider": "restcountries.com",
        "region_hint": "EU",
    },
    {
        "name": "jsonplaceholder-posts",
        "category": "mock",
        "url": "https://jsonplaceholder.typicode.com/posts",
        "params": {},
        "provider": "jsonplaceholder.typicode.com",
        "region_hint": "US",
    },
    {
        "name": "catfact",
        "category": "trivial",
        "url": "https://catfact.ninja/fact",
        "params": {},
        "provider": "catfact.ninja",
        "region_hint": "US",
    },
    {
        "name": "agify",
        "category": "inference",
        "url": "https://api.agify.io/",
        "params": {"name": "michael"},
        "provider": "agify.io",
        "region_hint": "EU",
    },
    {
        "name": "dog-ceo",
        "category": "media",
        "url": "https://dog.ceo/api/breeds/list/all",
        "params": {},
        "provider": "dog.ceo",
        "region_hint": "US",
    },
]

TIMEOUT_SECONDS = 10
INTERVAL_SECONDS = 60          # cadence between full rounds
BATCH_WRITE = True             # write all results per round in one bulk insert


def ping(endpoint: dict) -> dict:
    """Ping one endpoint and return a structured metric record."""
    url = endpoint["url"]
    start = time.perf_counter()
    record = {
        "timestamp": datetime.now(timezone.utc),
        "endpoint": endpoint["name"],
        "category": endpoint["category"],
        "provider": endpoint["provider"],
        "region_hint": endpoint["region_hint"],
        "url": url,
    }
    try:
        resp = requests.get(url, params=endpoint.get("params", {}),
                            timeout=TIMEOUT_SECONDS)
        elapsed_ms = (time.perf_counter() - start) * 1000

        # DNS + TCP + TLS timing (only available if requests exposes it)
        try:
            dns_ms  = resp.elapsed.total_seconds() * 1000  # server-side only
        except Exception:
            dns_ms = None

        payload_bytes = len(resp.content)

        record.update({
            "status_code": resp.status_code,
            "success": resp.ok,
            "latency_ms": round(elapsed_ms, 2),
            "server_time_ms": round(dns_ms, 2) if dns_ms else None,
            "payload_bytes": payload_bytes,
            "content_type": resp.headers.get("content-type", ""),
            "error": None,
        })
    except requests.exceptions.Timeout:
        record.update({
            "status_code": None,
            "success": False,
            "latency_ms": TIMEOUT_SECONDS * 1000,
            "server_time_ms": None,
            "payload_bytes": 0,
            "content_type": "",
            "error": "timeout",
        })
    except requests.exceptions.RequestException as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        record.update({
            "status_code": None,
            "success": False,
            "latency_ms": round(elapsed_ms, 2),
            "server_time_ms": None,
            "payload_bytes": 0,
            "content_type": "",
            "error": str(exc)[:200],
        })

    log.info("%-30s  %s  %7.1f ms  %d B",
             record["endpoint"],
             record["status_code"] or "ERR",
             record["latency_ms"],
             record["payload_bytes"])
    return record


def run(mongo_uri: str | None, limit: int | None):
    """Main collection loop."""
    collection = None
    if mongo_uri:
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db = client["api_monitor"]
            collection = db["latency_logs"]
            log.info("Connected to MongoDB Atlas ✓")
        except ConnectionFailure as exc:
            log.warning("MongoDB unavailable (%s) — writing to local JSONL fallback.", exc)

    fallback_path = "latency_logs.jsonl"
    total = 0

    while True:
        round_records = [ping(ep) for ep in ENDPOINTS]

        if collection is not None:
            try:
                collection.insert_many(round_records, ordered=False)
            except Exception as exc:
                log.warning("Mongo insert failed: %s", exc)
                collection = None   # fall back for rest of session

        if collection is None:
            with open(fallback_path, "a") as fh:
                for r in round_records:
                    r["timestamp"] = r["timestamp"].isoformat()
                    fh.write(json.dumps(r) + "\n")

        total += len(round_records)
        log.info("Round complete. Total records: %d", total)

        if limit and total >= limit:
            log.info("Limit %d reached — stopping.", limit)
            break

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="API Latency Collector")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI"),
                        help="MongoDB Atlas connection string (or set MONGO_URI env var)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N total records (default: run forever)")
    parser.add_argument("--interval", type=int, default=INTERVAL_SECONDS,
                        help="Seconds between rounds (default: 60)")
    args = parser.parse_args()

    INTERVAL_SECONDS = args.interval
    run(mongo_uri=args.mongo_uri, limit=args.limit)
