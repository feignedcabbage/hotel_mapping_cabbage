#!/usr/bin/env python3
"""
Download {provider}_property_info for IN, US, NZ from static_db.
Output: data/raw/{country}/{provider}_property_info.ndjson
Resumable: skips files that already exist and are non-empty.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

BASE = "https://hotelmappingadmin.iweensoft.com/embedding_gateway"
API_KEY = os.environ["EMBEDDING_GATEWAY_API_KEY"]
HEADERS = {"X-API-Key": API_KEY}

COUNTRIES = ["IN", "US", "NZ"]

# Providers where country_code is numeric — filter on country_code_iso2 instead
ISO2_PROVIDERS = {"agoda", "grnc"}

# All 13 providers
PROVIDERS = [
    "agoda", "cleartrip", "expedia", "gogobal", "grnc",
    "hotelbeds", "ioxl", "ratehawk", "restel", "rezlive",
    "tbo", "tripjack", "veturis",
]

LIMIT = 10_000
OUT_DIR = Path("data/raw")


def get(path: str, params: dict | None = None) -> dict:
    url = f"{BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def download(provider: str, country: str) -> int:
    out_path = OUT_DIR / country / f"{provider}_property_info.ndjson"

    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"  SKIP  {out_path} (already exists)")
        return -1

    country_col = "country_code_iso2" if provider in ISO2_PROVIDERS else "country_code"
    where = f"{country_col}='{country}'"
    table = f"{provider}_property_info"

    total = 0
    after = None
    page = 0

    with out_path.open("w") as f:
        while True:
            params: dict = {
                "limit": LIMIT,
                "order_by": "property_code",
                "where": where,
            }
            if after is not None:
                params["after"] = after

            for attempt in range(5):
                try:
                    data = get(f"/dbs/static_db/tables/{table}/rows", params)
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    wait = 2 ** attempt
                    print(f"    retry {attempt+1}/5 after {wait}s — {e}")
                    time.sleep(wait)

            rows = data["rows"]
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

            total += len(rows)
            page += 1
            after = data.get("next_cursor")

            print(f"  {provider}/{country}  page {page}  +{len(rows)}  total={total}", flush=True)

            if not data.get("has_more"):
                break

    return total


def main():
    jobs = [(p, c) for c in COUNTRIES for p in PROVIDERS]
    done = skipped = 0

    for provider, country in jobs:
        print(f"\n→ {provider} / {country}")
        n = download(provider, country)
        if n == -1:
            skipped += 1
        else:
            print(f"  DONE  {n} rows")
            done += 1

    print(f"\nFinished. {done} downloaded, {skipped} skipped.")


if __name__ == "__main__":
    main()
