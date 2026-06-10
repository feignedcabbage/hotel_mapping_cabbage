"""Download {provider}_property_info NDJSON from the embedding-gateway static_db.

Output: data/raw/{country}/{provider}_property_info.ndjson
Resumable: skips files that already exist and are non-empty.

Needs EMBEDDING_GATEWAY_API_KEY in the environment.
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://hotelmappingadmin.iweensoft.com/embedding_gateway"
REPO_ROOT = Path(__file__).resolve().parents[1]
_KEY_NAMES = ("DB_API_KEY", "EMBEDDING_GATEWAY_API_KEY")

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


def gateway_api_key() -> str | None:
    """Gateway key from the environment or the repo-root .env file.

    Accepted names: DB_API_KEY or EMBEDDING_GATEWAY_API_KEY.
    """
    for name in _KEY_NAMES:
        if os.environ.get(name):
            return os.environ[name]
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            if name.strip() in _KEY_NAMES:
                value = value.strip().strip("'\"")
                if value:
                    return value
    return None


def _headers() -> dict:
    key = gateway_api_key()
    if not key:
        raise SystemExit(
            "no gateway key — set DB_API_KEY in .env (repo root) or export "
            "EMBEDDING_GATEWAY_API_KEY, or run with --skip-download"
        )
    return {"X-API-Key": key}


def get(path: str, params: dict | None = None) -> dict:
    url = f"{BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=_headers())
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def download(provider: str, country: str) -> int:
    """Download one provider/country table. Returns row count, -1 if skipped."""
    out_path = OUT_DIR / country / f"{provider}_property_info.ndjson"
    out_path.parent.mkdir(parents=True, exist_ok=True)

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


def download_country(country: str) -> dict:
    """Download all providers for one country. Returns {provider: rows}."""
    counts: dict[str, int] = {}
    for provider in PROVIDERS:
        print(f"\n→ {provider} / {country}")
        n = download(provider, country)
        counts[provider] = n
        if n >= 0:
            print(f"  DONE  {n} rows")
    return counts


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Download provider property info NDJSON")
    p.add_argument("--country", action="append", default=None,
                   help="ISO2 country (repeatable); default IN, US, NZ")
    args = p.parse_args(argv)
    for country in args.country or ["IN", "US", "NZ"]:
        download_country(country.upper())
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
