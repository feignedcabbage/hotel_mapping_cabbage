"""Stage 09F — export normalize + clustering output to the hotelmap MySQL DB
via the Embedding Gateway Write API
(https://hotelmappingadmin.iweensoft.com/embedding_gateway/write).

Write logic mirrored from hotel_mapping_modified/hotelmap/export/run_export.py.
Reads normalized_hotels.parquet (stages 1-8) and cluster_members_{version}.parquet
/ cluster_diagnostics_{version}.parquet (stage 09D) for a run, and refreshes three
tables:

  - tripgain_hotel_info_v6      one row per cluster, merged from member records
  - tripgain_hotel_mappings_v6  one row per provider record (auto/review/unmatched)
  - cluster_summary_v6          one row per cluster, with supplier_breakdown/members JSON

Divergences from the mirrored original (deliberate):
  - refresh is COUNTRY-scoped (DELETE where country, not 1=1): sequential
    multi-country runs must not wipe each other's rows. Singleton ids are
    country-prefixed ("IN_GH6-<hash>") for the same reason — the bare GH6-
    ids of the original are not attributable to a country in the mappings table.
  - write key resolves like the read key: WRITE_API_KEY or
    EMBEDDING_GATEWAY_WRITE_API_KEY from the environment or repo-root .env
    (no python-dotenv dependency).
  - defaults match this repo: version v2_4, clusters under splink_v2_1/.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
BASE = "https://hotelmappingadmin.iweensoft.com/embedding_gateway/write"
_WRITE_KEY_NAMES = ("WRITE_API_KEY", "EMBEDDING_GATEWAY_WRITE_API_KEY")

BATCH_SIZE = 500

INFO_TABLE = "tripgain_hotel_info_v6"
MAPPINGS_TABLE = "tripgain_hotel_mappings_v6"
SUMMARY_TABLE = "cluster_summary_v6"

# tripgain_hotel_info_v6 column -> normalized_hotels.parquet column
INFO_COLUMNS = {
    "property_name": "property_name",
    "address_lines": "address_lines",
    "city_name": "city_name",
    "city_code": "city_code",
    "state": "state_norm",
    "country_code": "country_code_norm",
    "country_name": "country_name_clean",
    "postal_code": "postal_code_final",
    "lat": "lat_norm",
    "lng": "lng_norm",
    "geohash6": "geohash6_norm",
    "star_rating": "star_rating_norm",
    "reviewrating": "average_of_rating_norm",
    "totalreviewcount": "total_reviews",
    "number_of_rooms": "number_of_rooms_norm",
    "hotel_chain": "hotel_chain_norm",
    "property_type": "property_type_norm",
    "amenities": "amenities",
    "thumbnail": "thumbnail",
    "land_mark": "land_mark",
    "phone_numbers": "phone_numbers",
    "fax_numbers": "fax_numbers",
    "emails": "emails",
    "weburl": "weburl",
    "check_in_time": "check_in_time",
    "check_out_time": "check_out_time",
}

STATUS_TO_MAPPING_TYPE = {
    "matched_cluster": "auto",
    "review_cluster": "review",
    "singleton_unmatched": "auto",
}


def write_api_key() -> str | None:
    """Write key from the environment or the repo-root .env file."""
    for name in _WRITE_KEY_NAMES:
        if os.environ.get(name):
            return os.environ[name]
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            if name.strip() in _WRITE_KEY_NAMES:
                value = value.strip().strip("'\"")
                if value:
                    return value
    return None


def _headers() -> dict:
    return {"X-Write-API-Key": write_api_key() or "", "Content-Type": "application/json"}


def _clean(value):
    """NaN -> None (NaN is not valid JSON)."""
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _request(method: str, path: str, *, params: dict | None = None, body: dict | None = None) -> dict:
    url = f"{BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=_headers(), method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")
        raise RuntimeError(f"{method} {path} -> {e.code}: {detail}") from e


def delete_country(table: str, country: str) -> int:
    """Country-scoped refresh; cluster AND singleton ids carry the country prefix."""
    where = f"tripgain_id LIKE '{country}\\_%'"
    return _request("DELETE", f"/tables/{table}/rows", params={"where": where}).get("deleted", 0)


def insert_rows(table: str, rows: list[dict]) -> int:
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        result = _request("POST", f"/tables/{table}/rows", body={"rows": batch})
        inserted += result.get("inserted", 0)
        print(f"    {table}: {inserted}/{len(rows)} rows", flush=True)
    return inserted


def load_inputs(
    run_dir: Path, clusters_dir: Path, version: str, country: str
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    normalized = pl.read_parquet(run_dir / "normalized_hotels.parquet")
    members = pl.read_parquet(clusters_dir / f"cluster_members_{version}.parquet")
    diagnostics = pl.read_parquet(clusters_dir / f"cluster_diagnostics_{version}.parquet")

    def _singleton_id(record_id: str) -> str:
        parts = record_id.split("::")[:2]
        s = f"{parts[0]}::{parts[1]}"
        h = hashlib.md5(s.encode()).hexdigest()[:16].upper()
        return f"{country}_GH6-{h}"

    members = members.with_columns(
        pl.when(pl.col("cluster_id").is_null())
        .then(pl.col("record_id").map_elements(_singleton_id, return_dtype=pl.Utf8))
        .otherwise(pl.col("cluster_id"))
        .alias("cluster_id")
    )

    return normalized, members, diagnostics


def build_hotel_info_rows(normalized: pl.DataFrame, members: pl.DataFrame) -> list[dict]:
    src_cols = ["record_id", *sorted(set(INFO_COLUMNS.values()))]

    # Join all members with normalized to get full data
    joined_all = members.join(normalized.select(src_cols), on="record_id", how="inner")

    # Nullify empty strings and dashes so drop_nulls() skips them
    for col in set(INFO_COLUMNS.values()):
        if joined_all.schema[col] in (pl.Utf8, pl.String):
            expr = pl.when(pl.col(col).str.strip_chars().is_in(["", "-"])).then(None).otherwise(pl.col(col))
            joined_all = joined_all.with_columns(expr.alias(col))

    # Sort so representative is first in each group
    joined_all = joined_all.sort(["cluster_id", "is_representative"], descending=[False, True])

    # Aggregate by taking the first non-null value per column
    agg_exprs = [pl.col(col).drop_nulls().first().alias(col) for col in set(INFO_COLUMNS.values())]
    merged = joined_all.group_by("cluster_id").agg(agg_exprs)

    rows = []
    for r in merged.iter_rows(named=True):
        row = {"tripgain_id": r["cluster_id"]}
        for db_col, src_col in INFO_COLUMNS.items():
            row[db_col] = _clean(r.get(src_col))
        rows.append(row)
    return rows


def build_mapping_rows(
    normalized: pl.DataFrame, members: pl.DataFrame, diagnostics: pl.DataFrame
) -> list[dict]:
    joined = members.join(
        normalized.select("record_id", "property_code"), on="record_id", how="left"
    ).join(diagnostics.select("cluster_id", "min_edge_probability"), on="cluster_id", how="left")

    rows = []
    for r in joined.iter_rows(named=True):
        provider = r["provider"]
        code = r["property_code"] or ""
        rows.append(
            {
                "tripgain_id": r["cluster_id"],
                "provider_name": provider,
                "provider_hotel_code": code,
                "providernamehotelcode": f"{provider}###{code}",
                "is_representative": 1 if r["is_representative"] else 0,
                "mapping_type": STATUS_TO_MAPPING_TYPE[r["status"]],
                "confidence": _clean(r["min_edge_probability"]),
            }
        )
    return rows


def build_cluster_summary_rows(
    normalized: pl.DataFrame, members: pl.DataFrame
) -> list[dict]:
    matched = members.join(
        normalized.select(
            "record_id", "property_code", "property_name",
            "address_lines", "city_name", "lat_norm", "lng_norm", "geohash6_norm"
        ), on="record_id", how="left"
    )

    breakdown = (
        matched.group_by("cluster_id", "provider")
        .agg(pl.len().alias("n"))
        .group_by("cluster_id")
        .agg(pl.struct("provider", "n").alias("breakdown"))
    )
    member_lists = matched.group_by("cluster_id").agg(
        pl.struct(
            pl.col("provider").alias("provider_name"),
            pl.col("property_code").alias("provider_hotel_code"),
            pl.col("is_representative").cast(pl.Int32),
            pl.col("property_name").alias("name"),
            pl.col("address_lines").alias("address"),
            pl.col("city_name").alias("city"),
            pl.col("lat_norm").alias("lat"),
            pl.col("lng_norm").alias("lng"),
            pl.col("geohash6_norm").alias("geohash6")
        ).alias("members")
    )

    rep = normalized.select("record_id", "provider", "property_code", "property_name", "country_code_norm")

    cluster_stats = matched.group_by("cluster_id").agg(
        pl.len().alias("cluster_size"),
        pl.col("provider").n_unique().alias("provider_count")
    )

    reps_only = members.filter(pl.col("is_representative") == True).select("cluster_id", "record_id")  # noqa: E712

    joined = (
        reps_only
        .join(cluster_stats, on="cluster_id", how="left")
        .join(rep, on="record_id", how="left")
        .join(breakdown, on="cluster_id", how="left")
        .join(member_lists, on="cluster_id", how="left")
    )

    rows = []
    for r in joined.iter_rows(named=True):
        breakdown_dict = {item["provider"]: item["n"] for item in (r["breakdown"] or [])}
        rows.append(
            {
                "tripgain_id": r["cluster_id"],
                "representative_provider": r["provider"],
                "representative_hotel_code": r["property_code"],
                "property_name": r["property_name"],
                "country_code": r["country_code_norm"],
                "cluster_size": r["cluster_size"],
                "supplier_count": r["provider_count"],
                "supplier_breakdown": json.dumps(breakdown_dict, ensure_ascii=False),
                "members": json.dumps(r["members"] or [], ensure_ascii=False),
            }
        )
    return rows


def export_to_db(
    run_dir: Path,
    clusters_dir: Path | None = None,
    version: str = "v2_4",
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Build the three table payloads from a run's parquets and refresh the DB.

    Returns a {table: inserted_count} dict. Raises if the write key is missing
    (unless dry_run). Refresh is scoped to the run's country.
    """
    run_dir = Path(run_dir)
    clusters_dir = Path(clusters_dir) if clusters_dir else run_dir / "splink_v2_1" / "clusters"

    if not dry_run and not write_api_key():
        raise RuntimeError(
            "no write key — set WRITE_API_KEY in .env (repo root) or export "
            "EMBEDDING_GATEWAY_WRITE_API_KEY"
        )

    diag = json.loads((run_dir / "diagnostics.json").read_text())
    country = str(diag.get("country", "")).upper()
    if not country:
        raise RuntimeError(f"no country in {run_dir}/diagnostics.json — cannot scope the refresh")

    t0 = time.time()
    normalized, members, diagnostics = load_inputs(run_dir, clusters_dir, version, country)
    print(f"[export] country={country} normalized={normalized.height:,} "
          f"members={members.height:,} clusters={diagnostics.height:,}")

    tables = [
        (INFO_TABLE, build_hotel_info_rows(normalized, members)),
        (MAPPINGS_TABLE, build_mapping_rows(normalized, members, diagnostics)),
        (SUMMARY_TABLE, build_cluster_summary_rows(normalized, members)),
    ]
    for table, rows in tables:
        print(f"[export] {table}: {len(rows):,} rows")

    if dry_run:
        for table, rows in tables:
            print(f"\n--- {table} sample row ---")
            print(json.dumps(rows[0] if rows else {}, indent=2, ensure_ascii=False, default=str))
        return {table: len(rows) for table, rows in tables}

    inserted: dict[str, int] = {}
    for table, rows in tables:
        print(f"[export] refreshing {table} ({country}) ...")
        deleted = delete_country(table, country)
        print(f"  deleted {deleted:,} existing {country} rows")
        inserted[table] = insert_rows(table, rows)

    print(f"[export] done in {time.time()-t0:.1f}s")
    return inserted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export normalize+clustering output to the hotelmap DB")
    parser.add_argument("--run", required=True, help="run dir, e.g. data/artifacts/runs/2026-06-10_IN_001")
    parser.add_argument("--clusters-dir", default=None, help="default: <run>/splink_v2_1/clusters")
    parser.add_argument("--version", default="v2_4", help="clustering version suffix (default v2_4)")
    parser.add_argument("--dry-run", action="store_true", help="build payloads but do not call the write API")
    args = parser.parse_args(argv)

    try:
        export_to_db(Path(args.run), args.clusters_dir, args.version, dry_run=args.dry_run)
    except RuntimeError as e:
        sys.exit(str(e))
    return 0


if __name__ == "__main__":
    sys.exit(main())
