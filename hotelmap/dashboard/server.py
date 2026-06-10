"""Hotel mapping status dashboard — dependency-free backend.

Serves a static dark-mode frontend and a small JSON API that queries the
normalization + Splink clustering parquet artifacts live via DuckDB.

Run:
    .venv/bin/python -m hotelmap.dashboard.server
    .venv/bin/python -m hotelmap.dashboard.server --run data/artifacts/runs/2026-06-09_005 --version v2_1 --port 8000

No external web framework required (uses stdlib http.server + duckdb).
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
import time
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNS_DIR = REPO_ROOT / "data" / "artifacts" / "runs"
STATIC_DIR = Path(__file__).resolve().parent / "static"

# Filled in by main().
CFG: dict = {}
_DB_LOCK = threading.Lock()
_CON: duckdb.DuckDBPyConnection | None = None
_REVIEW_QUEUE_READY = False


# --------------------------------------------------------------------------- #
# Run / version resolution
# --------------------------------------------------------------------------- #
def latest_run() -> Path | None:
    """Newest run that actually has clustering output — a run currently being
    built by the pipeline must not break server startup. Returns None when no
    completed runs exist yet (fresh checkout / empty runs dir): the server
    then starts in pipeline-only mode instead of refusing to boot."""
    if not RUNS_DIR.is_dir():
        return None
    for p in sorted((p for p in RUNS_DIR.iterdir() if p.is_dir()), reverse=True):
        if not (p / "normalized_hotels.parquet").exists():
            continue
        try:
            resolve_version(p, None)
        except SystemExit:
            continue
        return p
    return None


def _version_tree(version: str) -> str:
    # v2_2 only changes clustering rules; its artifacts live in the splink_v2_1
    # tree with a _v2_2 file suffix (review queue in review_queue_v2_2/)
    return "v2_1" if version in ("v2_2", "v2_3", "v2_4") else version


def resolve_version(run: Path, version: str | None) -> str:
    """Pick the clustering version dir that actually has clusters."""
    candidates = [version] if version else ["v2_4", "v2_3", "v2_2", "v2_1", "v2", "v1"]
    for v in candidates:
        base = run / f"splink_{_version_tree(v)}" / "clusters"
        if not v or not base.is_dir():
            continue
        if v == _version_tree(v) or (base / f"cluster_diagnostics_{v}.parquet").exists():
            return v
    raise SystemExit(f"No clustering output found in {run}")


def cluster_paths(run: Path, version: str) -> dict[str, str]:
    base = run / f"splink_{_version_tree(version)}" / "clusters"
    suffix = f"_{version}" if (base / f"cluster_diagnostics_{version}.parquet").exists() else ""
    return {
        "diagnostics": str(base / f"cluster_diagnostics{suffix}.parquet"),
        "members": str(base / f"cluster_members{suffix}.parquet"),
        "edges": str(base / f"cluster_edges{suffix}.parquet"),
        "summary": str(base / f"clustering_summary{suffix}.json"),
    }


def review_paths(run: Path, version: str) -> dict[str, str]:
    tree = _version_tree(version)
    queue_dir = "review_queue" if version == tree else f"review_queue_{version}"
    base = run / f"splink_{tree}" / queue_dir
    return {
        "dir": str(base),
        "summary": str(base / "review_queue_summary.json"),
        "clusters": str(base / "review_queue_clusters.parquet"),
        "edges": str(base / "review_queue_edges.parquet"),
        "members": str(base / "review_queue_members.parquet"),
        "samples": str(base / "review_queue_samples.parquet"),
        "top_csv": str(base / "review_queue_top.csv"),
        "decisions_template": str(base / "review_decisions_template.parquet"),
    }


# --------------------------------------------------------------------------- #
# DuckDB helpers
# --------------------------------------------------------------------------- #
def con() -> duckdb.DuckDBPyConnection:
    global _CON
    if _CON is None:
        _CON = duckdb.connect()
    return _CON


def q(sql: str, params: list | None = None) -> list[dict]:
    with _DB_LOCK:
        cur = con().execute(sql, params or [])
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    out = []
    for r in rows:
        d = {}
        for c, v in zip(cols, r):
            # DuckDB returns lists for LIST columns and Decimal for decimals.
            if isinstance(v, list):
                d[c] = list(v)
            else:
                try:
                    import decimal

                    if isinstance(v, decimal.Decimal):
                        v = int(v)
                except Exception:
                    pass
                d[c] = v
        out.append(d)
    return out


def q1(sql: str, params: list | None = None) -> dict:
    rows = q(sql, params)
    return rows[0] if rows else {}


# --------------------------------------------------------------------------- #
# API endpoints
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def api_overview() -> dict:
    diag = json.loads((CFG["run"] / "diagnostics.json").read_text())
    summary = json.loads(Path(CFG["paths"]["summary"]).read_text())
    review_summary_path = Path(CFG["review_paths"]["summary"])
    review_summary = json.loads(review_summary_path.read_text()) if review_summary_path.exists() else None
    return {
        "run_id": diag.get("run_id"),
        "country": diag.get("country"),
        "version": CFG["version"],
        "normalization_version": diag.get("normalization_version"),
        "total_records": diag.get("total_records"),
        "records_per_provider": diag.get("records_per_provider", {}),
        "rates": diag.get("rates", {}),
        "guardrails": diag.get("guardrails", {}),
        "guardrails_passed": diag.get("guardrails_passed"),
        "token_policy_sizes": diag.get("token_policy_sizes", {}),
        "distinct_global_tokens": diag.get("distinct_global_tokens"),
        "clustering": summary,
        "review_queue": review_summary,
    }


@lru_cache(maxsize=1)
def api_providers() -> dict:
    pq = str(CFG["run"] / "provider_quality.parquet")
    rows = q(f"SELECT * FROM read_parquet('{pq}') ORDER BY records DESC")
    return {"providers": rows}


@lru_cache(maxsize=1)
def api_review_reasons() -> dict:
    d = CFG["paths"]["diagnostics"]
    # review_reasons is a List(String); unnest and count.
    rows = q(
        f"""
        SELECT reason, count(*) AS clusters,
               sum(cluster_size) AS records
        FROM (
            SELECT unnest(review_reasons) AS reason, cluster_size
            FROM read_parquet('{d}')
            WHERE cluster_status = 'review'
        )
        GROUP BY reason ORDER BY clusters DESC
        """
    )
    return {"reasons": rows}


@lru_cache(maxsize=1)
def api_size_histogram() -> dict:
    d = CFG["paths"]["diagnostics"]
    rows = q(
        f"""
        SELECT cluster_size AS size,
               count(*) AS clusters,
               sum(CASE WHEN cluster_status='auto_accept' THEN 1 ELSE 0 END) AS auto,
               sum(CASE WHEN cluster_status='review' THEN 1 ELSE 0 END) AS review
        FROM read_parquet('{d}')
        WHERE cluster_size > 1
        GROUP BY cluster_size ORDER BY cluster_size
        """
    )
    return {"histogram": rows}


def api_clusters(params: dict) -> dict:
    ensure_city_tables()  # rep_name/city columns + name search table
    d = CFG["paths"]["diagnostics"]
    status = (params.get("status", [""])[0] or "").strip()
    provider = (params.get("provider", [""])[0] or "").strip()
    search = (params.get("search", [""])[0] or "").strip()
    contact = (params.get("contact", [""])[0] or "").strip()
    min_size = int(params.get("min_size", ["0"])[0] or 0)
    sort = (params.get("sort", ["cluster_size"])[0] or "cluster_size")
    direction = (params.get("dir", ["desc"])[0] or "desc").lower()
    page = max(1, int(params.get("page", ["1"])[0] or 1))
    page_size = min(200, max(1, int(params.get("page_size", ["50"])[0] or 50)))

    sort_cols = {
        "cluster_size": "cluster_size",
        "provider_count": "provider_count",
        "geo": "max_geo_diameter_m",
        "prob": "min_edge_probability",
        "name_share": "dominant_name_signature_share",
    }
    sort_col = sort_cols.get(sort, "cluster_size")
    direction = "DESC" if direction == "desc" else "ASC"

    where = ["cluster_size > 1"]
    args: list = []
    if status in ("auto_accept", "review"):
        where.append("cluster_status = ?")
        args.append(status)
    if provider:
        where.append("list_contains(providers_present, ?)")
        args.append(provider)
    if contact == "yes":
        where.append("has_contact_evidence = TRUE")
    elif contact == "no":
        where.append("has_contact_evidence = FALSE")
    if min_size > 1:
        where.append("cluster_size >= ?")
        args.append(min_size)
    if search:
        # hotel name (any member) or cluster id
        where.append(
            "(cluster_id ILIKE ? OR cluster_id IN "
            "(SELECT cluster_id FROM dashboard_cluster_names WHERE names ILIKE ?))"
        )
        args.extend([f"%{search}%", f"%{search}%"])
    where_sql = " AND ".join(where)

    total = q1(
        f"SELECT count(*) AS n FROM read_parquet('{d}') WHERE {where_sql}", args
    )["n"]
    offset = (page - 1) * page_size
    rows = q(
        f"""
        SELECT d.cluster_id, d.cluster_size, d.provider_count, d.providers_present,
               round(d.max_geo_diameter_m, 1) AS max_geo_diameter_m,
               round(d.min_edge_probability, 4) AS min_edge_probability,
               round(d.dominant_name_signature_share, 3) AS dominant_name_signature_share,
               d.has_contact_evidence, d.has_phone_edge, d.has_email_edge, d.has_domain_edge,
               d.cluster_status, d.review_reasons, d.representative_record_id,
               c.rep_name, c.city
        FROM (SELECT * FROM read_parquet('{d}') WHERE {where_sql}) d
        LEFT JOIN dashboard_cluster_centroids c ON d.cluster_id = c.cluster_id
        ORDER BY d.{sort_col} {direction}, d.cluster_id
        LIMIT ? OFFSET ?
        """,
        args + [page_size, offset],
    )
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
        "clusters": rows,
    }


_CID_RE = re.compile(r"^[A-Za-z0-9_]+$")


def api_cluster_detail(cluster_id: str) -> dict:
    if not _CID_RE.match(cluster_id):
        return {"error": "invalid cluster id"}
    d = CFG["paths"]["diagnostics"]
    m = CFG["paths"]["members"]
    e = CFG["paths"]["edges"]
    nh = str(CFG["run"] / "normalized_hotels.parquet")

    meta = q1(
        f"SELECT * FROM read_parquet('{d}') WHERE cluster_id = ?", [cluster_id]
    )
    if not meta:
        return {"error": "not found"}
    # Tolerate normalized parquets from older runs that miss newer columns
    # (e.g. IN run 005 predates property_name_match_signature/_tokens):
    # select NULL for anything absent instead of binder-erroring.
    with _DB_LOCK:
        nh_cols = {r[0] for r in con().execute(
            f"DESCRIBE SELECT * FROM read_parquet('{nh}')").fetchall()}
    wanted = [
        "property_code", "hotel_id", "source_credential", "source_row_number",
        "property_name", "address_lines", "city_name", "city_code",
        "state", "country_code", "country_name", "postal_code",
        "lat", "lng", "star_rating", "property_type",
        "phone_numbers", "emails", "fax_numbers", "hotel_chain",
        "amenities", "check_in_time", "check_out_time",
        "number_of_rooms", "average_of_rating", "total_reviews",
        "thumbnail", "weburl", "land_mark", "area", "geohash6",
        "updated_at", "country_code_iso2",
        "property_name_norm", "property_name_core",
        "property_name_signature", "property_name_match_signature",
        "property_name_tokens", "property_name_brand_tokens",
        "property_name_low_value_tokens", "property_name_location_tokens",
        "property_name_match_tokens",
        "address_norm", "city_name_norm", "state_norm",
        "country_code_norm", "postal_code_final",
        "lat_norm", "lng_norm", "lat_lng_valid",
        "coord_precision_bucket", "h3_8", "h3_9",
        "phone_e164_list", "phone_last10_non_reused_list",
        "phone_reused_flag", "email_norm_list", "email_generic_flag",
        "email_reused_flag", "website_domain_norm",
        "website_weak_domain_flag", "website_reused_flag",
        "property_type_norm", "star_rating_norm", "hotel_chain_norm",
    ]
    select_cols = ",\n               ".join(
        f"h.{c}" if c in nh_cols else f"NULL AS {c}" for c in wanted
    )
    # coerce decimal / list already handled by q()
    members = q(
        f"""
        WITH cluster_center AS (
            SELECT avg(h.lat_norm) AS center_lat, avg(h.lng_norm) AS center_lng
            FROM read_parquet('{m}') mem
            JOIN read_parquet('{nh}') h ON mem.record_id = h.record_id
            WHERE mem.cluster_id = ?
              AND h.lat_norm IS NOT NULL AND h.lng_norm IS NOT NULL
        )
        SELECT mem.record_id, mem.provider, mem.is_representative,
               mem.status,
               {select_cols},
               CASE
                   WHEN h.lat_norm IS NULL OR h.lng_norm IS NULL
                        OR center_lat IS NULL OR center_lng IS NULL
                   THEN NULL
                   ELSE round(2 * 6371000 * asin(sqrt(
                       pow(sin(radians(h.lat_norm - center_lat) / 2), 2)
                       + cos(radians(center_lat)) * cos(radians(h.lat_norm))
                         * pow(sin(radians(h.lng_norm - center_lng) / 2), 2))))
               END AS dist_from_cluster_m
        FROM read_parquet('{m}') mem
        JOIN read_parquet('{nh}') h ON mem.record_id = h.record_id
        CROSS JOIN cluster_center
        WHERE mem.cluster_id = ?
        ORDER BY mem.is_representative DESC, mem.provider, h.property_name
        """,
        [cluster_id, cluster_id],
    )
    edges = q(
        f"""
        SELECT record_id_l, record_id_r, provider_l, provider_r,
               round(match_probability, 4) AS match_probability,
               gate_reason,
               round(name_match_jaccard, 3) AS name_match_jaccard,
               sig_exact,
               round(dist_m, 1) AS dist_m,
               phone_ov
        FROM read_parquet('{e}')
        WHERE cluster_id = ?
        ORDER BY match_probability DESC
        LIMIT 500
        """,
        [cluster_id],
    )
    return {"meta": meta, "members": members, "edges": edges}


@lru_cache(maxsize=1)
def api_review_summary() -> dict:
    path = Path(CFG["review_paths"]["summary"])
    if not path.exists():
        return {"available": False}
    data = json.loads(path.read_text())
    data["available"] = True
    return data


def ensure_review_queue_table() -> bool:
    """Materialize full 09E review items once per server process."""
    global _REVIEW_QUEUE_READY
    if _REVIEW_QUEUE_READY:
        return True

    rp = CFG["review_paths"]
    clusters = rp["clusters"]
    edges = rp["edges"]
    members = rp["members"]
    if not (Path(clusters).exists() and Path(edges).exists() and Path(members).exists()):
        return False

    sql = f"""
    CREATE OR REPLACE TEMP TABLE dashboard_review_queue AS
    WITH pair_members AS (
        SELECT review_id,
               string_agg(DISTINCT provider, ', ' ORDER BY provider) AS providers,
               string_agg(name, ' | ' ORDER BY provider, record_id) AS representative_names,
               string_agg(DISTINCT city, ', ' ORDER BY city) AS cities,
               string_agg(DISTINCT postal_code, ', ' ORDER BY postal_code) AS postal_codes
        FROM read_parquet('{members}')
        WHERE review_entity_type = 'pair'
        GROUP BY review_id
    ),
    pair_base AS (
        SELECT e.review_id, 'pair' AS review_entity_type, e.cluster_id,
               2 AS cluster_size, pm.providers, pm.representative_names,
               pm.cities, pm.postal_codes,
               e.dist_m AS geo_diameter_m,
               e.match_probability AS best_edge_probability,
               e.gate_reason AS best_edge_reason,
               e.gate_reason AS worst_edge_reason,
               NULL::VARCHAR AS shared_phones,
               NULL::VARCHAR AS shared_emails,
               NULL::VARCHAR AS shared_domains,
               CASE
                   WHEN e.gate_reason = 'exact_email' AND e.dist_m <= 500
                       THEN 'contact_or_email_true_like_but_failed_coherence'
                   WHEN e.gate_reason = 'phone' AND e.name_match_jaccard >= 0.60 AND e.dist_m <= 500
                       THEN 'contact_or_email_true_like_but_failed_coherence'
                   WHEN e.gate_reason = 'exact_email' AND e.name_match_jaccard >= 0.60
                       THEN 'exact_email_strong_name'
                   WHEN e.gate_reason = 'phone' AND e.name_match_jaccard >= 0.60
                       THEN 'phone_strong_name'
                   WHEN e.gate_reason = 'signature' THEN 'signature_review_pair'
                   WHEN e.gate_reason = 'medium_name_plus_geo' THEN 'medium_name_plus_geo'
                   ELSE 'needs_more_info'
               END AS review_reason
        FROM read_parquet('{edges}') e
        JOIN pair_members pm USING (review_id)
        WHERE e.review_entity_type = 'pair'
    ),
    pair_items AS (
        SELECT review_id, review_entity_type, cluster_id, cluster_size,
               providers, representative_names, cities, postal_codes,
               geo_diameter_m, best_edge_probability, best_edge_reason,
               worst_edge_reason, shared_phones, shared_emails, shared_domains,
               CASE
                   WHEN review_reason IN (
                       'contact_or_email_true_like_but_failed_coherence',
                       'exact_email_strong_name',
                       'phone_strong_name',
                       'signature_review_pair'
                   ) THEN 'high_confidence_likely_true'
                   WHEN review_reason = 'medium_name_plus_geo' THEN 'medium_name_plus_geo'
                   ELSE 'medium_name_plus_geo'
               END AS review_bucket,
               (
                   CASE WHEN best_edge_reason = 'exact_email'
                             AND review_reason IN ('contact_or_email_true_like_but_failed_coherence', 'exact_email_strong_name')
                        THEN 40 ELSE 0 END
                 + CASE WHEN review_reason IN ('contact_or_email_true_like_but_failed_coherence', 'phone_strong_name')
                              AND best_edge_reason = 'phone'
                        THEN 35 ELSE 0 END
                 + CASE WHEN best_edge_reason = 'signature' THEN 30 ELSE 0 END
                 + CASE WHEN best_edge_reason = 'medium_name_plus_geo' THEN 10 ELSE 0 END
                 + 6
                 + CASE WHEN review_reason = 'contact_or_email_true_like_but_failed_coherence' THEN 140 ELSE 0 END
               )::INTEGER AS priority,
               CASE
                   WHEN review_reason IN (
                       'contact_or_email_true_like_but_failed_coherence',
                       'exact_email_strong_name',
                       'phone_strong_name',
                       'signature_review_pair',
                       'medium_name_plus_geo'
                   ) THEN 'accept_pair_only'
                   ELSE 'needs_more_info'
               END AS suggested_action
        FROM pair_base
    ),
    cluster_items AS (
        SELECT review_id, 'cluster' AS review_entity_type, cluster_id, cluster_size,
               providers, representative_names, cities, postal_codes,
               geo_diameter_m, best_edge_probability, best_edge_reason,
               worst_edge_reason, shared_phones, shared_emails, shared_domains,
               review_bucket, priority, suggested_action
        FROM read_parquet('{clusters}')
    )
    SELECT * FROM cluster_items
    UNION ALL
    SELECT * FROM pair_items
    """
    with _DB_LOCK:
        if not _REVIEW_QUEUE_READY:
            con().execute(sql)
            _REVIEW_QUEUE_READY = True
    return True


def api_review_queue(params: dict) -> dict:
    rp = CFG["review_paths"]
    top = rp["top_csv"]
    has_full_queue = ensure_review_queue_table()
    if not has_full_queue and not Path(top).exists():
        return {"available": False, "items": [], "total": 0, "page": 1, "pages": 0}

    bucket = (params.get("bucket", [""])[0] or "").strip()
    action = (params.get("action", [""])[0] or "").strip()
    search = (params.get("search", [""])[0] or "").strip()
    page = max(1, int(params.get("page", ["1"])[0] or 1))
    page_size = min(200, max(1, int(params.get("page_size", ["50"])[0] or 50)))

    where = ["TRUE"]
    args: list = []
    if bucket:
        where.append("review_bucket = ?")
        args.append(bucket)
    if action:
        where.append("suggested_action = ?")
        args.append(action)
    if search:
        where.append(
            "(review_id ILIKE ? OR cluster_id ILIKE ? OR representative_names ILIKE ? OR providers ILIKE ? OR cities ILIKE ?)"
        )
        args.extend([f"%{search}%"] * 5)
    where_sql = " AND ".join(where)

    if has_full_queue:
        source_sql = f"SELECT * FROM dashboard_review_queue WHERE {where_sql}"
    else:
        source_sql = f"""
        SELECT *, CASE WHEN starts_with(review_id, 'RQC_') THEN 'cluster' ELSE 'pair' END AS review_entity_type
        FROM read_csv_auto('{top}')
        WHERE {where_sql}
        """

    total = q1(f"SELECT count(*) AS n FROM ({source_sql})", args)["n"]
    offset = (page - 1) * page_size
    rows = q(
        f"""
        SELECT review_id, review_entity_type,
               priority, review_bucket, cluster_id, cluster_size, providers,
               representative_names, cities, postal_codes,
               round(geo_diameter_m, 1) AS geo_diameter_m,
               round(best_edge_probability, 4) AS best_edge_probability,
               best_edge_reason, worst_edge_reason,
               shared_phones, shared_emails, shared_domains,
               suggested_action
        FROM ({source_sql})
        ORDER BY priority DESC, best_edge_probability DESC, review_id
        LIMIT ? OFFSET ?
        """,
        args + [page_size, offset],
    )
    return {
        "available": True,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
        "items": rows,
    }


_RID_RE = re.compile(r"^(RQC|RQE)_[A-Fa-f0-9]+$")


def api_review_detail(review_id: str) -> dict:
    if not _RID_RE.match(review_id):
        return {"error": "invalid review id"}
    rp = CFG["review_paths"]
    if not Path(rp["edges"]).exists():
        return {"error": "review queue not found"}

    is_cluster = review_id.startswith("RQC_")
    if is_cluster:
        meta = q1(
            f"""
            SELECT *, 'cluster' AS review_entity_type
            FROM read_parquet('{rp["clusters"]}')
            WHERE review_id = ?
            """,
            [review_id],
        )
    else:
        meta = q1(
            f"""
            SELECT review_id, 'pair' AS review_entity_type, cluster_id,
                   2 AS cluster_size,
                   string_agg(DISTINCT provider, ', ' ORDER BY provider) AS providers,
                   string_agg(name, ' | ' ORDER BY provider, record_id) AS representative_names,
                   string_agg(DISTINCT city, ', ' ORDER BY city) AS cities
            FROM read_parquet('{rp["members"]}')
            WHERE review_id = ?
            GROUP BY review_id, cluster_id
            """,
            [review_id],
        )
    if not meta:
        return {"error": "not found"}

    members = q(
        f"""
        SELECT record_id, provider, name, city, postal_code,
               round(lat_norm, 6) AS lat_norm, round(lng_norm, 6) AS lng_norm
        FROM read_parquet('{rp["members"]}')
        WHERE review_id = ?
        ORDER BY provider, record_id
        LIMIT 500
        """,
        [review_id],
    )
    edges = q(
        f"""
        SELECT provider_l, provider_r, round(match_probability, 4) AS match_probability,
               gate_reason, round(name_match_jaccard, 3) AS name_match_jaccard,
               sig_exact, round(dist_m, 1) AS dist_m, phone_ov,
               record_id_l, record_id_r
        FROM read_parquet('{rp["edges"]}')
        WHERE review_id = ?
        ORDER BY match_probability DESC
        LIMIT 500
        """,
        [review_id],
    )
    decision = load_decisions().get(review_id)
    return {"meta": meta, "members": members, "edges": edges, "decision": decision}


# --------------------------------------------------------------------------- #
# Cities / unmapped / nearest-clusters
# --------------------------------------------------------------------------- #
_CITY_TABLES_READY = False


def ensure_city_tables() -> None:
    """Materialize per-city stats + cluster centroids once per process."""
    global _CITY_TABLES_READY
    if _CITY_TABLES_READY:
        return
    m = CFG["paths"]["members"]
    d = CFG["paths"]["diagnostics"]
    nh = str(CFG["run"] / "normalized_hotels.parquet")
    with _DB_LOCK:
        if _CITY_TABLES_READY:
            return
        diag_cols = [
            r[0] for r in con().execute(
                f"DESCRIBE SELECT * FROM read_parquet('{d}')"
            ).fetchall()
        ]
        entity_sql = (
            "any_value(d.entity_level)" if "entity_level" in diag_cols else "'property'"
        )
        con().execute(
            f"""
            CREATE OR REPLACE TEMP TABLE dashboard_city_stats AS
            SELECT COALESCE(h.city_name_norm, '(no city)') AS city,
                   COUNT(*) AS records,
                   COUNT(*) FILTER (WHERE mem.status = 'matched_cluster') AS matched_records,
                   COUNT(*) FILTER (WHERE mem.status = 'review_cluster') AS review_records,
                   COUNT(*) FILTER (WHERE mem.status = 'singleton_unmatched') AS unmapped_records,
                   COUNT(DISTINCT mem.cluster_id) FILTER (WHERE mem.cluster_id IS NOT NULL) AS clusters,
                   COUNT(DISTINCT h.provider) AS providers
            FROM read_parquet('{m}') mem
            JOIN read_parquet('{nh}') h USING (record_id)
            GROUP BY 1
            """
        )
        con().execute(
            f"""
            CREATE OR REPLACE TEMP TABLE dashboard_cluster_centroids AS
            SELECT mem.cluster_id,
                   any_value(d.cluster_status) AS cluster_status,
                   any_value(d.cluster_size) AS cluster_size,
                   any_value(d.provider_count) AS provider_count,
                   {entity_sql} AS entity_level,
                   AVG(h.lat_norm) AS lat,
                   AVG(h.lng_norm) AS lng,
                   mode(h.city_name_norm) AS city,
                   arg_max(h.property_name_norm, CAST(mem.is_representative AS INT)) AS rep_name
            FROM read_parquet('{m}') mem
            JOIN read_parquet('{nh}') h USING (record_id)
            JOIN read_parquet('{d}') d USING (cluster_id)
            WHERE mem.cluster_id IS NOT NULL
            GROUP BY mem.cluster_id
            """
        )
        # all member names per cluster, for name search in the clusters view
        con().execute(
            f"""
            CREATE OR REPLACE TEMP TABLE dashboard_cluster_names AS
            SELECT mem.cluster_id,
                   string_agg(DISTINCT h.property_name_norm, ' | ') AS names
            FROM read_parquet('{m}') mem
            JOIN read_parquet('{nh}') h USING (record_id)
            WHERE mem.cluster_id IS NOT NULL
            GROUP BY mem.cluster_id
            """
        )
        _CITY_TABLES_READY = True


def api_cities(params: dict) -> dict:
    ensure_city_tables()
    search = (params.get("search", [""])[0] or "").strip()
    page = max(1, int(params.get("page", ["1"])[0] or 1))
    page_size = min(100, max(1, int(params.get("page_size", ["30"])[0] or 30)))
    where, args = "TRUE", []
    if search:
        where = "city ILIKE ?"
        args = [f"%{search}%"]
    total = q1(f"SELECT count(*) AS n FROM dashboard_city_stats WHERE {where}", args)["n"]
    rows = q(
        f"""
        SELECT city, records, matched_records, review_records, unmapped_records,
               clusters, providers,
               round(matched_records * 1.0 / NULLIF(records, 0), 4) AS matched_share
        FROM dashboard_city_stats WHERE {where}
        ORDER BY records DESC, city
        LIMIT ? OFFSET ?
        """,
        args + [page_size, (page - 1) * page_size],
    )
    return {"total": total, "page": page, "page_size": page_size,
            "pages": (total + page_size - 1) // page_size, "cities": rows}


def api_city_clusters(params: dict) -> dict:
    ensure_city_tables()
    city = (params.get("city", [""])[0] or "").strip()
    if not city:
        return {"clusters": []}
    rows = q(
        """
        SELECT cluster_id, cluster_status, cluster_size, provider_count,
               entity_level, rep_name, round(lat, 5) AS lat, round(lng, 5) AS lng
        FROM dashboard_cluster_centroids
        WHERE city = ?
        ORDER BY cluster_size DESC, cluster_id
        LIMIT 200
        """,
        [city],
    )
    return {"city": city, "clusters": rows}


def api_unmapped(params: dict) -> dict:
    m = CFG["paths"]["members"]
    nh = str(CFG["run"] / "normalized_hotels.parquet")
    city = (params.get("city", [""])[0] or "").strip()
    provider = (params.get("provider", [""])[0] or "").strip()
    search = (params.get("search", [""])[0] or "").strip()
    page = max(1, int(params.get("page", ["1"])[0] or 1))
    page_size = min(200, max(1, int(params.get("page_size", ["50"])[0] or 50)))
    where, args = ["mem.status = 'singleton_unmatched'"], []
    if city:
        where.append("h.city_name_norm = ?")
        args.append(city)
    if provider:
        where.append("h.provider = ?")
        args.append(provider)
    if search:
        where.append("(h.property_name_norm ILIKE ? OR h.city_name_norm ILIKE ? OR h.record_id ILIKE ?)")
        args.extend([f"%{search}%"] * 3)
    where_sql = " AND ".join(where)
    base = (
        f"FROM read_parquet('{m}') mem JOIN read_parquet('{nh}') h USING (record_id) "
        f"WHERE {where_sql}"
    )
    total = q1(f"SELECT count(*) AS n {base}", args)["n"]
    rows = q(
        f"""
        SELECT h.record_id, h.provider, h.property_name_norm AS name,
               h.city_name_norm AS city, h.postal_code_final AS postal_code,
               h.star_rating_norm AS star_rating, h.property_type_norm AS property_type,
               round(h.lat_norm, 5) AS lat, round(h.lng_norm, 5) AS lng
        {base}
        ORDER BY h.city_name_norm, h.property_name_norm
        LIMIT ? OFFSET ?
        """,
        args + [page_size, (page - 1) * page_size],
    )
    return {"total": total, "page": page, "page_size": page_size,
            "pages": (total + page_size - 1) // page_size, "items": rows}


def api_nearest_clusters(params: dict) -> dict:
    ensure_city_tables()
    record_id = (params.get("record_id", [""])[0] or "").strip()
    if not record_id:
        return {"error": "record_id required"}
    limit = min(20, max(1, int(params.get("limit", ["8"])[0] or 8)))
    nh = str(CFG["run"] / "normalized_hotels.parquet")
    rec = q1(
        f"""
        SELECT record_id, provider, property_name_norm AS name, city_name_norm AS city,
               postal_code_final AS postal_code, lat_norm AS lat, lng_norm AS lng
        FROM read_parquet('{nh}') WHERE record_id = ?
        """,
        [record_id],
    )
    if not rec:
        return {"error": "record not found"}
    if rec.get("lat") is None or rec.get("lng") is None:
        # no usable coords: fall back to same-city clusters only
        rows = q(
            """
            SELECT cluster_id, cluster_status, cluster_size, provider_count,
                   entity_level, rep_name, city, NULL AS dist_m, TRUE AS same_city
            FROM dashboard_cluster_centroids WHERE city = ?
            ORDER BY cluster_size DESC LIMIT ?
            """,
            [rec.get("city"), limit],
        )
        return {"record": rec, "nearest": rows, "geo": False}
    rows = q(
        """
        SELECT cluster_id, cluster_status, cluster_size, provider_count,
               entity_level, rep_name, city,
               round(2 * 6371000 * asin(sqrt(
                   pow(sin(radians(lat - ?) / 2), 2)
                   + cos(radians(?)) * cos(radians(lat))
                     * pow(sin(radians(lng - ?) / 2), 2)))) AS dist_m,
               (city IS NOT DISTINCT FROM ?) AS same_city
        FROM dashboard_cluster_centroids
        WHERE lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY dist_m ASC
        LIMIT ?
        """,
        [rec["lat"], rec["lat"], rec["lng"], rec.get("city"), limit],
    )
    return {"record": rec, "nearest": rows, "geo": True}


# --------------------------------------------------------------------------- #
# Review decisions (run-scoped log; the persistent registry is 09G's job)
# --------------------------------------------------------------------------- #
DECISION_VALUES = ("same_hotel", "different_hotel",
                   "same_property_group_not_same_hotel", "unclear")


def _decisions_path() -> Path:
    return Path(CFG["review_paths"]["dir"]) / "review_decisions.jsonl"


def load_decisions() -> dict[str, dict]:
    """Latest decision per review_id from the append-only log."""
    path = _decisions_path()
    out: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                d = json.loads(line)
                out[d["review_id"]] = d
            except Exception:  # noqa: BLE001
                continue
    return out


def api_review_decision(params: dict) -> dict:
    review_id = (params.get("review_id", [""])[0] or "").strip()
    decision = (params.get("decision", [""])[0] or "").strip()
    notes = (params.get("notes", [""])[0] or "").strip()
    if not review_id:
        return {"error": "review_id required"}
    if decision not in DECISION_VALUES:
        return {"error": f"decision must be one of {DECISION_VALUES}"}
    rec = {
        "review_id": review_id,
        "decision": decision,
        "notes": notes or None,
        "reviewer": "dashboard",
        "reviewed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "run_id": CFG["run"].name,
        "version": CFG["version"],
    }
    path = _decisions_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _DB_LOCK:
        with path.open("a") as f:
            f.write(json.dumps(rec) + "\n")
    return {"ok": True, "decision": rec}


def api_review_decisions_summary(params: dict) -> dict:
    decisions = load_decisions()
    counts: dict[str, int] = {}
    for d in decisions.values():
        counts[d["decision"]] = counts.get(d["decision"], 0) + 1
    return {"total": len(decisions), "by_decision": counts}


# --------------------------------------------------------------------------- #
# Pipeline runner (download -> normalize -> ... -> review queue)
# --------------------------------------------------------------------------- #
PIPELINE_LOG_DIR = REPO_ROOT / "data" / "artifacts" / "pipeline_logs"
_PIPE_LOCK = threading.Lock()
_PIPE: dict = {"proc": None, "country": None, "log": None, "started": None,
               "skip_download": None}


def _country_configs() -> dict:
    # the server may have been launched as `python hotelmap/dashboard/server.py`,
    # where the repo root is not on sys.path and the package import would fail
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from hotelmap.pipeline import country_configs

    return country_configs()


def _gateway_key() -> str | None:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from hotelmap.download import gateway_api_key

    return gateway_api_key()


def _write_key() -> str | None:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from hotelmap.export.run_export import write_api_key

    return write_api_key()


def api_pipeline_configs(params: dict) -> dict:
    raw_dir = REPO_ROOT / "data" / "raw"
    countries = []
    for cc, cfg in sorted(_country_configs().items()):
        cdir = raw_dir / cc
        has_raw = cdir.is_dir() and any(
            f.stat().st_size > 0 for f in cdir.glob("*_property_info.ndjson")
        )
        countries.append({"country": cc, "config": cfg.name, "has_raw": has_raw})
    return {
        "countries": countries,
        "download_key_present": bool(_gateway_key()),
        "write_key_present": bool(_write_key()),
    }


def _pipeline_state() -> dict:
    proc = _PIPE["proc"]
    running = proc is not None and proc.poll() is None
    state = {
        "running": running,
        "country": _PIPE["country"],
        "skip_download": _PIPE["skip_download"],
        "started": _PIPE["started"],
        "exit_code": (proc.poll() if proc is not None else None),
    }
    log_path = _PIPE["log"]
    if log_path and Path(log_path).exists():
        lines = Path(log_path).read_text(errors="replace").splitlines()
        state["log_tail"] = lines[-80:]
        stages = [ln for ln in lines
                  if ln.startswith("[pipeline] stage") and "stage done" not in ln]
        stage = stages[-1].replace("[pipeline] stage ", "") if stages else None
        markers = [ln for ln in lines if ln.startswith("[pipeline] ===== country")]
        if markers and stage:
            # "===== country 2/3: US =====" -> "US 2/3 · <stage>"
            m = markers[-1].replace("[pipeline] ===== country ", "").rstrip(" =")
            idx, _, cc = m.partition(": ")
            stage = f"{cc} {idx} · {stage}"
        state["stage"] = stage
        done = [ln for ln in lines if ln.startswith("[pipeline] DONE run=")]
        state["run_ids"] = [Path(ln.split("run=", 1)[1].strip()).name for ln in done]
        if done:
            state["run_id"] = state["run_ids"][-1]
    return state


def api_pipeline_start(params: dict) -> dict:
    raw_value = (params.get("country", [""])[0] or "").strip().upper()
    countries = [c.strip() for c in raw_value.split(",") if c.strip()]
    skip_download = (params.get("skip_download", ["0"])[0] or "0").lower() in (
        "1", "true", "yes")
    export_db = (params.get("export_db", ["0"])[0] or "0").lower() in (
        "1", "true", "yes")
    if not countries:
        return {"error": "enter at least one 2-letter ISO country code"}
    bad = [c for c in countries if not re.fullmatch(r"[A-Z]{2}", c)]
    if bad:
        return {"error": f"not 2-letter ISO codes: {', '.join(bad)}"}
    known = _country_configs()
    for country in countries:
        if country not in known:
            # new country: the pipeline auto-generates a generic config; it
            # still needs raw data from somewhere
            raw_dir = REPO_ROOT / "data" / "raw" / country
            if skip_download and not any(raw_dir.glob("*_property_info.ndjson")):
                return {"error": f"new country {country}: no raw files under "
                                 f"{raw_dir.relative_to(REPO_ROOT)} — choose "
                                 f"'Download fresh' (needs the gateway key)"}
    if not skip_download and not _gateway_key():
        return {"error": "no gateway key — put DB_API_KEY in .env at the repo "
                         "root (or export EMBEDDING_GATEWAY_API_KEY), or use "
                         "existing raw data"}
    if export_db and not _write_key():
        return {"error": "no write key — put WRITE_API_KEY in .env at the repo "
                         "root to enable DB export, or untick it"}
    country_arg = ",".join(countries)
    with _PIPE_LOCK:
        if _PIPE["proc"] is not None and _PIPE["proc"].poll() is None:
            return {"error": f"pipeline already running for {_PIPE['country']}"}
        PIPELINE_LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = (PIPELINE_LOG_DIR
                    / f"{time.strftime('%Y-%m-%d_%H%M%S')}_{'-'.join(countries)}.log")
        cmd = [sys.executable, "-m", "hotelmap.pipeline", "--country", country_arg]
        if skip_download:
            cmd.append("--skip-download")
        if export_db:
            cmd.append("--export-db")
        log_fh = log_path.open("w")
        proc = subprocess.Popen(
            cmd, cwd=REPO_ROOT, stdout=log_fh, stderr=subprocess.STDOUT)
        log_fh.close()
        _PIPE.update(proc=proc, country=country_arg, log=str(log_path),
                     started=time.time(), skip_download=skip_download)
    return {"ok": True, **_pipeline_state()}


def api_pipeline_status(params: dict) -> dict:
    return _pipeline_state()


# --------------------------------------------------------------------------- #
# Run switching (IN / NZ / US ...)
# --------------------------------------------------------------------------- #
def api_runs(params: dict) -> dict:
    runs = []
    if not RUNS_DIR.is_dir():
        return {"runs": runs}
    for p in sorted(RUNS_DIR.iterdir()):
        if not (p / "normalized_hotels.parquet").exists():
            continue
        try:
            version = resolve_version(p, None)
        except SystemExit:
            continue
        country, records = None, None
        diag = p / "diagnostics.json"
        if diag.exists():
            try:
                dd = json.loads(diag.read_text())
                country, records = dd.get("country"), dd.get("total_records")
            except Exception:  # noqa: BLE001
                pass
        runs.append({
            "run_id": p.name, "country": country, "version": version,
            "records": records, "current": p == CFG["run"],
        })
    return {"runs": runs}


def api_switch_run(params: dict) -> dict:
    run_id = (params.get("run", [""])[0] or "").strip()
    target = (RUNS_DIR / run_id).resolve()
    if not str(target).startswith(str(RUNS_DIR.resolve())) or not (
        target / "normalized_hotels.parquet"
    ).exists():
        return {"error": "unknown run"}
    version = resolve_version(target, None)
    global _REVIEW_QUEUE_READY, _CITY_TABLES_READY
    with _DB_LOCK:
        CFG.update(
            run=target,
            version=version,
            paths=cluster_paths(target, version),
            review_paths=review_paths(target, version),
        )
        _REVIEW_QUEUE_READY = False
        _CITY_TABLES_READY = False
    for fn in (api_overview, api_providers, api_review_reasons,
               api_size_histogram, api_review_summary):
        fn.cache_clear()
    return {"ok": True, "run_id": target.name, "version": version}


ROUTES = {
    "/api/runs": api_runs,
    "/api/switch-run": api_switch_run,
    "/api/overview": lambda p: api_overview(),
    "/api/providers": lambda p: api_providers(),
    "/api/review-reasons": lambda p: api_review_reasons(),
    "/api/size-histogram": lambda p: api_size_histogram(),
    "/api/clusters": api_clusters,
    "/api/review-summary": lambda p: api_review_summary(),
    "/api/review-queue": api_review_queue,
    "/api/cities": api_cities,
    "/api/city-clusters": api_city_clusters,
    "/api/unmapped": api_unmapped,
    "/api/nearest-clusters": api_nearest_clusters,
    "/api/pipeline/configs": api_pipeline_configs,
    "/api/pipeline/start": api_pipeline_start,
    "/api/pipeline/status": api_pipeline_status,
    "/api/review-decision": api_review_decision,
    "/api/review-decisions-summary": api_review_decisions_summary,
}

# Routes that work without any run loaded (pipeline-only mode). Everything
# else answers {"no_data": true} until a run exists and is switched to.
NO_RUN_ROUTES = {
    "/api/runs",
    "/api/switch-run",
    "/api/pipeline/configs",
    "/api/pipeline/start",
    "/api/pipeline/status",
}


# --------------------------------------------------------------------------- #
# HTTP handler
# --------------------------------------------------------------------------- #
class Handler(BaseHTTPRequestHandler):
    server_version = "HotelMapDash/1.0"

    def log_message(self, fmt, *args):  # quieter logs
        pass

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path):
        if not path.is_file():
            self.send_error(404)
            return
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
        }.get(path.suffix, "application/octet-stream")
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        route = parsed.path
        try:
            if (route.startswith("/api/") and CFG.get("run") is None
                    and route not in NO_RUN_ROUTES):
                return self._send_json({
                    "no_data": True,
                    "message": "no completed runs yet — start one from the "
                               "Pipeline page",
                })
            if route.startswith("/api/clusters/"):
                cid = route[len("/api/clusters/"):]
                return self._send_json(api_cluster_detail(cid))
            if route.startswith("/api/review-queue/"):
                rid = route[len("/api/review-queue/"):]
                return self._send_json(api_review_detail(rid))
            if route in ROUTES:
                params = parse_qs(parsed.query)
                return self._send_json(ROUTES[route](params))
            # static files
            if route in ("/", ""):
                return self._send_file(STATIC_DIR / "index.html")
            safe = (STATIC_DIR / route.lstrip("/")).resolve()
            if str(safe).startswith(str(STATIC_DIR.resolve())):
                return self._send_file(safe)
            self.send_error(404)
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": str(exc)}, code=500)


def main():
    ap = argparse.ArgumentParser(description="Hotel mapping status dashboard")
    ap.add_argument("--run", default=None, help="path to run dir (default: latest)")
    ap.add_argument("--version", default=None, help="clustering version (v2_1/v2/v1)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()

    run = Path(args.run).resolve() if args.run else latest_run()
    if run is None:
        CFG.update(run=None, version=None, paths=None, review_paths=None)
        print("  run        : none yet — pipeline-only mode (start a country "
              "from the Pipeline page)")
    else:
        version = resolve_version(run, args.version)
        CFG.update(
            run=run,
            version=version,
            paths=cluster_paths(run, version),
            review_paths=review_paths(run, version),
        )
        print(f"  run        : {run.relative_to(REPO_ROOT)}")
        print(f"  version    : {version}")
    print(f"  dashboard  : http://{args.host}:{args.port}")
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
