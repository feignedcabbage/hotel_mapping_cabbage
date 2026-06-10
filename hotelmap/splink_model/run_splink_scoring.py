"""Stage 09B — Splink scoring trial + audit (pair-level only, NO clustering).

Supports --version v1 (original, correlated comparisons) and v2 (de-correlated:
distance-only geo + one order-insensitive name comparison + reuse-aware contact +
weak context). The audit answers whether scores separate matches from non-matches:
score distribution, and crosstabs of band x {identity gate, name level, phone,
distance}, plus the location-only-pair max score (the key safety metric).

The identity gate (diagnostic, not applied to clustering yet):
    passes_identity_gate = meaningful_name OR phone OR exact_email OR strong_domain
Location/context alone must never imply a match.

Outputs in <run>/splink_<version>/.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb
import polars as pl
import yaml

from splink import DuckDBAPI, Linker, block_on
from hotelmap.normalize.config import load_config
from hotelmap.normalize.names import (
    add_match_token_features,
    build_city_overcommon_token_map,
    build_match_area_identity_keep,
    build_match_global_stoplist,
)
from hotelmap.splink_model.splink_settings import (
    build_settings,
    build_settings_v2,
    build_settings_v2_1,
    em_training_rules_v2,
)

# fields whose empty string -> NULL before exact / equality use
_NULLIF_STR = [
    "property_name_signature",
    "city_name_norm",
    "postal_code_final",
    "hotel_chain_norm",
    "property_type_norm",
    "website_domain_norm",
]

_BANDS = [
    ("ge_0_99", 0.99, 1.01),
    ("b_095_099", 0.95, 0.99),
    ("b_090_095", 0.90, 0.95),
    ("b_080_090", 0.80, 0.90),
    ("b_050_080", 0.50, 0.80),
    ("b_001_050", 0.01, 0.50),
    ("lt_0_01", -0.01, 0.01),
]
PERSIST_FLOOR = 0.01
SAMPLE_PER_BAND = 200
GUARD_PAIRS_WARN = 80_000_000


def _load_effective_policy(normalized: str, config_path: str) -> dict:
    run_dir = Path(normalized).parent
    policy_path = run_dir / "token_policy_effective.yaml"
    if policy_path.exists():
        return yaml.safe_load(policy_path.read_text()) or {}
    config = load_config(config_path)
    return config.get("manual_tokens", {})


def _match_token_frame(normalized: str, config_path: str) -> pl.DataFrame:
    cols = [
        "record_id", "provider", "country_code_norm", "h3_7", "h3_8", "h3_9",
        "lat_norm", "lng_norm", "lat_lng_low_precision",
        "property_name_norm", "property_name_core",
        "property_name_tokens", "property_name_core_tokens", "property_name_blocking_tokens",
        "property_name_brand_tokens",
        "property_name_signature", "phone_last10_non_reused_list", "email_norm_list",
        "website_reused_flag", "website_weak_domain_flag", "website_domain_norm",
        "star_rating_norm", "city_name_norm", "postal_code_final", "hotel_chain_norm",
        "property_type_norm", "area_clean", "state_norm", "country_name_clean",
    ]
    df = pl.read_parquet(normalized, columns=cols)
    config = load_config(config_path)
    run_dir = Path(normalized).parent
    token_stats = {
        "global": pl.read_parquet(run_dir / "name_token_stats_global.parquet"),
        "city": pl.read_parquet(run_dir / "name_token_stats_city.parquet"),
    }
    policy = _load_effective_policy(normalized, config_path)
    global_stop = build_match_global_stoplist(token_stats, config)
    city_over = build_city_overcommon_token_map(df, token_stats, config)
    area_keep = build_match_area_identity_keep(token_stats, config)
    min_len = config.get("tokens", {}).get("match_min_token_length", 4)
    return add_match_token_features(df, policy, global_stop, city_over, min_len, area_keep)


def _prep(
    con: duckdb.DuckDBPyConnection,
    normalized: str,
    version: str = "v2",
    config_path: str = "configs/india.yaml",
) -> int:
    if version == "v2_1":
        con.register("_norm_source", _match_token_frame(normalized, config_path))
        source = "_norm_source"
        match_cols = """
            CASE WHEN len(property_name_match_tokens) = 0 THEN NULL
                 ELSE property_name_match_tokens END AS property_name_match_tokens,
            NULLIF(property_name_match_signature, '') AS property_name_match_signature,
        """
    else:
        source = f"read_parquet('{normalized}')"
        match_cols = ""

    # 'unknown' is the property_type mapping's fallback sentinel, not a type:
    # comparing it manufactures evidence both ways (unknown!=hotel reads as a
    # mismatch ~12x against; unknown==unknown reads as agreement). NULL it.
    nullifs = ", ".join(
        f"NULLIF(NULLIF({c}, ''), 'unknown') AS {c}" if c == "property_type_norm"
        else f"NULLIF({c}, '') AS {c}"
        for c in _NULLIF_STR
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE prepped AS
        SELECT
            record_id, provider, country_code_norm, h3_7, h3_8, h3_9,
            -- 2-decimal coords are ~1km of fake precision: as a distance
            -- comparison they manufacture NEGATIVE evidence against true
            -- matches. NULL them (null distance level = neutral); h3/blocking
            -- keys are unaffected.
            CASE WHEN COALESCE(lat_lng_low_precision, FALSE) THEN NULL ELSE lat_norm END AS lat_norm,
            CASE WHEN COALESCE(lat_lng_low_precision, FALSE) THEN NULL ELSE lng_norm END AS lng_norm,
            property_name_norm, property_name_core, property_name_blocking_tokens,
            -- empty core-token list -> NULL so the name comparison's null level fires
            CASE WHEN len(property_name_core_tokens) = 0 THEN NULL
                 ELSE property_name_core_tokens END AS property_name_core_tokens,
            {match_cols}
            phone_last10_non_reused_list,
            -- scalar anchor for EM training (block_on cannot explode arrays)
            list_extract(phone_last10_non_reused_list, 1) AS primary_phone,
            email_norm_list,
            COALESCE(website_reused_flag, FALSE) AS website_reused_flag,
            COALESCE(website_weak_domain_flag, FALSE) AS website_weak_domain_flag,
            star_rating_norm,
            {nullifs}
        FROM {source}
        """
    )
    return con.execute("SELECT COUNT(*) FROM prepped").fetchone()[0]


def _haversine_sql(a: str, b: str) -> str:
    return (
        f"6371000 * acos(LEAST(1, GREATEST(-1, "
        f"sin(radians({a}.lat_norm))*sin(radians({b}.lat_norm)) + "
        f"cos(radians({a}.lat_norm))*cos(radians({b}.lat_norm))*"
        f"cos(radians({a}.lng_norm - {b}.lng_norm)))))"
    )


def _audit(con: duckdb.DuckDBPyConnection, pred: str, out: Path, version: str) -> dict:
    """Build per-pair feature table, then crosstabs + safety metrics."""
    token_col = "property_name_match_tokens" if version == "v2_1" else "property_name_core_tokens"
    sig_col = "property_name_match_signature" if version == "v2_1" else "property_name_signature"
    jac = (
        f"len(list_intersect(l.{token_col}, r.{token_col}))::DOUBLE "
        f"/ len(list_distinct(list_concat(l.{token_col}, r.{token_col})))"
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE scored_aug AS
        SELECT
            p.match_probability AS mp,
            p.match_key,
            (l.{sig_col} = r.{sig_col}) AS sig_exact,
            CASE WHEN l.{token_col} IS NULL OR r.{token_col} IS NULL
                 THEN NULL ELSE {jac} END AS name_jac,
            len(list_intersect(l.phone_last10_non_reused_list, r.phone_last10_non_reused_list)) > 0 AS phone_ov,
            len(list_intersect(COALESCE(l.email_norm_list, []), COALESCE(r.email_norm_list, []))) > 0 AS email_ov,
            (l.website_domain_norm = r.website_domain_norm
                 AND NOT (l.website_reused_flag OR r.website_reused_flag)
                 AND NOT (l.website_weak_domain_flag OR r.website_weak_domain_flag)) AS domain_ok,
            {_haversine_sql('l', 'r')} AS dist_m
        FROM {pred} p
        JOIN prepped l ON p.record_id_l = l.record_id
        JOIN prepped r ON p.record_id_r = r.record_id
        WHERE p.match_probability >= {PERSIST_FLOOR}
        """
    )
    # derived view: bands, name level, identity gate, location-only
    band_case = " ".join(f"WHEN mp >= {lo} AND mp < {hi} THEN '{n}'" for n, lo, hi in _BANDS)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE aug AS
        SELECT *,
            CASE {band_case} END AS band,
            COALESCE(sig_exact, FALSE) OR COALESCE(name_jac >= 0.45, FALSE) AS meaningful_name,
            CASE WHEN sig_exact THEN 'signature'
                 WHEN name_jac >= 0.85 THEN 'jac85'
                 WHEN name_jac >= 0.65 THEN 'jac65'
                 WHEN name_jac >= 0.45 THEN 'jac45'
                 WHEN name_jac > 0 THEN 'shared'
                 WHEN name_jac = 0 THEN 'none'
                 ELSE 'no_name' END AS name_level,
            CASE WHEN dist_m <= 25 THEN 'le25m' WHEN dist_m <= 75 THEN 'le75m'
                 WHEN dist_m <= 200 THEN 'le200m' WHEN dist_m <= 500 THEN 'le500m'
                 WHEN dist_m IS NULL THEN 'missing' ELSE 'gt500m' END AS dist_bucket
        FROM scored_aug
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE aug2 AS
        SELECT *,
            -- clean boolean: each term can be null when its list/domain is missing;
            -- coalesce all so NOT identity_gate exactly counts location-only pairs
            (meaningful_name
             OR COALESCE(phone_ov, FALSE)
             OR COALESCE(email_ov, FALSE)
             OR COALESCE(domain_ok, FALSE)) AS identity_gate
        FROM aug
        """
    )

    def dump(name: str, sql: str):
        con.execute(sql).pl().write_parquet(out / name)

    dump("audit_band_x_gate.parquet",
         "SELECT band, identity_gate, COUNT(*) c FROM aug2 GROUP BY 1,2 ORDER BY 1 DESC,2")
    dump("audit_band_x_name_level.parquet",
         "SELECT band, name_level, COUNT(*) c FROM aug2 GROUP BY 1,2 ORDER BY 1 DESC")
    dump("audit_band_x_phone.parquet",
         "SELECT band, phone_ov, COUNT(*) c FROM aug2 GROUP BY 1,2 ORDER BY 1 DESC")
    dump("audit_band_x_distance.parquet",
         "SELECT band, dist_bucket, COUNT(*) c FROM aug2 GROUP BY 1,2 ORDER BY 1 DESC")

    g = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE mp >= 0.99) AS ge_099,
            COUNT(*) FILTER (WHERE mp >= 0.95) AS ge_095,
            COUNT(*) FILTER (WHERE mp >= 0.95 AND identity_gate) AS ge_095_gated,
            COALESCE(MAX(mp) FILTER (WHERE NOT identity_gate), 0) AS loc_only_max_score,
            COUNT(*) FILTER (WHERE NOT identity_gate AND mp >= 0.50) AS loc_only_ge_050,
            COUNT(*) FILTER (WHERE NOT identity_gate AND mp >= 0.90) AS loc_only_ge_090,
            COUNT(*) FILTER (WHERE sig_exact) AS sig_exact_pairs,
            COALESCE(quantile_cont(mp, 0.05) FILTER (WHERE sig_exact), 0) AS sig_exact_p05_score,
            COALESCE(MIN(mp) FILTER (WHERE sig_exact), 0) AS sig_exact_min_score
        FROM aug2
        """
    ).pl().to_dicts()[0]
    g["pct_ge_095_with_identity"] = (g["ge_095_gated"] / g["ge_095"]) if g["ge_095"] else None
    return g


def _band_samples(con: duckdb.DuckDBPyConnection, pred: str, out: Path):
    for name, lo, hi in _BANDS:
        if name == "lt_0_01":
            continue
        con.execute(
            f"""
            COPY (
                SELECT p.match_probability, p.match_weight, p.match_key,
                       p.record_id_l, p.record_id_r, p.provider_l, p.provider_r,
                       l.property_name_norm AS name_l, r.property_name_norm AS name_r,
                       l.property_name_core AS core_l, r.property_name_core AS core_r,
                       l.city_name_norm AS city_l, r.city_name_norm AS city_r,
                       ROUND({_haversine_sql('l', 'r')}, 0) AS distance_m,
                       len(list_intersect(l.phone_last10_non_reused_list,
                                          r.phone_last10_non_reused_list)) AS phone_shared
                FROM {pred} p
                JOIN prepped l ON p.record_id_l = l.record_id
                JOIN prepped r ON p.record_id_r = r.record_id
                WHERE p.match_probability >= {lo} AND p.match_probability < {hi}
                USING SAMPLE {SAMPLE_PER_BAND} ROWS
            ) TO '{out / f"splink_pairs_band_{name}.parquet"}' (FORMAT parquet)
            """
        )


def run(normalized: str, output_dir: Path, version: str, config_path: str) -> dict:
    timings: dict[str, float] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    t = time.time()
    n_records = _prep(con, normalized, version, config_path)
    timings["prep_s"] = round(time.time() - t, 1)
    print(f"[splink-{version}] prepped {n_records:,} records ({timings['prep_s']}s)")

    if version == "v2_1":
        settings = build_settings_v2_1()
        em_rules = em_training_rules_v2()
    elif version == "v2":
        settings = build_settings_v2()
        em_rules = em_training_rules_v2()
    else:
        settings = build_settings()
        em_rules = [block_on("city_name_norm", "property_name_signature"), block_on("h3_9")]

    db_api = DuckDBAPI(connection=con)
    linker = Linker("prepped", settings, db_api=db_api)

    t = time.time()
    linker.training.estimate_u_using_random_sampling(max_pairs=2_000_000)
    timings["estimate_u_s"] = round(time.time() - t, 1)

    t = time.time()
    for rule in em_rules:
        linker.training.estimate_parameters_using_expectation_maximisation(rule)
    timings["estimate_m_s"] = round(time.time() - t, 1)
    print(f"[splink-{version}] trained (u {timings['estimate_u_s']}s, EM {timings['estimate_m_s']}s)")

    linker.misc.save_model_to_json(str(output_dir / f"splink_settings_{version}.json"), overwrite=True)

    t = time.time()
    df_pred = linker.inference.predict()
    pred = df_pred.physical_name
    n_pairs = con.execute(f"SELECT COUNT(*) FROM {pred}").fetchone()[0]
    timings["predict_s"] = round(time.time() - t, 1)
    print(f"[splink-{version}] predicted {n_pairs:,} pairs ({timings['predict_s']}s)")

    case = " ".join(f"WHEN match_probability >= {lo} AND match_probability < {hi} THEN '{n}'"
                    for n, lo, hi in _BANDS)
    dist = con.execute(
        f"SELECT band, COUNT(*) pair_count FROM "
        f"(SELECT *, CASE {case} END band FROM {pred}) GROUP BY band ORDER BY band DESC"
    ).pl()
    dist.write_parquet(output_dir / "splink_score_distribution.parquet")
    print(f"[splink-{version}] score distribution:")
    print(dist)

    con.execute(
        f"COPY (SELECT * FROM {pred} WHERE match_probability >= {PERSIST_FLOOR}) "
        f"TO '{output_dir / f'splink_scored_pairs_{version}.parquet'}' (FORMAT parquet)"
    )

    t = time.time()
    audit = _audit(con, pred, output_dir, version)
    _band_samples(con, pred, output_dir)
    timings["audit_s"] = round(time.time() - t, 1)
    print(f"[splink-{version}] audit ({timings['audit_s']}s):")
    for k, v in audit.items():
        print(f"        {k} = {v}")

    runtime = {
        "version": version,
        "n_records": int(n_records),
        "n_scored_pairs": int(n_pairs),
        "timings_s": timings,
        "total_s": round(sum(timings.values()), 1),
        "score_distribution": dist.to_dicts(),
        "audit": audit,
        "guardrails": {"pairs_warn": n_pairs > GUARD_PAIRS_WARN},
    }
    (output_dir / "splink_runtime.json").write_text(json.dumps(runtime, indent=2, default=str))
    print(f"[splink-{version}] total {runtime['total_s']}s -> {output_dir}")
    return runtime


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Splink scoring trial + audit")
    p.add_argument("--normalized", required=True)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--version", default="v2", choices=["v1", "v2", "v2_1"])
    p.add_argument("--config", default="configs/india.yaml")
    args = p.parse_args(argv)
    out = Path(args.output_dir) if args.output_dir else Path(args.normalized).parent / f"splink_{args.version}"
    run(args.normalized, out, args.version, args.config)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
