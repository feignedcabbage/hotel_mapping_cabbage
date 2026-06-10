"""Stage 09C — gated threshold analysis (NO clustering).

Reads v2 scored pairs, applies the identity gate (with a *reason* label), and
profiles the GATED pairs by fine score band so a clerical-review threshold policy
can be chosen from evidence, not guesswork.

Identity gate reason (priority order; gate passes iff reason != 'none'):
    phone                 shared non-reused phone        (strong; tolerates weak name)
    exact_email           shared exact email
    non_reused_domain     shared strong (non-OTA/non-reused) website domain
    signature             exact sorted-core-name signature
    strong_name           Jaccard core tokens >= 0.85
    medium_name_plus_geo  Jaccard core in [0.45,0.85) AND distance <= 200m
    none                  fails gate (location/context only, or weak name w/o geo)

Note medium name alone does NOT pass — a name-only pair needs geo support, because
dense Indian markets put many unrelated hotels at near-identical coordinates.

Outputs in <run>/splink_v2/threshold/:
    threshold_band_summary.parquet     per-band feature profile
    threshold_band_x_reason.parquet
    threshold_band_x_namelevel.parquet
    threshold_provider_pairs.parquet
    threshold_samples_<band>.parquet   review samples per band
    gated_pairs.parquet                gated edges + reason (input to 09D clustering)
    threshold_analysis.json            headline counts + policy hypothesis
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import polars as pl

from hotelmap.splink_model.run_splink_scoring import _haversine_sql, _prep

# finer bands for threshold setting (descending)
BANDS = [
    ("ge_0_995", 0.995, 1.01),
    ("b_099_0995", 0.99, 0.995),
    ("b_098_099", 0.98, 0.99),
    ("b_095_098", 0.95, 0.98),
    ("b_090_095", 0.90, 0.95),
    ("b_080_090", 0.80, 0.90),
]
ANALYSIS_FLOOR = 0.80
SAMPLE_PER_BAND = 250

# gate reason priority (SQL CASE)
_REASON_SQL = """
    CASE
        WHEN phone_ov THEN 'phone'
        WHEN email_ov THEN 'exact_email'
        WHEN domain_ok THEN 'non_reused_domain'
        WHEN sig_exact THEN 'signature'
        WHEN name_match_jaccard >= 0.85 THEN 'strong_name'
        WHEN name_match_jaccard >= 0.45 AND dist_m <= 200 THEN 'medium_name_plus_geo'
        ELSE 'none'
    END
"""

_BAND_SQL = " ".join(f"WHEN mp >= {lo} AND mp < {hi} THEN '{n}'" for n, lo, hi in BANDS)


def _build_features(con: duckdb.DuckDBPyConnection, scored: str, version: str) -> None:
    token_col = "property_name_match_tokens" if version == "v2_1" else "property_name_core_tokens"
    sig_col = "property_name_match_signature" if version == "v2_1" else "property_name_signature"
    jac = (
        f"len(list_intersect(l.{token_col}, r.{token_col}))::DOUBLE "
        f"/ len(list_distinct(list_concat(l.{token_col}, r.{token_col})))"
    )
    con.execute(f"CREATE VIEW scored AS SELECT * FROM read_parquet('{scored}')")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE feat AS
        SELECT
            p.record_id_l, p.record_id_r, p.provider_l, p.provider_r,
            p.match_probability AS mp,
            COALESCE(l.{sig_col} = r.{sig_col}, FALSE) AS sig_exact,
            CASE WHEN l.{token_col} IS NULL OR r.{token_col} IS NULL
                 THEN NULL ELSE {jac} END AS name_match_jaccard,
            COALESCE(len(list_intersect(l.phone_last10_non_reused_list, r.phone_last10_non_reused_list)) > 0, FALSE) AS phone_ov,
            COALESCE(len(list_intersect(COALESCE(l.email_norm_list, []), COALESCE(r.email_norm_list, []))) > 0, FALSE) AS email_ov,
            COALESCE(l.website_domain_norm = r.website_domain_norm
                 AND NOT (l.website_reused_flag OR r.website_reused_flag)
                 AND NOT (l.website_weak_domain_flag OR r.website_weak_domain_flag), FALSE) AS domain_ok,
            {_haversine_sql('l', 'r')} AS dist_m,
            (l.city_name_norm IS NOT DISTINCT FROM r.city_name_norm) AS same_city,
            (l.postal_code_final IS NOT DISTINCT FROM r.postal_code_final
                 AND l.postal_code_final IS NOT NULL) AS same_postal,
            l.property_name_norm AS name_l, r.property_name_norm AS name_r,
            l.city_name_norm AS city_l
        FROM scored p
        JOIN prepped l ON p.record_id_l = l.record_id
        JOIN prepped r ON p.record_id_r = r.record_id
        WHERE p.match_probability >= {ANALYSIS_FLOOR}
        """
    )
    # add reason, band, name_level, gate
    con.execute(
        f"""
        CREATE OR REPLACE TABLE gated AS
        SELECT *,
            {_REASON_SQL} AS gate_reason,
            CASE {_BAND_SQL} END AS band,
            CASE WHEN sig_exact THEN 'signature'
                 WHEN name_match_jaccard >= 0.85 THEN 'jac85'
                 WHEN name_match_jaccard >= 0.65 THEN 'jac65'
                 WHEN name_match_jaccard >= 0.45 THEN 'jac45'
                 WHEN name_match_jaccard > 0 THEN 'shared'
                 WHEN name_match_jaccard = 0 THEN 'none'
                 ELSE 'no_name' END AS name_level
        FROM feat
        """
    )
    con.execute("CREATE OR REPLACE TABLE gated_only AS SELECT * FROM gated WHERE gate_reason <> 'none'")


def _write_threshold_delta(normalized: str, out: Path, report: dict) -> None:
    old_path = Path(normalized).parent / "splink_v2" / "threshold" / "gated_pairs.parquet"
    new_path = out / "gated_pairs_v2_1.parquet"
    if not old_path.exists() or not new_path.exists():
        return

    con = duckdb.connect()
    old = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE gate_reason='medium_name_plus_geo') AS medium_name_plus_geo,
            COUNT(*) FILTER (WHERE match_probability >= 0.99) AS gated_ge_099,
            COUNT(*) FILTER (WHERE match_probability >= 0.80) AS gated_ge_080,
            COUNT(*) FILTER (WHERE match_probability >= 0.90 AND match_probability < 0.99) AS gated_review_090_099
        FROM read_parquet('{old_path}')
        """
    ).fetchone()
    new_medium = con.execute(
        f"""
        SELECT COUNT(*) FROM read_parquet('{new_path}')
        WHERE gate_reason='medium_name_plus_geo'
        """
    ).fetchone()[0]
    new_head = report["headline"]
    rows = [
        {"metric": "medium_name_plus_geo", "v2": old[0], "v2_1": new_medium, "delta": new_medium - old[0]},
        {"metric": "gated_ge_099", "v2": old[1], "v2_1": new_head["gated_ge_099"], "delta": new_head["gated_ge_099"] - old[1]},
        {"metric": "gated_ge_080", "v2": old[2], "v2_1": new_head["gated_ge_080"], "delta": new_head["gated_ge_080"] - old[2]},
        {"metric": "gated_review_090_099", "v2": old[3], "v2_1": new_head["gated_review_090_099"], "delta": new_head["gated_review_090_099"] - old[3]},
    ]
    pl.DataFrame(rows).write_parquet(out / "v2_vs_v2_1_threshold_delta.parquet")


def run(normalized: str, scored: str, out: Path, version: str, config_path: str) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    _prep(con, normalized, version, config_path)
    _build_features(con, scored, version)
    suffix = "_v2_1" if version == "v2_1" else ""

    def dump(name, sql):
        con.execute(sql).pl().write_parquet(out / name)

    # per-band feature profile (gated only)
    dump(
        "threshold_band_summary.parquet",
        """
        SELECT band,
            COUNT(*) AS pair_count,
            COUNT(DISTINCT record_id_l) AS distinct_l,
            COUNT(DISTINCT record_id_r) AS distinct_r,
            ROUND(AVG(phone_ov::INT), 3) AS phone_rate,
            ROUND(AVG(email_ov::INT), 3) AS email_rate,
            ROUND(AVG(domain_ok::INT), 3) AS domain_rate,
            ROUND(AVG(sig_exact::INT), 3) AS signature_rate,
            ROUND(AVG((name_match_jaccard >= 0.85)::INT), 3) AS jac85_rate,
            ROUND(AVG(same_city::INT), 3) AS same_city_rate,
            ROUND(AVG(same_postal::INT), 3) AS same_postal_rate,
            ROUND(median(dist_m)) AS median_dist_m,
            ROUND(quantile_cont(dist_m, 0.95)) AS p95_dist_m
        FROM gated_only GROUP BY band ORDER BY band DESC
        """,
    )
    dump(
        "threshold_band_x_reason.parquet",
        "SELECT band, gate_reason, COUNT(*) c FROM gated_only GROUP BY 1,2 ORDER BY 1 DESC, c DESC",
    )
    dump(
        "threshold_band_x_namelevel.parquet",
        "SELECT band, name_level, COUNT(*) c FROM gated_only GROUP BY 1,2 ORDER BY 1 DESC, c DESC",
    )
    dump(
        "threshold_provider_pairs.parquet",
        """
        SELECT provider_l, provider_r, COUNT(*) c
        FROM gated_only WHERE mp >= 0.90 GROUP BY 1,2 ORDER BY c DESC LIMIT 100
        """,
    )

    # review samples per band (gated)
    for name, lo, hi in BANDS:
        con.execute(
            f"""
            COPY (
                SELECT ROUND(mp,4) mp, gate_reason, name_level,
                       provider_l, provider_r, name_l, name_r, city_l,
                       ROUND(name_match_jaccard, 3) name_match_jaccard,
                       ROUND(dist_m) dist_m, phone_ov, same_postal,
                       record_id_l, record_id_r
                FROM gated_only WHERE band = '{name}'
                USING SAMPLE {SAMPLE_PER_BAND} ROWS
            ) TO '{out / f"threshold_samples_{name}.parquet"}' (FORMAT parquet)
            """
        )

    # persist gated edges (input to 09D clustering)
    con.execute(
        f"""
        COPY (
            SELECT record_id_l, record_id_r, provider_l, provider_r, mp AS match_probability,
                   gate_reason, name_level, ROUND(name_match_jaccard, 3) AS name_match_jaccard,
                   sig_exact, ROUND(dist_m) AS dist_m, phone_ov, same_postal
            FROM gated_only
        ) TO '{out / f"gated_pairs{suffix}.parquet"}' (FORMAT parquet)
        """
    )

    # headline: gate effect + policy hypothesis
    head = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM gated) AS pairs_ge_080,
            (SELECT COUNT(*) FROM gated_only) AS gated_ge_080,
            (SELECT COUNT(*) FROM gated_only WHERE mp >= 0.99) AS gated_ge_099,
            (SELECT COUNT(*) FROM gated_only WHERE mp >= 0.90 AND mp < 0.99) AS gated_review_090_099,
            (SELECT COUNT(*) FROM gated WHERE gate_reason='none') AS failed_gate_ge_080
        """
    ).pl().to_dicts()[0]
    policy = {
        "auto_match (gated AND >=0.99)": int(head["gated_ge_099"]),
        "review (gated AND 0.90-0.99)": int(head["gated_review_090_099"]),
        "rejected_by_gate (>=0.80 but gate fails)": int(head["failed_gate_ge_080"]),
    }
    report = {"headline": head, "policy_hypothesis": policy}
    (out / f"threshold_analysis{suffix}.json").write_text(json.dumps(report, indent=2, default=str))
    if version == "v2_1":
        _write_threshold_delta(normalized, out, report)
    print("[09C] gate-reason policy:", json.dumps(policy, indent=2))
    return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gated threshold analysis (09C)")
    p.add_argument("--normalized", required=True)
    p.add_argument("--scored", required=True, help="splink_scored_pairs_v2.parquet")
    p.add_argument("--output-dir", default=None)
    p.add_argument("--version", default="v2", choices=["v2", "v2_1"])
    p.add_argument("--config", default="configs/india.yaml")
    args = p.parse_args(argv)
    out = Path(args.output_dir) if args.output_dir else Path(args.scored).parent / "threshold"
    run(args.normalized, args.scored, out, args.version, args.config)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
