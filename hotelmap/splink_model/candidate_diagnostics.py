"""Stage 09A candidate-pair diagnostics (analytic, explosion-safe).

For one blocking rule we materialize an `elig(record_id, provider, block_key)` temp
table, then derive everything from per-(block, provider) counts:

  pairs_in_block = (S^2 - sum_i c_i^2) / 2     # cross-provider pairs only

No pair is ever enumerated for counting, so a giant block is measured, not
exploded. The self-join appears only in `sample_pairs`, and is bounded to small
blocks. All functions operate on a DuckDB connection that already has a `hotels`
view over the normalized parquet.
"""

from __future__ import annotations

import duckdb
import pandas as pd

from hotelmap.splink_model.blocking_rules import BlockingRule


DEFAULT_POSTAL_VALIDATION_REGEX = r"^[1-9][0-9]{5}$"  # India PIN (original behavior)


def build_eligible(
    con: duckdb.DuckDBPyConnection,
    rule: BlockingRule,
    reuse_min: int,
    postal_regex: str = DEFAULT_POSTAL_VALIDATION_REGEX,
) -> int:
    # token replace (not str.format) — some rules contain literal SQL braces, e.g.
    # the regexp quantifier {5} in the postal validation regex.
    sql = rule.eligible_sql.replace("{reuse_min}", str(reuse_min)).replace(
        "{postal_regex}", postal_regex
    )
    con.execute("CREATE OR REPLACE TEMP TABLE elig AS " + sql)

    # per (block, provider) counts -> the basis for all analytic metrics
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE bpc AS
        SELECT block_key, provider, COUNT(*) AS cnt
        FROM elig GROUP BY block_key, provider
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE blocks AS
        SELECT
            block_key,
            SUM(cnt) AS n_records,
            COUNT(*) AS n_providers,
            (SUM(cnt) * SUM(cnt) - SUM(cnt * cnt)) / 2 AS candidate_pairs
        FROM bpc GROUP BY block_key
        """
    )
    return con.execute("SELECT COUNT(*) FROM elig").fetchone()[0]


def rule_summary(con: duckdb.DuckDBPyConnection, rule: BlockingRule, eligible_records: int) -> dict:
    agg = con.execute(
        """
        SELECT
            COALESCE(SUM(candidate_pairs), 0) AS candidate_pair_count,
            COUNT(*) AS n_blocks,
            COUNT(*) FILTER (WHERE candidate_pairs > 0) AS n_blocks_with_pairs,
            COALESCE(MAX(candidate_pairs), 0) AS max_block_pairs,
            COALESCE(MAX(n_records), 0) AS max_block_records,
            CAST(quantile_cont(n_records, 0.5) AS DOUBLE) AS median_block_records,
            CAST(quantile_cont(n_records, 0.95) AS DOUBLE) AS p95_block_records,
            CAST(quantile_cont(n_records, 0.99) AS DOUBLE) AS p99_block_records,
            CAST(quantile_cont(candidate_pairs, 0.95) AS DOUBLE) AS p95_block_pairs,
            CAST(quantile_cont(candidate_pairs, 0.99) AS DOUBLE) AS p99_block_pairs
        FROM blocks
        WHERE candidate_pairs > 0
        """
    ).fetchdf().iloc[0].to_dict()

    # records that land in at least one cross-provider pair
    covered = con.execute(
        """
        SELECT COALESCE(SUM(bpc.cnt), 0)
        FROM bpc JOIN blocks USING (block_key)
        WHERE blocks.n_records - bpc.cnt > 0
        """
    ).fetchone()[0]

    # for geo rules: share of eligible records sitting on invalid/out-of-range coords
    invalid_geo_share = None
    if rule.is_geo:
        invalid_geo_share = con.execute(
            """
            SELECT AVG(CASE WHEN h.lat_lng_valid THEN 0.0 ELSE 1.0 END)
            FROM elig e JOIN hotels h ON e.record_id = h.record_id
            """
        ).fetchone()[0]

    out = {
        "rule_name": rule.name,
        "description": rule.description,
        "eligible_records": int(eligible_records),
        "candidate_pair_count": int(agg["candidate_pair_count"]),
        "distinct_records_covered": int(covered),
        "coverage_rate_of_eligible": (covered / eligible_records) if eligible_records else 0.0,
        "n_blocks": int(agg["n_blocks"]),
        "n_blocks_with_pairs": int(agg["n_blocks_with_pairs"]),
        "median_block_records": agg["median_block_records"],
        "p95_block_records": agg["p95_block_records"],
        "p99_block_records": agg["p99_block_records"],
        "max_block_records": int(agg["max_block_records"]),
        "p95_block_pairs": agg["p95_block_pairs"],
        "p99_block_pairs": agg["p99_block_pairs"],
        "max_block_pairs": int(agg["max_block_pairs"]),
        "invalid_coord_share_of_eligible": invalid_geo_share,
    }
    return out


def provider_pair_counts(con: duckdb.DuckDBPyConnection, rule_name: str) -> pd.DataFrame:
    return con.execute(
        f"""
        SELECT
            '{rule_name}' AS rule_name,
            a.provider AS l_provider,
            b.provider AS r_provider,
            SUM(a.cnt * b.cnt) AS candidate_pair_count
        FROM bpc a
        JOIN bpc b ON a.block_key = b.block_key AND a.provider < b.provider
        GROUP BY 1, 2, 3
        ORDER BY candidate_pair_count DESC
        """
    ).fetchdf()


def block_sizes(con: duckdb.DuckDBPyConnection, rule_name: str) -> pd.DataFrame:
    return con.execute(
        f"""
        SELECT '{rule_name}' AS rule_name, block_key, n_records, n_providers, candidate_pairs
        FROM blocks WHERE candidate_pairs > 0
        ORDER BY candidate_pairs DESC
        """
    ).fetchdf()


def large_blocks(con: duckdb.DuckDBPyConnection, rule_name: str, top_n: int, members_per_block: int) -> pd.DataFrame:
    return con.execute(
        f"""
        WITH top AS (
            SELECT block_key, candidate_pairs, n_records, n_providers
            FROM blocks WHERE candidate_pairs > 0
            ORDER BY candidate_pairs DESC LIMIT {top_n}
        )
        SELECT '{rule_name}' AS rule_name, t.block_key, t.candidate_pairs, t.n_records, t.n_providers,
               h.provider, h.property_name_norm, h.property_name_core, h.city_name_norm,
               h.postal_code_final, h.lat_norm, h.lng_norm
        FROM top t
        JOIN elig e USING (block_key)
        JOIN hotels h ON e.record_id = h.record_id
        QUALIFY row_number() OVER (PARTITION BY t.block_key ORDER BY random()) <= {members_per_block}
        """
    ).fetchdf()


def token_contributions(con: duckdb.DuckDBPyConnection, rule_name: str) -> pd.DataFrame:
    """For name-token rules: how many candidate pairs each shared token contributes.

    The token is the last '|' segment of block_key. A token dominating the rule (or
    an obviously generic one near the top) means the blocking-token stoplist needs
    tightening.
    """
    return con.execute(
        f"""
        SELECT
            '{rule_name}' AS rule_name,
            regexp_extract(block_key, '([^|]*)$', 1) AS token,
            SUM(candidate_pairs) AS candidate_pairs,
            COUNT(*) AS n_blocks
        FROM blocks
        WHERE candidate_pairs > 0
        GROUP BY token
        ORDER BY candidate_pairs DESC
        """
    ).fetchdf()


def sample_pairs(con: duckdb.DuckDBPyConnection, rule_name: str, limit: int, max_block_pairs: int) -> pd.DataFrame:
    """Bounded actual pairs for human inspection (small/medium blocks only)."""
    return con.execute(
        f"""
        WITH sampled_blocks AS (
            SELECT block_key FROM blocks
            WHERE candidate_pairs BETWEEN 1 AND {max_block_pairs}
        ),
        e AS (SELECT * FROM elig WHERE block_key IN (SELECT block_key FROM sampled_blocks))
        SELECT
            '{rule_name}' AS rule_name,
            l.provider AS l_provider, r.provider AS r_provider,
            hl.property_name_norm AS l_property_name_norm, hr.property_name_norm AS r_property_name_norm,
            hl.property_name_core AS l_property_name_core, hr.property_name_core AS r_property_name_core,
            hl.city_name_norm AS l_city_name_norm, hr.city_name_norm AS r_city_name_norm,
            hl.postal_code_final AS l_postal_code_final, hr.postal_code_final AS r_postal_code_final,
            hl.lat_norm AS l_lat_norm, hl.lng_norm AS l_lng_norm,
            hr.lat_norm AS r_lat_norm, hr.lng_norm AS r_lng_norm,
            hl.h3_8 AS l_h3_8, hr.h3_8 AS r_h3_8,
            hl.geohash6_norm AS l_geohash6_norm, hr.geohash6_norm AS r_geohash6_norm,
            hl.phone_last10_list AS l_phone_last10_list, hr.phone_last10_list AS r_phone_last10_list
        FROM e l
        JOIN e r ON l.block_key = r.block_key AND l.provider < r.provider
        JOIN hotels hl ON l.record_id = hl.record_id
        JOIN hotels hr ON r.record_id = hr.record_id
        USING SAMPLE {limit} ROWS
        """
    ).fetchdf()
