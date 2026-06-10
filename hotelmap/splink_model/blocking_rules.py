"""Stage 09A blocking rules.

Each rule is expressed as an *eligible-records* query that projects every record
down to (record_id, provider, block_key). Two records are a candidate pair iff they
share a block_key AND come from different providers (cross-provider linkage first —
intra-provider dedupe is a separate, later concern).

Why this shape (and not a raw self-join condition): candidate counts are derived
analytically from per-(block, provider) counts — within a block of size S with
per-provider counts c_i, cross-provider pairs = (S^2 - sum c_i^2) / 2. That never
enumerates pairs, so even a pathologically large block is measured cheaply. The
self-join is used only for bounded human-readable samples.
"""

from __future__ import annotations

from dataclasses import dataclass

# Obvious junk postal codes to exclude from city+postal blocking. The normalizer's
# PIN regex already drops leading-zero codes; this catches repdigit/sequential junk.
JUNK_POSTAL = ["111111", "123456", "100000", "999999", "654321", "121212"]


@dataclass(frozen=True)
class BlockingRule:
    name: str
    description: str
    # SELECT returning exactly: record_id, provider, block_key.
    # May contain `{reuse_min}` / `{postal_regex}` which are substituted at runtime.
    eligible_sql: str
    is_geo: bool = False
    # block_key ends with a name token (last '|' segment) -> run token diagnostics
    has_name_token: bool = False


_JUNK_POSTAL_SQL = ", ".join(f"'{p}'" for p in JUNK_POSTAL)

BLOCKING_RULES: list[BlockingRule] = [
    BlockingRule(
        name="same_country_h3_8",
        description="same country + same H3 res-8 cell (exact cell, no neighbors yet)",
        is_geo=True,
        eligible_sql="""
            SELECT record_id, provider,
                   country_code_norm || '|' || h3_8 AS block_key
            FROM hotels
            WHERE country_code_norm IS NOT NULL
              AND h3_8 IS NOT NULL
        """,
    ),
    BlockingRule(
        name="same_country_geohash6",
        description="same country + same geohash6 cell",
        is_geo=True,
        eligible_sql="""
            SELECT record_id, provider,
                   country_code_norm || '|' || geohash6_norm AS block_key
            FROM hotels
            WHERE country_code_norm IS NOT NULL
              AND geohash6_norm IS NOT NULL
        """,
    ),
    BlockingRule(
        name="same_country_city_postal",
        description="same country + city + postal (locality fallback when coords bad)",
        eligible_sql=f"""
            SELECT record_id, provider,
                   country_code_norm || '|' || city_name_norm || '|' || postal_code_final AS block_key
            FROM hotels
            WHERE country_code_norm IS NOT NULL
              AND city_name_norm IS NOT NULL
              AND postal_code_final IS NOT NULL
              AND regexp_matches(postal_code_final, '{{postal_regex}}')
              AND postal_code_final NOT IN ({_JUNK_POSTAL_SQL})
        """,
    ),
    BlockingRule(
        name="same_country_city_name_signature",
        description="same country + city + sorted core-name signature (coord/postal-free)",
        eligible_sql="""
            SELECT record_id, provider,
                   country_code_norm || '|' || city_name_norm || '|' || property_name_signature AS block_key
            FROM hotels
            WHERE country_code_norm IS NOT NULL
              AND city_name_norm IS NOT NULL
              AND property_name_signature IS NOT NULL
              AND property_name_signature <> ''
        """,
    ),
    BlockingRule(
        name="same_country_city_postal_nametoken",
        description="same country+city+postal + a shared distinctive core name token (safe city_postal replacement)",
        has_name_token=True,
        eligible_sql=f"""
            SELECT record_id, provider,
                   country_code_norm || '|' || city_name_norm || '|' || postal_code_final || '|' || tok AS block_key
            FROM (
                SELECT record_id, provider, country_code_norm, city_name_norm, postal_code_final,
                       UNNEST(property_name_blocking_tokens) AS tok
                FROM hotels
                WHERE country_code_norm IS NOT NULL
                  AND city_name_norm IS NOT NULL
                  AND postal_code_final IS NOT NULL
                  AND regexp_matches(postal_code_final, '{{postal_regex}}')
                  AND postal_code_final NOT IN ({_JUNK_POSTAL_SQL})
            )
            WHERE tok IS NOT NULL
        """,
    ),
    BlockingRule(
        name="shared_non_reused_phone",
        description="shared valid last-10 phone, excluding reused (shared-line) numbers",
        eligible_sql="""
            WITH ph AS (
                SELECT record_id, provider, UNNEST(phone_last10_list) AS phone
                FROM hotels
                WHERE phone_valid_count > 0
            ),
            phone_counts AS (
                SELECT phone, COUNT(DISTINCT record_id) AS n
                FROM ph GROUP BY phone
            )
            SELECT p.record_id, p.provider, p.phone AS block_key
            FROM ph p
            JOIN phone_counts c USING (phone)
            WHERE c.n < {reuse_min}
        """,
    ),
]
