"""Stage 3 — country / city / state normalization.

Country codes arrive numeric (agoda '35'), null (grnc), or ISO2. We resolve to a
canonical ISO2 via, in priority order: provider's iso2 field, numeric code map,
country-name map. Resolution is *observable*: we keep the raw values and record
which source resolved it (or that it failed). We do not silently coerce.

City/state get only light normalization here — real city alias mapping
(Bengaluru<->Bangalore) is a later, table-driven concern.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from hotelmap.normalize.text import ascii_expr


def _iso2_providers(config: dict[str, Any]) -> list[str]:
    providers_cfg = config.get("providers_config", {}) or {}
    out = []
    for name, cfg in providers_cfg.items():
        if cfg and cfg.get("country_code_field") == "country_code_iso2":
            out.append(name)
    return out


def normalize_country(lf: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
    cmaps = config.get("country_maps", {}) or {}
    numeric_map: dict[str, str] = {str(k): v for k, v in (cmaps.get("numeric_code_map") or {}).items()}
    name_map: dict[str, str] = {str(k).lower(): v for k, v in (cmaps.get("name_map") or {}).items()}

    iso2_providers = _iso2_providers(config)

    # Candidate code: prefer iso2 field for providers that supply it.
    candidate = (
        pl.when(pl.col("provider").is_in(iso2_providers))
        .then(pl.col("country_code_iso2"))
        .otherwise(pl.col("country_code_clean"))
        .cast(pl.Utf8)
        .str.strip_chars()
    )
    candidate_upper = candidate.str.to_uppercase()

    # Three resolution sources, each null when it doesn't apply.
    # Any ISO2-shaped code passes through as-is (a hardcoded whitelist made every
    # new country resolve to 100% null -> zero blocking pairs). Garbage values
    # ('0', 'NAS', numerics) don't match the shape and fall to the other sources.
    pass_hit = (
        pl.when(candidate_upper.str.contains(r"^[A-Z]{2}$"))
        .then(candidate_upper)
        .otherwise(None)
    )
    numeric_hit = (
        candidate.replace_strict(numeric_map, default=None) if numeric_map else pl.lit(None, dtype=pl.Utf8)
    )
    name_hit = (
        pl.col("country_name_clean").replace_strict(name_map, default=None)
        if name_map
        else pl.lit(None, dtype=pl.Utf8)
    )

    country_norm = pl.coalesce([pass_hit, numeric_hit, name_hit])
    country_source = (
        pl.when(pass_hit.is_not_null())
        .then(pl.lit("iso2_passthrough"))
        .when(numeric_hit.is_not_null())
        .then(pl.lit("numeric_map"))
        .when(name_hit.is_not_null())
        .then(pl.lit("name_map"))
        .otherwise(pl.lit("unresolved"))
    )

    expected = str(config.get("country", "")).upper()

    # Optional country-specific state/region canonicalization (state_maps.yaml,
    # keyed by run country). Keys are matched lowercase; values are the canonical
    # lowercase form (e.g. US full names AND 2-letter codes -> the 2-letter code).
    # Unmapped values pass through unchanged — observable, never dropped.
    smap_raw = (config.get("state_maps") or {}).get(expected) or {}
    smap = {str(k).lower(): str(v).lower() for k, v in smap_raw.items()}
    state_norm_expr = pl.col("state_clean")
    if smap:
        state_norm_expr = pl.coalesce(
            [pl.col("state_clean").replace_strict(smap, default=None), pl.col("state_clean")]
        )

    return lf.with_columns(
        # diagnostics: keep raw, record resolution
        pl.col("country_code").alias("country_code_raw"),
        pl.col("country_name").alias("country_name_raw"),
        country_norm.alias("country_code_norm"),
        country_source.alias("country_norm_source"),
        country_norm.is_null().alias("country_norm_warning"),
        # flags a resolved-but-off-country row (e.g. restel Aberdeen leak)
        (country_norm.is_not_null() & (country_norm != expected)).alias(
            "country_off_target"
        ),
        # city/state light normalization
        pl.col("city_clean").alias("city_name_norm"),
        ascii_expr("city_clean").alias("city_ascii"),
        pl.col("city_clean").str.split(" ").list.first().alias("city_token"),
        state_norm_expr.alias("state_norm"),
    )
