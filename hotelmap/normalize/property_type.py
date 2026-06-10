"""Stage 10 — property type / star / rating normalization.

Property type is inconsistent: free text ('Hotel', 'Resort') or provider codes
(hotelbeds 'W'/'H', restel '1'/'8'). We translate codes via providers.yaml, then
bucket free text into coarse categories by keyword. Star/rating are normalized but
kept as weak signals (useful to reduce confidence, not to prove a match).
"""

from __future__ import annotations

from typing import Any

import polars as pl


def normalize_property_type(df: pl.DataFrame, config: dict[str, Any]) -> pl.DataFrame:
    providers_cfg = config.get("providers_config", {}) or {}
    type_maps = config.get("property_type_maps", {}) or {}
    keywords: dict[str, list[str]] = type_maps.get("keywords", {})

    # Step 1: translate provider coded values to a free-text category token.
    # Build one big mapping keyed by (provider, raw_code) is awkward in polars; do it
    # per provider with when/then, since only a couple providers use codes.
    coded = pl.col("property_type_clean")
    code_expr = coded
    for provider, pcfg in providers_cfg.items():
        pmap = (pcfg or {}).get("property_type_map")
        if not pmap:
            continue
        # lowercase keys to match property_type_clean
        pmap_l = {str(k).lower(): v for k, v in pmap.items()}
        code_expr = (
            pl.when((pl.col("provider") == provider) & coded.is_in(list(pmap_l.keys())))
            .then(coded.replace_strict(pmap_l, default=None))
            .otherwise(code_expr)
        )

    df = df.with_columns(code_expr.alias("_ptype_resolved"))

    # Step 2: keyword bucket into a coarse category.
    cat_expr = pl.lit("unknown")
    # apply in reverse so earlier (more specific) keys win via overwrite order
    for category, kws in reversed(list(keywords.items())):
        pattern = "|".join(_escape(k) for k in kws)
        cat_expr = (
            pl.when(pl.col("_ptype_resolved").str.contains(pattern))
            .then(pl.lit(category))
            .otherwise(cat_expr)
        )

    df = df.with_columns(
        pl.col("property_type").alias("property_type_raw"),
        pl.when(pl.col("_ptype_resolved").is_null())
        .then(pl.lit("unknown"))
        .otherwise(cat_expr)
        .alias("property_type_norm"),
    ).drop("_ptype_resolved")

    # star rating: treat 0 as missing
    star = pl.col("star_rating")
    df = df.with_columns(
        star.alias("star_rating_raw"),
        pl.when((star > 0) & (star <= 5)).then(star).otherwise(None).alias("star_rating_norm"),
    ).with_columns(
        pl.col("star_rating_norm").round(0).cast(pl.Int8, strict=False).alias("star_rating_bucket"),
        pl.col("star_rating_norm").is_null().alias("star_rating_missing"),
    )

    # average rating: 0.0 likely means unrated
    avg = pl.col("average_of_rating")
    df = df.with_columns(
        avg.alias("average_of_rating_raw"),
        pl.when(avg > 0).then(avg).otherwise(None).alias("average_of_rating_norm"),
    )

    # rooms: 0 -> null; reviews: 0 can be real, keep
    rooms = pl.col("number_of_rooms")
    df = df.with_columns(
        rooms.alias("number_of_rooms_raw"),
        pl.when(rooms > 0).then(rooms).otherwise(None).alias("number_of_rooms_norm"),
        pl.col("total_reviews").alias("total_reviews_raw"),
        pl.col("total_reviews").alias("total_reviews_norm"),
    )

    # hotel chain — null out "not a chain" sentinels
    chain = pl.col("hotel_chain_clean")
    chain_norm = pl.when(chain.is_in(["no chain", "independent", "none", "n/a"])).then(None).otherwise(chain)
    return df.with_columns(
        pl.col("hotel_chain").alias("hotel_chain_raw"),
        chain_norm.alias("hotel_chain_norm"),
    ).with_columns(
        pl.col("hotel_chain_norm").str.split(" ").list.first().alias("hotel_chain_token"),
    )


def _escape(s: str) -> str:
    import re

    return re.escape(s)
