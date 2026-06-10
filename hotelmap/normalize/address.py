"""Stage 8 — address normalization (v1, conservative).

Address text is messy and can create false confidence, so v1 only: expands common
abbreviations, tokenizes, and extracts a postal code from the free text. It
reconciles the postal_code field with any code found in the address and flags
conflicts. No heavy address parsing yet.

Postal patterns are country-specific and config-driven (`postal:` section of the
run config, each pattern's FIRST capture group is the code). Defaults preserve the
original India behavior (6-digit PIN, safe to match mid-string). For countries
whose postal codes collide with street numbers (US 5-digit ZIP, NZ 4-digit) the
address-text pattern should be END-ANCHORED.
"""

from __future__ import annotations

from typing import Any

import polars as pl

# v1 abbreviation expansions (whole-word). Country configs can extend/override
# via `address_abbreviations:` (e.g. US ave/blvd/ste).
ABBREVIATIONS = {
    "rd": "road",
    "st": "street",
    "opp": "opposite",
    "nr": "near",
    "no": "number",
    "bldg": "building",
    "apt": "apartment",
    "flr": "floor",
    "hwy": "highway",
    "jn": "junction",
    "jct": "junction",
}

# tokens too generic to be address identity
ADDRESS_STOPWORDS = {
    "road", "street", "near", "opposite", "number", "building", "floor", "the",
    "and", "main", "cross", "lane", "area", "city", "district", "po", "dist",
}

# India defaults (original behavior)
DEFAULT_POSTAL_FIELD_PATTERN = r"([1-9][0-9]{5})"
DEFAULT_POSTAL_ADDRESS_PATTERN = r"\b([1-9][0-9]{5})\b"


def add_address_features(lf: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
    abbreviations = {**ABBREVIATIONS, **(config.get("address_abbreviations") or {})}
    postal_cfg = config.get("postal") or {}
    field_pattern = postal_cfg.get("field_pattern", DEFAULT_POSTAL_FIELD_PATTERN)
    address_pattern = postal_cfg.get("address_pattern", DEFAULT_POSTAL_ADDRESS_PATTERN)

    norm = pl.col("address_clean")
    for abbr, full in abbreviations.items():
        norm = norm.str.replace_all(rf"\b{abbr}\b", full)
    norm = norm.str.replace_all(r"[^0-9a-z ]+", " ").str.replace_all(r"\s+", " ").str.strip_chars()

    lf = lf.with_columns(
        pl.col("address_lines").alias("address_raw"),
        pl.when(norm == "").then(None).otherwise(norm).alias("address_norm"),
    )

    tokens = (
        pl.col("address_norm")
        .fill_null("")
        .str.split(" ")
        .list.eval(pl.element().filter(pl.element().str.len_chars() >= 2))
    )
    lf = lf.with_columns(tokens.alias("address_tokens")).with_columns(
        pl.col("address_tokens")
        .list.eval(pl.element().filter(~pl.element().is_in(list(ADDRESS_STOPWORDS))))
        .alias("address_core_tokens")
    )

    # postal reconciliation (first capture group of each pattern is the code)
    postal_field = pl.col("postal_code_clean").str.extract(field_pattern, 1)
    postal_from_addr = pl.col("address_norm").str.extract(address_pattern, 1)

    lf = lf.with_columns(
        pl.col("postal_code").alias("postal_code_raw"),
        postal_field.alias("postal_code_norm"),
        postal_from_addr.alias("postal_code_from_address"),
    )

    return lf.with_columns(
        pl.coalesce(["postal_code_norm", "postal_code_from_address"]).alias("postal_code_final"),
        (
            pl.col("postal_code_norm").is_not_null()
            & pl.col("postal_code_from_address").is_not_null()
            & (pl.col("postal_code_norm") != pl.col("postal_code_from_address"))
        ).alias("postal_code_conflict"),
    )
