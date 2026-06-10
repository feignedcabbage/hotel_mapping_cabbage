"""Stage 2 — safe (conservative) text normalization.

Only does: unicode NFKC, trim, lowercase, whitespace collapse, and null-like
standardization. It does NOT strip identity-bearing words — that happens later in
the name/token stages. These `*_clean` columns are the safe base other stages read.
"""

from __future__ import annotations

from typing import Any

import polars as pl
from unidecode import unidecode


def _ascii_fold(value: str | None) -> str | None:
    if value is None:
        return None
    folded = unidecode(value)
    return folded or None


def ascii_expr(col: str) -> pl.Expr:
    """ASCII transliteration (handles Devanagari etc.) via unidecode.

    Used where a name/city may be non-Latin; downstream norm strips to [0-9a-z].
    map_elements is unavoidable here (no native polars transliteration).
    """
    return pl.col(col).map_elements(_ascii_fold, return_dtype=pl.Utf8)

# Values that are effectively null regardless of field.
NULL_LIKE = {
    "",
    "na",
    "n/a",
    "nan",
    "none",
    "null",
    "nil",
    "nas",
    "-",
    "--",
    "n.a",
    "n.a.",
}

# (raw column, clean column) pairs to produce.
CLEAN_FIELDS = [
    ("property_name", "property_name_clean"),
    ("address_lines", "address_clean"),
    ("city_name", "city_clean"),
    ("state", "state_clean"),
    ("country_code", "country_code_clean"),
    ("country_name", "country_name_clean"),
    ("postal_code", "postal_code_clean"),
    ("hotel_chain", "hotel_chain_clean"),
    ("property_type", "property_type_clean"),
    ("area", "area_clean"),
]


def clean_text_expr(col: str) -> pl.Expr:
    """Trim/lowercase/NFKC/collapse-whitespace + map null-likes to null."""
    cleaned = (
        pl.col(col)
        .cast(pl.Utf8)
        .str.normalize("NFKC")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_lowercase()
    )
    # null-like -> null
    return (
        pl.when(cleaned.is_in(list(NULL_LIKE)))
        .then(None)
        .otherwise(cleaned)
        .alias(col)  # caller renames
    )


def add_clean_text_columns(lf: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
    exprs = [clean_text_expr(raw).alias(clean) for raw, clean in CLEAN_FIELDS]
    return lf.with_columns(exprs)
