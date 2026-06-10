"""Stage 1 — ingest + schema standardization.

Each provider feed lives in its own NDJSON file but the downloader already coerced
them into a shared column set, so standardization here is: read every provider file
for the country, cast each column to a canonical dtype (provider feeds disagree on
e.g. property_code int-vs-string), add provenance columns, and concat.

Raw values are preserved verbatim; later stages derive *new* columns and never
mutate these.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

# Canonical dtype per raw column. Anything not listed is dropped at ingest.
STRING_COLS = [
    "property_code",
    "property_name",
    "address_lines",
    "city_name",
    "city_code",
    "state",
    "country_code",
    "country_name",
    "postal_code",
    "property_type",
    "phone_numbers",
    "emails",
    "fax_numbers",
    "hotel_chain",
    "amenities",
    "check_in_time",
    "check_out_time",
    "thumbnail",
    "weburl",
    "land_mark",
    "area",
    "geohash6",
    "updated_at",
    # provider-only extras (null where absent)
    "country_code_iso2",
    "hotel_id",
    "source_credential",
]
FLOAT_COLS = ["lat", "lng", "star_rating", "average_of_rating"]
INT_COLS = ["number_of_rooms", "total_reviews"]

CANONICAL: dict[str, pl.DataType] = {
    **{c: pl.Utf8 for c in STRING_COLS},
    **{c: pl.Float64 for c in FLOAT_COLS},
    **{c: pl.Int64 for c in INT_COLS},
}


def _scan_provider(raw_dir: Path, provider: str) -> pl.LazyFrame | None:
    """Scan one provider NDJSON, cast to the canonical schema, add provider/row idx.

    Returns None if the file is missing or empty (e.g. gogobal IN).
    """
    path = raw_dir / f"{provider}_property_info.ndjson"
    if not path.exists() or path.stat().st_size == 0:
        return None

    lf = pl.scan_ndjson(path, infer_schema_length=2000, ignore_errors=True)
    present = set(lf.collect_schema().names())

    # Cast present columns; synthesize missing ones as typed nulls.
    select_exprs: list[pl.Expr] = []
    for col, dtype in CANONICAL.items():
        if col in present:
            select_exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))
        else:
            select_exprs.append(pl.lit(None, dtype=dtype).alias(col))

    return (
        lf.select(select_exprs)
        .with_columns(
            pl.lit(provider).alias("provider"),
            pl.int_range(0, pl.len(), dtype=pl.Int64).alias("source_row_number"),
        )
    )


def standardize_schema(config: dict[str, Any], run_id: str) -> pl.LazyFrame:
    """Read all configured providers for the country into one standardized frame."""
    raw_dir = Path(config["paths"]["raw_dir"])
    providers = config["providers"]
    version = config["normalization_version"]

    frames: list[pl.LazyFrame] = []
    for provider in providers:
        lf = _scan_provider(raw_dir, provider)
        if lf is not None:
            frames.append(lf)

    if not frames:
        raise RuntimeError(f"No non-empty provider files found in {raw_dir}")

    combined = pl.concat(frames, how="diagonal_relaxed")

    # record_id = provider :: property_code :: source_row_number
    # property_code alone is untrustworthy (cleartrip uses '0', dupes/missing).
    record_id = (
        pl.col("provider")
        + pl.lit("::")
        + pl.col("property_code").fill_null("").cast(pl.Utf8)
        + pl.lit("::")
        + pl.col("source_row_number").cast(pl.Utf8)
    )

    return combined.with_columns(
        record_id.alias("record_id"),
        pl.lit(run_id).alias("normalization_run_id"),
        pl.lit(version).alias("normalization_version"),
    )
