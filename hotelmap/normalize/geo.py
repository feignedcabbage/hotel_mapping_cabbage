"""Stage 4 — geo normalization.

Produces locality blocking keys (geohash 5/6/7, h3 7/8/9) and quality flags.
Bad coordinates are flagged, never corrected (normalization v1 rule). geohash and
h3 for all resolutions are computed in a single UDF pass over (lat, lng) to avoid
re-walking the frame per resolution.
"""

from __future__ import annotations

from typing import Any

import h3
import polars as pl

_GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def _encode_geohash(lat: float, lng: float, precision: int = 7) -> str:
    """Standard geohash encoder (precision = number of base32 chars)."""
    lat_lo, lat_hi = -90.0, 90.0
    lng_lo, lng_hi = -180.0, 180.0
    geohash: list[str] = []
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    even = True
    while len(geohash) < precision:
        if even:
            mid = (lng_lo + lng_hi) / 2
            if lng > mid:
                ch |= bits[bit]
                lng_lo = mid
            else:
                lng_hi = mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if lat > mid:
                ch |= bits[bit]
                lat_lo = mid
            else:
                lat_hi = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(_GEOHASH_BASE32[ch])
            bit = 0
            ch = 0
    return "".join(geohash)


def _decimal_places(value: float, max_dp: int = 7) -> int:
    for dp in range(max_dp + 1):
        if round(value, dp) == value:
            return dp
    return max_dp


def _precision_bucket(lat: float, lng: float) -> str:
    dp = min(_decimal_places(lat), _decimal_places(lng))
    if dp <= 1:
        # whole-degree-ish coordinates are almost always junk/placeholder
        return "suspicious_rounded"
    return f"precision_{dp}dp"


_GEO_STRUCT = pl.Struct(
    {
        "geohash7": pl.Utf8,
        "h3_7": pl.Utf8,
        "h3_8": pl.Utf8,
        "h3_9": pl.Utf8,
        "precision_bucket": pl.Utf8,
    }
)


def _geo_cells(row: dict[str, Any]) -> dict[str, Any]:
    lat = row["lat"]
    lng = row["lng"]
    if lat is None or lng is None or not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        return {"geohash7": None, "h3_7": None, "h3_8": None, "h3_9": None, "precision_bucket": None}
    return {
        "geohash7": _encode_geohash(lat, lng, 7),
        "h3_7": str(h3.latlng_to_cell(lat, lng, 7)),
        "h3_8": str(h3.latlng_to_cell(lat, lng, 8)),
        "h3_9": str(h3.latlng_to_cell(lat, lng, 9)),
        "precision_bucket": _precision_bucket(lat, lng),
    }


def add_geo_features(lf: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
    geo_cfg = config.get("geo", {}) or {}
    lat_min = geo_cfg.get("lat_min", -90.0)
    lat_max = geo_cfg.get("lat_max", 90.0)
    lng_min = geo_cfg.get("lng_min", -180.0)
    lng_max = geo_cfg.get("lng_max", 180.0)

    lat = pl.col("lat")
    lng = pl.col("lng")

    has_coords = lat.is_not_null() & lng.is_not_null()
    is_zero = has_coords & (lat == 0) & (lng == 0)
    in_global = has_coords & lat.is_between(-90, 90) & lng.is_between(-180, 180)
    valid = in_global & ~is_zero
    out_of_range = valid & ~(lat.is_between(lat_min, lat_max) & lng.is_between(lng_min, lng_max))

    lf = lf.with_columns(
        lat.alias("lat_raw"),
        lng.alias("lng_raw"),
        pl.when(valid).then(lat).otherwise(None).alias("lat_norm"),
        pl.when(valid).then(lng).otherwise(None).alias("lng_norm"),
        valid.alias("lat_lng_valid"),
        is_zero.alias("lat_lng_zero"),
        out_of_range.alias("lat_lng_out_of_range"),
    )

    # Single UDF pass for geohash7 + h3 cells + precision bucket.
    geo = pl.struct(["lat_norm", "lng_norm"]).map_elements(
        lambda r: _geo_cells({"lat": r["lat_norm"], "lng": r["lng_norm"]}),
        return_dtype=_GEO_STRUCT,
    )
    lf = lf.with_columns(geo.alias("_geo"))

    lf = lf.with_columns(
        pl.col("_geo").struct.field("geohash7").alias("geohash7"),
        pl.col("_geo").struct.field("geohash7").str.slice(0, 6).alias("geohash6_norm"),
        pl.col("_geo").struct.field("geohash7").str.slice(0, 5).alias("geohash5"),
        pl.col("_geo").struct.field("h3_7").alias("h3_7"),
        pl.col("_geo").struct.field("h3_8").alias("h3_8"),
        pl.col("_geo").struct.field("h3_9").alias("h3_9"),
        pl.col("_geo").struct.field("precision_bucket").alias("coord_precision_bucket"),
        (pl.col("_geo").struct.field("precision_bucket") == "suspicious_rounded").alias(
            "lat_lng_low_precision"
        ),
    ).drop("_geo")

    return lf
