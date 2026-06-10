"""Phone normalization.

Provider phone fields are free text with wild formatting:
  '+919372868698, +919372868698' | '91-471-2212068' | '090990 58189'
  '0091 0 8028544444' | '0' | '0333 777 3653--01224'
We split on separators, parse each candidate with `phonenumbers` (default region
per country), and keep valid numbers as E.164 + last-10. Phone match is high
precision but reuse-aware: a number on hundreds of hotels (reservation centre) is
not hotel identity, so we flag reused numbers.
"""

from __future__ import annotations

import re
from typing import Any

import phonenumbers
import polars as pl

# split on comma/slash/semicolon/pipe/ampersand or runs of 2+ dashes (keep single
# dashes, which appear inside formatted numbers like 91-471-2212068)
_SPLIT = re.compile(r"[,/;|&]+|-{2,}|\s{2,}")
_JUNK = re.compile(r"^[0\s\-+().]*$")  # all zeros / punctuation => junk


def _parse(raw: str | None, region: str) -> dict[str, Any]:
    empty = {"e164_list": [], "last10_list": [], "valid_count": 0, "invalid_count": 0}
    if not raw:
        return empty
    e164: list[str] = []
    last10: list[str] = []
    invalid = 0
    for part in _SPLIT.split(raw):
        part = part.strip()
        if not part or _JUNK.match(part):
            continue
        try:
            num = phonenumbers.parse(part, region)
        except phonenumbers.NumberParseException:
            invalid += 1
            continue
        if phonenumbers.is_valid_number(num):
            e = phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
            if e not in e164:
                e164.append(e)
                last10.append(e[-10:])
        else:
            invalid += 1
    return {
        "e164_list": e164,
        "last10_list": last10,
        "valid_count": len(e164),
        "invalid_count": invalid,
    }


_STRUCT = pl.Struct(
    {
        "e164_list": pl.List(pl.Utf8),
        "last10_list": pl.List(pl.Utf8),
        "valid_count": pl.Int64,
        "invalid_count": pl.Int64,
    }
)


def add_phone_features(df: pl.DataFrame, config: dict[str, Any]) -> pl.DataFrame:
    region = config.get("default_phone_region", "IN")
    reuse_min = config.get("reuse", {}).get("phone_reuse_min", 5)

    parsed = pl.col("phone_numbers").map_elements(
        lambda x: _parse(x, region), return_dtype=_STRUCT
    )
    df = df.with_columns(
        pl.col("phone_numbers").alias("phone_numbers_raw"),
        parsed.alias("_ph"),
    ).with_columns(
        pl.col("_ph").struct.field("e164_list").alias("phone_e164_list"),
        pl.col("_ph").struct.field("last10_list").alias("phone_last10_list"),
        pl.col("_ph").struct.field("valid_count").alias("phone_valid_count"),
        pl.col("_ph").struct.field("invalid_count").alias("phone_invalid_count"),
    ).drop("_ph")

    # reuse: a last-10 number appearing on >= reuse_min distinct records is reused
    counts = (
        df.select("record_id", "phone_last10_list")
        .explode("phone_last10_list")
        .drop_nulls("phone_last10_list")
        .rename({"phone_last10_list": "num"})
        .unique(["record_id", "num"])
        .group_by("num")
        .agg(pl.len().alias("n"))
    )
    reused_nums = counts.filter(pl.col("n") >= reuse_min).get_column("num").to_list()

    return df.with_columns(
        pl.col("phone_last10_list")
        .list.eval(pl.element().is_in(reused_nums))
        .list.any()
        .fill_null(False)
        .alias("phone_reused_flag"),
        # pre-computed for Splink blocking/comparison: reused (shared-line) numbers
        # removed so the matcher never treats a reservation-centre line as identity.
        pl.col("phone_last10_list")
        .list.set_difference(pl.lit(reused_nums, dtype=pl.List(pl.Utf8)))
        .alias("phone_last10_non_reused_list"),
    )
