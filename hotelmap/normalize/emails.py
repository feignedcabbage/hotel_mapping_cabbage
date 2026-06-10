"""Email normalization.

Splits multi-value email fields, validates loosely, and extracts domain/local.
Generic local parts (info@, reservations@, ...) are weak identity signals and get
flagged. Reuse is also flagged: an address shared across many hotels is a shared
inbox, not an identity.
"""

from __future__ import annotations

import re
from typing import Any

import polars as pl

_SPLIT = re.compile(r"[,;/\s]+")
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def add_email_features(df: pl.DataFrame, config: dict[str, Any]) -> pl.DataFrame:
    generic = config.get("generic_email_locals", [])
    reuse_min = config.get("reuse", {}).get("email_reuse_min", 5)

    # Native multi-delimiter split: replace separators with comma, then split.
    norm_list = (
        pl.col("emails")
        .fill_null("")
        .str.to_lowercase()
        .str.replace_all(r"[;/\s]+", ",")
        .str.split(",")
        .list.eval(pl.element().filter(pl.element().str.contains(r"^[^@]+@[^@]+\.[^@]+$")))
    )

    df = df.with_columns(
        pl.col("emails").alias("emails_raw"),
        norm_list.alias("email_norm_list"),
    ).with_columns(
        pl.col("email_norm_list")
        .list.eval(pl.element().str.split("@").list.last())
        .alias("email_domain_list"),
        pl.col("email_norm_list")
        .list.eval(pl.element().str.split("@").list.first())
        .alias("email_local_list"),
    ).with_columns(
        pl.col("email_local_list")
        .list.eval(pl.element().is_in(generic))
        .list.all()
        .fill_null(False)
        .alias("email_generic_flag")
    )

    # reuse on full address
    counts = (
        df.select("record_id", "email_norm_list")
        .explode("email_norm_list")
        .drop_nulls("email_norm_list")
        .rename({"email_norm_list": "e"})
        .unique(["record_id", "e"])
        .group_by("e")
        .agg(pl.len().alias("n"))
    )
    reused = counts.filter(pl.col("n") >= reuse_min).get_column("e").to_list()

    return df.with_columns(
        pl.col("email_norm_list")
        .list.eval(pl.element().is_in(reused))
        .list.any()
        .fill_null(False)
        .alias("email_reused_flag")
    )
