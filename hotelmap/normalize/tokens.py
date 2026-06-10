"""Stage 6 — automated token frequency profiling.

Explodes name tokens and computes document frequency (records a token appears in,
counted once per record) at global / provider / city / h3_8 levels. Document
frequency — not raw count — is what tells us whether a token is identity-bearing.
These tables are written every run and feed the token-policy generator.
"""

from __future__ import annotations

from typing import Any

import polars as pl


def explode_name_tokens(df: pl.DataFrame, min_len: int) -> pl.DataFrame:
    return (
        df.select(
            "record_id",
            "provider",
            "country_code_norm",
            "city_name_norm",
            "h3_8",
            "hotel_chain_clean",
            "property_name_tokens",
        )
        .explode("property_name_tokens")
        .rename({"property_name_tokens": "token"})
        .filter(pl.col("token").is_not_null())
        .filter(pl.col("token").str.len_chars() >= min_len)
    )


def build_token_stats(df: pl.DataFrame, config: dict[str, Any]) -> dict[str, pl.DataFrame]:
    min_len = config.get("tokens", {}).get("min_token_length", 2)
    total_records = df.height

    # count each token once per record
    tokens = explode_name_tokens(df, min_len).unique(["record_id", "token"])

    # does the token also show up as a chain word? (brand signal)
    # Exclude "not a chain" sentinels, else "no"/"chain"/"independent" dominate.
    chain_sentinels = ["no chain", "independent", "none", "n/a"]
    chain_tokens = (
        df.select("record_id", "hotel_chain_clean")
        .filter(pl.col("hotel_chain_clean").is_not_null())
        .filter(~pl.col("hotel_chain_clean").is_in(chain_sentinels))
        .with_columns(pl.col("hotel_chain_clean").str.split(" ").alias("ctok"))
        .explode("ctok")
        .select(pl.col("ctok").alias("token"))
        .group_by("token")
        .agg(pl.len().alias("chain_field_count"))
    )

    global_stats = (
        tokens.group_by("token")
        .agg(
            pl.len().alias("doc_freq"),
            pl.col("provider").n_unique().alias("provider_coverage"),
            pl.col("city_name_norm").n_unique().alias("city_coverage"),
            pl.col("h3_8").n_unique().alias("h3_8_coverage"),
        )
        .with_columns(
            (pl.col("doc_freq") / total_records).alias("doc_freq_ratio"),
            pl.col("token").str.len_chars().alias("token_length"),
            pl.col("token").str.contains(r"^[0-9]+$").alias("is_numeric"),
            pl.col("token").str.contains(r"^[a-z]+$").alias("is_alpha"),
        )
        .join(chain_tokens, on="token", how="left")
        .with_columns(pl.col("chain_field_count").fill_null(0))
        .sort("doc_freq", descending=True)
    )

    provider_stats = (
        tokens.group_by(["provider", "token"])
        .agg(pl.len().alias("doc_freq"))
        .sort(["provider", "doc_freq"], descending=[False, True])
    )

    city_stats = (
        tokens.filter(pl.col("city_name_norm").is_not_null())
        .group_by(["city_name_norm", "token"])
        .agg(
            pl.len().alias("city_doc_freq"),
            pl.col("provider").n_unique().alias("city_provider_coverage"),
        )
        .sort(["city_name_norm", "city_doc_freq"], descending=[False, True])
    )

    h3_stats = (
        tokens.filter(pl.col("h3_8").is_not_null())
        .group_by(["h3_8", "token"])
        .agg(pl.len().alias("h3_8_doc_freq"))
        .sort(["h3_8", "h3_8_doc_freq"], descending=[False, True])
    )

    return {
        "global": global_stats,
        "provider": provider_stats,
        "city": city_stats,
        "h3_8": h3_stats,
    }
