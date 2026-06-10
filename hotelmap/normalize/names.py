"""Stage 5 — name tokenization and (later) policy-driven token split.

`add_name_base_features` runs before token stats: it produces the normalized name
and raw token list. `add_name_policy_features` runs after the token policy is built:
it splits tokens into core / brand / low-value / location buckets. Nothing is
discarded — weak tokens are *separated* so Splink can weight them later.

All transforms are native polars expressions except the ASCII fold (no native
transliteration); token splitting uses vectorized list.eval, not row UDFs.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from hotelmap.normalize.text import NULL_LIKE, ascii_expr


def add_name_base_features(lf: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
    min_len = config.get("tokens", {}).get("min_token_length", 2)

    lf = lf.with_columns(ascii_expr("property_name").alias("property_name_ascii"))

    norm = (
        pl.col("property_name_ascii")
        .str.to_lowercase()
        .str.normalize("NFKC")
        .str.replace_all("&", " and ")
        .str.replace_all(r"[^0-9a-z]+", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )
    norm = pl.when(norm.is_in(list(NULL_LIKE))).then(None).otherwise(norm)

    lf = lf.with_columns(
        pl.col("property_name").alias("property_name_raw"),
        norm.alias("property_name_norm"),
    )

    # tokens: split on space, keep tokens of length >= min_len OR pure-numeric.
    tokens = (
        pl.col("property_name_norm")
        .fill_null("")
        .str.split(" ")
        .list.eval(
            pl.element().filter(
                (pl.element().str.len_chars() >= min_len)
                | pl.element().str.contains(r"^[0-9]+$")
            )
        )
    )

    lf = lf.with_columns(tokens.alias("property_name_tokens"))

    return lf.with_columns(
        pl.col("property_name_tokens").list.len().alias("property_name_token_count"),
        pl.col("property_name_tokens")
        .list.sort()
        .list.join(" ")
        .alias("property_name_sorted_tokens"),
    )


def add_name_policy_features(df: pl.DataFrame, policy: dict[str, Any]) -> pl.DataFrame:
    """Split name tokens using the effective token policy (vectorized)."""
    low_value = policy.get("low_value_global", [])
    brand = policy.get("brand_tokens", [])
    location = policy.get("location_descriptors", [])
    weak = list(set(low_value) | set(brand) | set(location))

    tok = pl.col("property_name_tokens")

    df = df.with_columns(
        tok.list.eval(pl.element().filter(~pl.element().is_in(weak))).alias(
            "property_name_core_tokens"
        ),
        tok.list.eval(pl.element().filter(pl.element().is_in(brand))).alias(
            "property_name_brand_tokens"
        ),
        tok.list.eval(pl.element().filter(pl.element().is_in(low_value))).alias(
            "property_name_low_value_tokens"
        ),
        tok.list.eval(pl.element().filter(pl.element().is_in(location))).alias(
            "property_name_location_tokens"
        ),
    )

    return df.with_columns(
        pl.col("property_name_core_tokens").list.join(" ").alias("property_name_core"),
        pl.col("property_name_core_tokens")
        .list.sort()
        .list.join(" ")
        .alias("property_name_signature"),
        (pl.col("property_name_core_tokens").list.len() == 0).alias(
            "property_name_core_empty"
        ),
    )


def add_blocking_token_features(
    df: pl.DataFrame, blocking_stoplist: list[str], min_len: int = 4
) -> pl.DataFrame:
    """Tokens safe to block on: distinctive enough not to create giant blocks.

    Starts from core tokens (already free of brand/low-value/location), then keeps
    only tokens >= `min_len` chars that are NOT globally over-common (the stoplist is
    a stricter superset of low-value, see run.py). Tokens equal to a word in the
    record's own city name are dropped too — a token identical to the city is pure
    location filler (e.g. "Hotel Mahabaleshwar" in Mahabaleshwar), no hotel-identity
    signal, and it creates self-referential city blocks. 4+ digit numerics (e.g. OYO
    codes) survive — strong shared identity. Feeds the city+postal+token rule.
    """
    stop = list(blocking_stoplist)
    city_tokens = pl.col("city_name_norm").fill_null("").str.split(" ")
    return df.with_columns(
        pl.col("property_name_core_tokens")
        .list.eval(
            pl.element().filter(
                (pl.element().str.len_chars() >= min_len) & ~pl.element().is_in(stop)
            )
        )
        .list.set_difference(city_tokens)
        .alias("property_name_blocking_tokens")
    )


def _context_tokens(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .fill_null("")
        .str.to_lowercase()
        .str.replace_all(r"[^0-9a-z]+", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.split(" ")
        .list.eval(pl.element().filter(pl.element().str.len_chars() >= 2))
    )


def build_match_global_stoplist(
    token_stats: dict[str, pl.DataFrame], config: dict[str, Any]
) -> list[str]:
    """Globally over-common tokens to remove from identity-name Jaccard."""
    tcfg = config.get("tokens", {})
    ratio = tcfg.get("match_global_overcommon_ratio", tcfg.get("blocking_overcommon_ratio", 0.001))
    return (
        token_stats["global"]
        .filter(pl.col("doc_freq_ratio") >= ratio)
        .get_column("token")
        .to_list()
    )


def build_match_area_identity_keep(
    token_stats: dict[str, pl.DataFrame], config: dict[str, Any]
) -> list[str]:
    """Rare name tokens that should survive even if repeated in noisy area text."""
    tcfg = config.get("tokens", {})
    max_docs = tcfg.get("match_area_identity_keep_max_doc_freq", 200)
    min_len = tcfg.get("match_min_token_length", 4)
    return (
        token_stats["global"]
        .filter(pl.col("doc_freq") <= max_docs)
        .filter(pl.col("token").str.len_chars() >= min_len)
        .filter(~pl.col("token").str.contains(r"^[0-9]+$"))
        .get_column("token")
        .to_list()
    )


def build_city_overcommon_token_map(
    df: pl.DataFrame, token_stats: dict[str, pl.DataFrame], config: dict[str, Any]
) -> dict[str, list[str]]:
    """Tokens too common within a city to be identity evidence.

    This catches city aliases in hotel names, e.g. many `kochi` records using
    `cochin` in the name. It is deliberately provider-coverage gated so a token that
    only repeats inside one feed is not stripped as local geography.

    Record counts alone are NOT enough: one hotel listed by 13 providers in a
    small town pushes its own brand tokens over the ratio (`ramada`/`wyndham`
    in Kranjska Gora went "overcommon", emptied the match tokens, and left every
    listing of the hotel an unmappable singleton). A genuine city alias appears
    across many DISTINCT names, so we additionally require the token to occur in
    >= `match_city_overcommon_min_distinct_signatures` different name signatures
    within the city.
    """
    tcfg = config.get("tokens", {})
    min_ratio = tcfg.get("match_city_overcommon_ratio", 0.02)
    min_docs = tcfg.get("match_city_overcommon_min_doc_freq", 20)
    min_providers = tcfg.get("match_city_overcommon_min_provider_coverage", 4)
    min_sigs = tcfg.get("match_city_overcommon_min_distinct_signatures", 10)

    city_counts = (
        df.filter(pl.col("city_name_norm").is_not_null())
        .group_by("city_name_norm")
        .agg(pl.len().alias("city_records"))
    )
    # distinct-name key: the full sorted-token string where available (normalize
    # path — signature isn't built yet), else the core signature (scoring path)
    if "property_name_sorted_tokens" in df.columns:
        key_col = pl.col("property_name_sorted_tokens")
        if df.schema["property_name_sorted_tokens"] != pl.Utf8:
            key_col = key_col.list.join(" ")
    else:
        key_col = pl.col("property_name_signature")
    name_key = key_col.alias("_name_key")
    sig_counts = (
        df.filter(pl.col("city_name_norm").is_not_null())
        .select("city_name_norm", name_key, "property_name_tokens")
        .explode("property_name_tokens")
        .rename({"property_name_tokens": "token"})
        .drop_nulls("token")
        .group_by("city_name_norm", "token")
        .agg(pl.col("_name_key").n_unique().alias("distinct_signatures"))
    )
    over = (
        token_stats["city"]
        .join(city_counts, on="city_name_norm", how="inner")
        .join(sig_counts, on=["city_name_norm", "token"], how="left")
        .with_columns((pl.col("city_doc_freq") / pl.col("city_records")).alias("city_doc_freq_ratio"))
        .filter(pl.col("city_doc_freq") >= min_docs)
        .filter(pl.col("city_provider_coverage") >= min_providers)
        .filter(pl.col("city_doc_freq_ratio") >= min_ratio)
        .filter(pl.col("distinct_signatures").fill_null(0) >= min_sigs)
        .group_by("city_name_norm")
        .agg(pl.col("token").alias("tokens"))
    )
    return dict(over.iter_rows())


def add_match_token_features(
    df: pl.DataFrame,
    policy: dict[str, Any],
    global_stoplist: list[str],
    city_overcommon: dict[str, list[str]],
    min_len: int = 4,
    area_identity_keep: list[str] | None = None,
) -> pl.DataFrame:
    """Strict identity tokens for name Jaccard/coherence.

    Starts from all name tokens, then removes weak policy tokens, global/city-local
    over-common tokens, per-record geography/context words, numeric-only tokens, and
    short tokens. This is stricter than `property_name_core_tokens`; it is meant for
    deciding whether two names share identity-bearing words, not for preserving every
    potentially useful matching hint.
    """
    default_match_low_value = [
        "hotel",
        "hotels",
        "inn",
        "resort",
        "resorts",
        "room",
        "rooms",
        "stay",
        "stays",
        "suite",
        "suites",
        "hostel",
        "hostels",
        "lodge",
        "lodges",
        "villa",
        "villas",
        "apartment",
        "apartments",
        "guest",
        "house",
    ]
    match_low_value = policy.get("match_low_value_global", default_match_low_value)
    weak = list(
        set(match_low_value)
        | set(policy.get("brand_tokens", []))
        | set(policy.get("location_descriptors", []))
        | set(global_stoplist)
    )

    city_rows = [
        {"city_name_norm": city, "token": token}
        for city, tokens in city_overcommon.items()
        for token in tokens
    ]
    city_over_df = (
        pl.DataFrame(city_rows, schema={"city_name_norm": pl.Utf8, "token": pl.Utf8})
        if city_rows
        else pl.DataFrame(schema={"city_name_norm": pl.Utf8, "token": pl.Utf8})
    )

    df = df.with_columns(
        _context_tokens("city_name_norm").alias("_match_city_tokens"),
        _context_tokens("area_clean").alias("_match_area_tokens"),
        _context_tokens("state_norm").alias("_match_state_tokens"),
        _context_tokens("country_name_clean").alias("_match_country_tokens"),
    )
    area_context_tokens = pl.col("_match_area_tokens").list.set_difference(
        pl.lit(area_identity_keep or [], dtype=pl.List(pl.Utf8))
    )
    context_tokens = (
        pl.col("_match_city_tokens")
        .list.concat(area_context_tokens)
        .list.concat(pl.col("_match_state_tokens"))
        .list.concat(pl.col("_match_country_tokens"))
    )

    base = (
        pl.col("property_name_tokens")
        .list.eval(
            pl.element().filter(
                (pl.element().str.len_chars() >= min_len)
                & ~pl.element().str.contains(r"^[0-9]+$")
                & ~pl.element().is_in(weak)
            )
        )
        .list.set_difference(context_tokens)
    )
    df = df.with_columns(base.alias("_property_name_match_tokens_base"))

    if city_over_df.height:
        exploded = (
            df.select("record_id", "city_name_norm", "_property_name_match_tokens_base")
            .explode("_property_name_match_tokens_base")
            .rename({"_property_name_match_tokens_base": "token"})
            .drop_nulls("token")
            .join(city_over_df, on=["city_name_norm", "token"], how="anti")
            .group_by("record_id")
            .agg(pl.col("token").alias("property_name_match_tokens"))
        )
        df = df.join(exploded, on="record_id", how="left").with_columns(
            pl.col("property_name_match_tokens").fill_null([])
        )
    else:
        df = df.with_columns(
            pl.col("_property_name_match_tokens_base").alias("property_name_match_tokens")
        )

    # Degenerate-name fallback: for names built ENTIRELY from weak/contextual/
    # short tokens ("vila bled", "hotel city maribor", "cha cha rooms") the
    # filtered set is empty, the name comparison goes NULL, and identical
    # cross-provider listings become unmappable singletons. Fall back to the
    # full name tokens (length>=2, non-numeric only): identical full names can
    # still match exactly, while against normal filtered tokens the overlap is
    # low — conservative in the right direction. Length 2 (not 3) is load-
    # bearing: 'hotel GK palace' and 'hotel KC palace' are DIFFERENT Bhopal
    # hotels — dropping the 2-char discriminator made both collapse to
    # 'hotel palace' and auto-merge.
    # Context (own city/state/country) tokens still leave the fallback:
    # "by the lake apartments ohrid" must get the same signature as
    # "by the lake apartments" listed by another provider in Ohrid.
    # Two tiers: prefer also dropping generic type words so "hotel rio re"
    # and "rio re" agree; if that empties the name (e.g. "apartments"),
    # keep the type words rather than have nothing.
    base_fallback = pl.col("property_name_tokens").list.eval(
        pl.element().filter(
            (pl.element().str.len_chars() >= 2)
            & ~pl.element().str.contains(r"^[0-9]+$")
        )
    ).list.set_difference(context_tokens)
    tier1 = base_fallback.list.set_difference(
        pl.lit(match_low_value, dtype=pl.List(pl.Utf8))
    )
    fallback = pl.when(tier1.list.len() > 0).then(tier1).otherwise(base_fallback)
    df = df.with_columns(
        pl.when(pl.col("property_name_match_tokens").list.len() == 0)
        .then(fallback)
        .otherwise(pl.col("property_name_match_tokens"))
        .alias("property_name_match_tokens")
    )

    return df.with_columns(
        pl.col("property_name_match_tokens")
        .list.sort()
        .list.join(" ")
        .alias("property_name_match_signature")
    ).drop(
        [
            "_match_city_tokens",
            "_match_area_tokens",
            "_match_state_tokens",
            "_match_country_tokens",
            "_property_name_match_tokens_base",
        ]
    )


def add_rare_token_features(
    df: pl.DataFrame, rare_global: list[str], rare_city_map: dict[str, list[str]]
) -> pl.DataFrame:
    """Tag rare global / rare in-city tokens (identity-bearing, valuable)."""
    rare_global_set = list(rare_global)

    df = df.with_columns(
        pl.col("property_name_tokens")
        .list.eval(pl.element().filter(pl.element().is_in(rare_global_set)))
        .alias("property_name_rare_tokens_global")
    )

    # rare-city is keyed per city; build via explode/join to stay vectorized.
    if rare_city_map:
        rare_rows = [
            {"city_name_norm": city, "token": tok}
            for city, toks in rare_city_map.items()
            for tok in toks
        ]
        rare_df = pl.DataFrame(rare_rows, schema={"city_name_norm": pl.Utf8, "token": pl.Utf8})
        exploded = (
            df.select("record_id", "city_name_norm", "property_name_tokens")
            .explode("property_name_tokens")
            .rename({"property_name_tokens": "token"})
            .join(rare_df, on=["city_name_norm", "token"], how="inner")
            .group_by("record_id")
            .agg(pl.col("token").alias("property_name_rare_tokens_city"))
        )
        df = df.join(exploded, on="record_id", how="left").with_columns(
            pl.col("property_name_rare_tokens_city").fill_null([])
        )
    else:
        df = df.with_columns(
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("property_name_rare_tokens_city")
        )

    return df
