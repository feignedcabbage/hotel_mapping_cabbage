"""Stage 7 — automated token policy generation.

Turns the token-frequency tables into a *generated* policy (low-value, brand
candidates, rare tokens), then layers manual config overrides on top to produce the
*effective* policy. Both are versioned to disk every run. The generator is
deliberately simple; thresholds live in config and are tuned by inspecting output.
"""

from __future__ import annotations

from typing import Any

import polars as pl


def generate_token_policy(
    token_stats: dict[str, pl.DataFrame], config: dict[str, Any]
) -> dict[str, Any]:
    g = token_stats["global"]
    tcfg = config["tokens"]

    # Low-value: frequent, spread across many cities and providers, not numeric.
    low_value = (
        g.filter(pl.col("doc_freq_ratio") >= tcfg["low_value_global_doc_freq_ratio"])
        .filter(pl.col("city_coverage") >= tcfg["low_value_min_city_coverage"])
        .filter(pl.col("provider_coverage") >= tcfg["low_value_min_provider_coverage"])
        .filter(~pl.col("is_numeric"))
        .sort("doc_freq", descending=True)
        .get_column("token")
        .to_list()
    )

    # Brand candidates: a REVIEW QUEUE, not auto-applied (see merge_token_policies).
    # The hotel_chain field is noisy (person names recur there), so we require a
    # strong, multi-provider chain footprint to even suggest a token. A human
    # promotes real brands into manual_tokens.brand_tokens in config.
    brand_min_chain = tcfg.get("brand_candidate_min_chain_count", 30)
    low_value_set = set(low_value)
    brand_candidates = (
        g.filter(pl.col("chain_field_count") >= brand_min_chain)
        .filter(pl.col("provider_coverage") >= 3)
        .filter(~pl.col("is_numeric"))
        .filter(pl.col("token_length") >= 3)
        .filter(~pl.col("token").is_in(list(low_value_set)))
        .sort("chain_field_count", descending=True)
        .get_column("token")
        .to_list()
    )

    very_rare_global = (
        g.filter(pl.col("doc_freq") <= tcfg["rare_global_max_doc_freq"])
        .filter(pl.col("token_length") >= tcfg["rare_global_min_length"])
        .filter(~pl.col("is_numeric"))
        .get_column("token")
        .to_list()
    )

    rare_city = _rare_city_tokens(token_stats["city"], g, tcfg)

    return {
        "generated_low_value_global": sorted(low_value),
        "generated_brand_candidates": sorted(brand_candidates),
        "generated_very_rare_global": sorted(very_rare_global),
        "generated_rare_city_tokens": rare_city,
    }


def _rare_city_tokens(
    city_stats: pl.DataFrame, global_stats: pl.DataFrame, tcfg: dict[str, Any]
) -> dict[str, list[str]]:
    """Tokens rare *within a city* but not globally trivial — locally identifying."""
    global_low = global_stats.filter(
        pl.col("doc_freq_ratio") >= tcfg["low_value_global_doc_freq_ratio"]
    ).get_column("token")

    rare = (
        city_stats.filter(
            pl.col("city_doc_freq").is_between(
                tcfg["rare_city_min_doc_freq"], tcfg["rare_city_max_doc_freq"]
            )
        )
        .filter(pl.col("token").str.len_chars() >= tcfg["rare_city_min_length"])
        .filter(~pl.col("token").is_in(global_low))
    )

    # cap the number of cities emitted to keep the policy file readable
    top_cities = (
        rare.group_by("city_name_norm")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .head(tcfg.get("rare_city_max_cities", 50))
        .get_column("city_name_norm")
        .to_list()
    )

    out: dict[str, list[str]] = {}
    for city in top_cities:
        toks = (
            rare.filter(pl.col("city_name_norm") == city)
            .sort("city_doc_freq")
            .get_column("token")
            .to_list()
        )
        if toks:
            out[city] = sorted(set(toks))
    return out


def load_manual_policy(config: dict[str, Any]) -> dict[str, Any]:
    manual = config.get("manual_tokens", {}) or {}
    return {
        "force_low_value": manual.get("force_low_value", []),
        "force_keep": manual.get("force_keep", []),
        "brand_tokens": manual.get("brand_tokens", []),
        "location_descriptors": manual.get("location_descriptors", []),
    }


def merge_token_policies(generated: dict[str, Any], manual: dict[str, Any]) -> dict[str, Any]:
    force_keep = set(manual.get("force_keep", []))
    manual_brand = set(manual.get("brand_tokens", [])) - force_keep

    # Low-value first (generated frequency words + manual forced), but never demote a
    # manually declared brand.
    low_value = (
        (set(generated.get("generated_low_value_global", [])) | set(manual.get("force_low_value", [])))
        - force_keep
        - manual_brand
    )

    # Brand = curated manual list ONLY. Generated brand candidates are a review
    # queue in token_policy_generated.yaml (the hotel_chain field is too noisy to
    # trust automatically — person names recur there). Promote real brands by
    # adding them to manual_tokens.brand_tokens in config.
    brand = manual_brand

    location = set(manual.get("location_descriptors", [])) - force_keep

    # A token lives in exactly one bucket. Priority: brand > location > low_value.
    location -= brand
    low_value -= brand | location

    return {
        "low_value_global": sorted(low_value),
        "brand_tokens": sorted(brand),
        "location_descriptors": sorted(location),
        "force_keep": sorted(force_keep),
        "rare_global_tokens": sorted(generated.get("generated_very_rare_global", [])),
        "rare_city_tokens": generated.get("generated_rare_city_tokens", {}),
    }
