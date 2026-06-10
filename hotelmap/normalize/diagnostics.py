"""Diagnostics + quality guardrails.

Every run emits a diagnostics.json (headline rates + guardrail pass/fail), a
provider_quality table, and the top-reused phone/email/domain tables. These are how
we "observe what happened" and catch a token policy that has become too aggressive.
"""

from __future__ import annotations

from typing import Any

import polars as pl

# Hard-fail thresholds (see AGENT.md).
GUARDRAILS = {
    "country_norm_failure_rate": 0.01,
    "name_norm_missing_rate": 0.005,
    "empty_name_core_rate": 0.20,
}


def _rate(col: pl.Expr) -> pl.Expr:
    return col.cast(pl.Float64).mean()


def build_diagnostics(
    df: pl.DataFrame,
    token_stats: dict[str, pl.DataFrame],
    policy: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    total = df.height

    rates = df.select(
        empty_name_core_rate=_rate(pl.col("property_name_core_empty")),
        name_norm_missing_rate=_rate(pl.col("property_name_norm").is_null()),
        empty_city_rate=_rate(pl.col("city_name_norm").is_null()),
        invalid_coord_rate=_rate(~pl.col("lat_lng_valid")),
        low_precision_coord_rate=_rate(pl.col("lat_lng_low_precision")),
        out_of_range_coord_rate=_rate(pl.col("lat_lng_out_of_range")),
        country_norm_failure_rate=_rate(pl.col("country_norm_warning")),
        country_off_target_rate=_rate(pl.col("country_off_target")),
        postal_code_conflict_rate=_rate(pl.col("postal_code_conflict")),
        phone_reuse_rate=_rate(pl.col("phone_reused_flag")),
        email_reuse_rate=_rate(pl.col("email_reused_flag")),
        domain_reuse_rate=_rate(pl.col("website_reused_flag")),
        phone_present_rate=_rate(pl.col("phone_valid_count") > 0),
        records_only_weak_name_tokens=_rate(
            pl.col("property_name_core_empty") & (pl.col("property_name_token_count") > 0)
        ),
    ).to_dicts()[0]

    guardrails = {
        name: {
            "value": rates[name],
            "threshold": thr,
            "pass": (rates[name] is not None and rates[name] <= thr),
        }
        for name, thr in GUARDRAILS.items()
    }
    all_pass = all(g["pass"] for g in guardrails.values())

    return {
        "run_id": df["normalization_run_id"][0] if total else None,
        "normalization_version": config.get("normalization_version"),
        "country": config.get("country"),
        "total_records": total,
        "records_per_provider": dict(
            df.group_by("provider").len().sort("provider").iter_rows()
        ),
        "rates": rates,
        "guardrails": guardrails,
        "guardrails_passed": all_pass,
        "token_policy_sizes": {
            "low_value_global": len(policy.get("low_value_global", [])),
            "brand_tokens": len(policy.get("brand_tokens", [])),
            "location_descriptors": len(policy.get("location_descriptors", [])),
            "rare_global_tokens": len(policy.get("rare_global_tokens", [])),
            "rare_city_tokens_cities": len(policy.get("rare_city_tokens", {})),
        },
        "distinct_global_tokens": token_stats["global"].height,
    }


def build_provider_quality(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("provider")
        .agg(
            pl.len().alias("records"),
            _rate(pl.col("property_name_norm").is_null()).alias("name_missing_rate"),
            _rate(pl.col("property_name_core_empty")).alias("empty_core_rate"),
            _rate(~pl.col("lat_lng_valid")).alias("invalid_coord_rate"),
            _rate(pl.col("lat_lng_low_precision")).alias("low_precision_rate"),
            _rate(pl.col("country_norm_warning")).alias("country_fail_rate"),
            _rate(pl.col("country_off_target")).alias("off_target_rate"),
            _rate(pl.col("city_name_norm").is_null()).alias("empty_city_rate"),
            _rate(pl.col("phone_valid_count") > 0).alias("phone_present_rate"),
            _rate(pl.col("email_norm_list").list.len() > 0).alias("email_present_rate"),
            _rate(pl.col("website_domain_norm").is_not_null()).alias("website_present_rate"),
            _rate(pl.col("star_rating_missing")).alias("star_missing_rate"),
        )
        .sort("records", descending=True)
    )


def _reuse_report(df: pl.DataFrame, list_col: str, value_name: str, top_n: int) -> pl.DataFrame:
    return (
        df.select("record_id", "provider", list_col)
        .explode(list_col)
        .drop_nulls(list_col)
        .rename({list_col: value_name})
        .unique(["record_id", value_name])
        .group_by(value_name)
        .agg(
            pl.len().alias("record_count"),
            pl.col("provider").n_unique().alias("provider_count"),
        )
        .sort("record_count", descending=True)
        .head(top_n)
    )


def build_reuse_reports(df: pl.DataFrame, config: dict[str, Any]) -> dict[str, pl.DataFrame]:
    top_n = config.get("reuse", {}).get("top_n", 200)
    domains = (
        df.select("record_id", "provider", "website_domain_norm")
        .drop_nulls("website_domain_norm")
        .unique(["record_id", "website_domain_norm"])
        .group_by("website_domain_norm")
        .agg(
            pl.len().alias("record_count"),
            pl.col("provider").n_unique().alias("provider_count"),
        )
        .sort("record_count", descending=True)
        .head(top_n)
    )
    return {
        "phones": _reuse_report(df, "phone_last10_list", "phone", top_n),
        "emails": _reuse_report(df, "email_norm_list", "email", top_n),
        "domains": domains,
    }
