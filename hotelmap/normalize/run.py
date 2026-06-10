"""Normalization pipeline orchestrator (stages 1-8).

Reads raw provider feeds for a country, runs the lazy base pipeline, materializes
once to compute token statistics, generates + merges the token policy, applies
policy-driven and contact/type features, then writes the normalized parquet plus
all per-run artifacts under data/artifacts/runs/<run_id>/.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl

from hotelmap.normalize.address import add_address_features
from hotelmap.normalize.config import load_config
from hotelmap.normalize.country import normalize_country
from hotelmap.normalize.diagnostics import (
    build_diagnostics,
    build_provider_quality,
    build_reuse_reports,
)
from hotelmap.normalize.emails import add_email_features
from hotelmap.normalize.geo import add_geo_features
from hotelmap.normalize.io import write_json, write_parquet, write_yaml
from hotelmap.normalize.names import (
    add_blocking_token_features,
    add_match_token_features,
    add_name_base_features,
    add_name_policy_features,
    add_rare_token_features,
    build_match_area_identity_keep,
    build_city_overcommon_token_map,
    build_match_global_stoplist,
)
from hotelmap.normalize.phones import add_phone_features
from hotelmap.normalize.property_type import normalize_property_type
from hotelmap.normalize.schema import standardize_schema
from hotelmap.normalize.text import add_clean_text_columns
from hotelmap.normalize.token_policy import (
    generate_token_policy,
    load_manual_policy,
    merge_token_policies,
)
from hotelmap.normalize.tokens import build_token_stats
from hotelmap.normalize.urls import add_url_features


def _make_run_id(output_root: Path, country: str) -> str:
    # country-tagged since multi-country runs landed (old IN dirs lack the tag)
    day = datetime.now().strftime("%Y-%m-%d")
    prefix = f"{day}_{country}" if country else day
    n = 1
    for existing in output_root.glob(f"{prefix}_*"):
        try:
            n = max(n, int(existing.name.split("_")[-1]) + 1)
        except ValueError:
            continue
    return f"{prefix}_{n:03d}"


def run_normalization(config_path: str) -> Path:
    t0 = time.time()
    config = load_config(config_path)

    output_root = Path(config["paths"]["output_dir"])
    output_root.mkdir(parents=True, exist_ok=True)
    run_id = _make_run_id(output_root, str(config.get("country", "")).upper())
    out = output_root / run_id
    out.mkdir(parents=True, exist_ok=True)
    print(f"[run] {run_id} -> {out}")

    # --- base lazy pipeline (stages 1-5 base, 8 address) ---
    base_lf = (
        standardize_schema(config, run_id)
        .pipe(add_clean_text_columns, config)
        .pipe(normalize_country, config)
        .pipe(add_geo_features, config)
        .pipe(add_name_base_features, config)
        .pipe(add_address_features, config)
    )

    print("[run] materializing base frame ...")
    base_df = base_lf.collect()
    print(f"[run] base rows={base_df.height:,} cols={len(base_df.columns)} "
          f"({time.time()-t0:.1f}s)")

    # --- stage 6: token frequency stats ---
    token_stats = build_token_stats(base_df, config)
    for name, frame in token_stats.items():
        write_parquet(frame, out / f"name_token_stats_{name}.parquet")
    print(f"[run] token stats: {token_stats['global'].height:,} distinct global tokens")

    # --- stage 7: token policy ---
    generated = generate_token_policy(token_stats, config)
    manual = load_manual_policy(config)
    effective = merge_token_policies(generated, manual)
    write_yaml(generated, out / "token_policy_generated.yaml")
    write_yaml(effective, out / "token_policy_effective.yaml")
    print(f"[run] policy: low_value={len(effective['low_value_global'])} "
          f"brand={len(effective['brand_tokens'])} "
          f"location={len(effective['location_descriptors'])}")

    # Blocking stoplist: stricter than low-value — tokens too common to block on.
    tcfg = config["tokens"]
    blocking_stoplist = (
        token_stats["global"]
        .filter(pl.col("doc_freq_ratio") >= tcfg.get("blocking_overcommon_ratio", 0.001))
        .get_column("token")
        .to_list()
    )
    blocking_min_len = tcfg.get("blocking_min_token_length", 4)
    match_global_stoplist = build_match_global_stoplist(token_stats, config)
    match_city_overcommon = build_city_overcommon_token_map(base_df, token_stats, config)
    match_area_keep = build_match_area_identity_keep(token_stats, config)
    match_min_len = tcfg.get("match_min_token_length", 4)

    # --- stage 5b + contact/type features (eager) ---
    normalized = (
        base_df.pipe(add_name_policy_features, effective)
        .pipe(add_blocking_token_features, blocking_stoplist, blocking_min_len)
        .pipe(
            add_match_token_features,
            effective,
            match_global_stoplist,
            match_city_overcommon,
            match_min_len,
            match_area_keep,
        )
        .pipe(add_rare_token_features, effective["rare_global_tokens"], effective["rare_city_tokens"])
        .pipe(add_phone_features, config)
        .pipe(add_email_features, config)
        .pipe(add_url_features, config)
        .pipe(normalize_property_type, config)
    )

    # --- stage 8: output ---
    write_parquet(normalized, out / "normalized_hotels.parquet")
    print(f"[run] wrote normalized_hotels.parquet ({normalized.height:,} rows)")

    # --- diagnostics ---
    write_parquet(build_provider_quality(normalized), out / "provider_quality.parquet")
    reuse = build_reuse_reports(normalized, config)
    write_parquet(reuse["phones"], out / "top_reused_phones.parquet")
    write_parquet(reuse["emails"], out / "top_reused_emails.parquet")
    write_parquet(reuse["domains"], out / "top_reused_domains.parquet")

    diagnostics = build_diagnostics(normalized, token_stats, effective, config)
    write_json(diagnostics, out / "diagnostics.json")

    status = "PASS" if diagnostics["guardrails_passed"] else "FAIL"
    print(f"[run] guardrails: {status}")
    for k, v in diagnostics["guardrails"].items():
        flag = "ok" if v["pass"] else "XX"
        print(f"       [{flag}] {k}={v['value']:.4f} (<= {v['threshold']})")
    print(f"[run] done in {time.time()-t0:.1f}s -> {out}")
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run hotel normalization pipeline")
    parser.add_argument("--config", default="configs/india.yaml")
    args = parser.parse_args(argv)
    run_normalization(args.config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
