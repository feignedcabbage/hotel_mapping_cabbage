"""Stage 09A — blocking / candidate-pair diagnostics runner.

Measures blocking behavior BEFORE any Splink scoring: how many candidate pairs each
rule generates, which blocks are too large, which provider pairs dominate, and
whether geo rules pull in invalid coordinates. Counts are analytic (see
candidate_diagnostics); only bounded samples enumerate real pairs.

Usage:
  .venv/bin/python -m hotelmap.splink_model.run_blocking_diagnostics \
      --normalized data/artifacts/runs/<run>/normalized_hotels.parquet \
      [--output-dir <dir>] [--config configs/india.yaml]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd
import yaml

from hotelmap.splink_model.blocking_rules import BLOCKING_RULES
from hotelmap.splink_model import candidate_diagnostics as cd

# Guardrail thresholds (overridable via config `blocking:` section).
DEFAULTS = {
    "block_pairs_warn": 1_000_000,        # any single block above this -> warn
    "rule_pairs_fail": 100_000_000,       # any rule above this -> fail (skip sampling)
    "invalid_geo_share_warn": 0.05,       # >5% eligible on invalid coords (geo rules)
    "provider_pair_dominance_warn": 0.50, # one provider pair > 50% of a rule's pairs
    "token_dominance_warn": 0.25,         # one name token > 25% of a token rule's pairs
    "sample_limit": 500,
    "sample_max_block_pairs": 2000,       # only sample blocks with <= this many pairs
    "large_blocks_top_n": 100,
    "large_block_members": 8,
}


def _evaluate_guardrails(summary: dict, prov_df: pd.DataFrame, cfg: dict) -> dict:
    warnings: list[str] = []
    failures: list[str] = []

    if summary["max_block_pairs"] > cfg["block_pairs_warn"]:
        warnings.append(
            f"largest block has {summary['max_block_pairs']:,} pairs "
            f"(> {cfg['block_pairs_warn']:,})"
        )
    if summary["candidate_pair_count"] > cfg["rule_pairs_fail"]:
        failures.append(
            f"rule generates {summary['candidate_pair_count']:,} pairs "
            f"(> {cfg['rule_pairs_fail']:,})"
        )
    geo_share = summary.get("invalid_coord_share_of_eligible")
    if geo_share is not None and geo_share > cfg["invalid_geo_share_warn"]:
        warnings.append(f"{geo_share:.1%} of eligible records have invalid coords")

    total = summary["candidate_pair_count"]
    dominance = None
    if total > 0 and not prov_df.empty:
        top = int(prov_df["candidate_pair_count"].iloc[0])
        dominance = top / total
        if dominance > cfg["provider_pair_dominance_warn"]:
            pair = f"{prov_df['l_provider'].iloc[0]}<->{prov_df['r_provider'].iloc[0]}"
            warnings.append(f"provider pair {pair} is {dominance:.1%} of this rule's pairs")

    return {
        "warnings": warnings,
        "failures": failures,
        "top_provider_pair_share": dominance,
        "passed": not failures,
    }


def run(normalized: str, output_dir: Path, config_path: str) -> dict:
    cfg = dict(DEFAULTS)
    reuse_min = 5
    postal_regex = cd.DEFAULT_POSTAL_VALIDATION_REGEX
    if config_path and Path(config_path).exists():
        raw = yaml.safe_load(Path(config_path).read_text()) or {}
        reuse_min = (raw.get("reuse") or {}).get("phone_reuse_min", reuse_min)
        postal_regex = (raw.get("postal") or {}).get("validation_pattern", postal_regex)
        cfg.update(raw.get("blocking") or {})

    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CREATE VIEW hotels AS SELECT * FROM read_parquet('{normalized}')")
    total_records = con.execute("SELECT COUNT(*) FROM hotels").fetchone()[0]
    print(f"[blocking] hotels: {total_records:,} records; reuse_min={reuse_min}")

    summaries: list[dict] = []
    prov_frames: list[pd.DataFrame] = []
    block_frames: list[pd.DataFrame] = []
    large_frames: list[pd.DataFrame] = []
    sample_frames: list[pd.DataFrame] = []
    token_frames: list[pd.DataFrame] = []
    guardrail_report: dict = {}

    for rule in BLOCKING_RULES:
        print(f"[blocking] rule: {rule.name}")
        n_elig = cd.build_eligible(con, rule, reuse_min, postal_regex)
        summary = cd.rule_summary(con, rule, n_elig)
        prov_df = cd.provider_pair_counts(con, rule.name)
        guards = _evaluate_guardrails(summary, prov_df, cfg)

        # extra guardrails for name-token rules: token dominance + top tokens
        if rule.has_name_token:
            tok_df = cd.token_contributions(con, rule.name)
            token_frames.append(tok_df)
            total = summary["candidate_pair_count"]
            if total > 0 and not tok_df.empty:
                top_tok_share = float(tok_df["candidate_pairs"].iloc[0]) / total
                guards["top_token_share"] = top_tok_share
                guards["top_tokens"] = tok_df.head(15)["token"].tolist()
                if top_tok_share > cfg["token_dominance_warn"]:
                    guards["warnings"].append(
                        f"token '{tok_df['token'].iloc[0]}' is {top_tok_share:.1%} of this rule's pairs"
                    )

        summaries.append(summary)
        prov_frames.append(prov_df)
        block_frames.append(cd.block_sizes(con, rule.name))
        large_frames.append(
            cd.large_blocks(con, rule.name, cfg["large_blocks_top_n"], cfg["large_block_members"])
        )
        guardrail_report[rule.name] = guards

        status = "PASS" if guards["passed"] else "FAIL"
        print(f"           pairs={summary['candidate_pair_count']:,} "
              f"covered={summary['distinct_records_covered']:,} "
              f"max_block_pairs={summary['max_block_pairs']:,} [{status}]")
        for w in guards["warnings"]:
            print(f"           WARN: {w}")
        for f in guards["failures"]:
            print(f"           FAIL: {f}")

        # Only enumerate samples for rules that passed the hard guardrail.
        if guards["passed"]:
            sample_frames.append(
                cd.sample_pairs(con, rule.name, cfg["sample_limit"], cfg["sample_max_block_pairs"])
            )

    # --- write artifacts ---
    rule_counts_df = pd.DataFrame(summaries)
    rule_counts_df.to_parquet(output_dir / "blocking_rule_counts.parquet", index=False)
    pd.concat(prov_frames, ignore_index=True).to_parquet(
        output_dir / "provider_pair_counts.parquet", index=False
    )
    pd.concat(block_frames, ignore_index=True).to_parquet(
        output_dir / "blocking_block_sizes.parquet", index=False
    )
    pd.concat(large_frames, ignore_index=True).to_parquet(
        output_dir / "large_blocks.parquet", index=False
    )
    if sample_frames:
        pd.concat(sample_frames, ignore_index=True).to_parquet(
            output_dir / "candidate_pair_samples.parquet", index=False
        )
    if token_frames:
        pd.concat(token_frames, ignore_index=True).to_parquet(
            output_dir / "blocking_token_contributions.parquet", index=False
        )

    diagnostics = {
        "total_records": int(total_records),
        "reuse_min": int(reuse_min),
        "naive_sum_candidate_pairs": int(rule_counts_df["candidate_pair_count"].sum()),
        "guardrail_thresholds": {k: cfg[k] for k in DEFAULTS},
        "rule_counts": rule_counts_df.to_dict(orient="records"),
        "guardrails": guardrail_report,
        "all_rules_passed": all(g["passed"] for g in guardrail_report.values()),
    }
    (output_dir / "blocking_diagnostics.json").write_text(
        json.dumps(diagnostics, indent=2, default=str)
    )
    print(f"[blocking] naive sum across rules: "
          f"{diagnostics['naive_sum_candidate_pairs']:,} pairs")
    print(f"[blocking] all rules passed: {diagnostics['all_rules_passed']}")
    print(f"[blocking] artifacts -> {output_dir}")
    return diagnostics


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Blocking / candidate-pair diagnostics")
    p.add_argument("--normalized", required=True, help="path to normalized_hotels.parquet")
    p.add_argument("--output-dir", default=None)
    p.add_argument("--config", default="configs/india.yaml")
    args = p.parse_args(argv)

    out = Path(args.output_dir) if args.output_dir else Path(args.normalized).parent / "blocking_diagnostics"
    run(args.normalized, out, args.config)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
