"""Single entry point: download -> normalize -> score -> gate -> cluster -> review queue.

Usage:
    .venv/bin/python -m hotelmap.pipeline --country NZ
    .venv/bin/python -m hotelmap.pipeline --country IN --skip-download

Stage versions are the production baseline (scoring v2_1, clustering v2_4);
each stage logs a `[pipeline] stage ...` line so callers (the dashboard) can
show progress. The final line is `[pipeline] DONE run=<run_dir>`.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIGS_DIR = REPO_ROOT / "configs"

SCORING_VERSION = "v2_1"
CLUSTER_VERSION = "v2_4"


def country_configs() -> dict[str, Path]:
    """Map ISO2 country -> run config path, from `country:` keys in configs/*.yaml."""
    out: dict[str, Path] = {}
    for p in sorted(CONFIGS_DIR.glob("*.yaml")):
        try:
            cfg = yaml.safe_load(p.read_text()) or {}
        except Exception:  # noqa: BLE001
            continue
        country = cfg.get("country")
        if country and "paths" in cfg:
            out[str(country).upper()] = p
    return out


# Generic starter config for a country with no hand-tuned yaml yet. Everything
# data-driven (token stats, weak lists, reuse flags) works as-is; the comments
# at the top of the generated file list what is degraded until tuned.
GENERIC_CONFIG_TEMPLATE = """\
# AUTO-GENERATED generic config for {cc} ({date}) — usable, but review before
# trusting results. Country-specific tuning still missing:
#   - geo bbox: wrong-country leak flags (lat_lng_out_of_range) are DISABLED,
#     so provider rows leaked from other countries are not caught (restel/
#     cleartrip are known leakers). Add lat/lng bounds when known.
#   - postal: generic permissive pattern, postal-field only (no address-text
#     extraction — too collision-prone without a country format). The
#     city+postal blocking rule is weaker until a real pattern is set.
#   - brand_tokens / brand_groups: EMPTY — the brand-conflict clustering guard
#     is inert. Promote candidates from token_policy_generated.yaml after the
#     first run (curate; do not auto-apply).
#   - state_maps.yaml has no entry for {cc}: state variants stay unmerged.
#   - country_maps.yaml: if agoda/grnc use a numeric code for {cc}, add it;
#     records fall back to country_code_iso2 meanwhile.
#   - non-Latin-script countries: ascii folding + length-4 token thresholds
#     are tuned for Latin names and may behave poorly.
country: {cc}
default_phone_region: {cc}

paths:
  raw_dir: data/raw/{cc}
  output_dir: data/artifacts/runs

providers:
  - agoda
  - cleartrip
  - expedia
  - gogobal
  - grnc
  - hotelbeds
  - ioxl
  - ratehawk
  - restel
  - rezlive
  - tbo
  - tripjack
  - veturis

geo:
  h3_resolutions: [7, 8, 9]
  geohash_precisions: [5, 6, 7]
  min_decimal_places: 3

postal:
  # permissive generic: 3-10 char alphanumeric token from the postal FIELD only
  field_pattern: "\\\\b([a-z0-9][a-z0-9 -]{{1,8}}[a-z0-9])\\\\b"
  # never matches — address-text extraction needs a country-specific pattern
  address_pattern: "([^\\\\s\\\\S])"
  validation_pattern: "^[a-z0-9][a-z0-9 -]{{1,8}}[a-z0-9]$"

address_abbreviations:
  ave: avenue
  blvd: boulevard
  dr: drive
  ln: lane
  ct: court
  pl: place
  st: street
  rd: road

tokens:
  min_token_length: 2
  low_value_global_doc_freq_ratio: 0.005
  low_value_min_city_coverage: 10
  low_value_min_provider_coverage: 4
  rare_global_max_doc_freq: 2
  rare_global_min_length: 4
  rare_city_min_doc_freq: 2
  rare_city_max_doc_freq: 50
  rare_city_min_length: 4
  rare_city_max_cities: 50
  brand_candidate_min_chain_count: 10
  blocking_overcommon_ratio: 0.001
  blocking_min_token_length: 4
  match_global_overcommon_ratio: 0.01
  match_city_overcommon_ratio: 0.02
  match_city_overcommon_min_doc_freq: 20
  match_city_overcommon_min_provider_coverage: 4
  match_min_token_length: 4
  match_area_identity_keep_max_doc_freq: 200

manual_tokens:
  force_low_value:
    - hotel
    - hotels
    - room
    - rooms
    - stay
    - stays
    - the
    - and
  # franchise type-words: "X Inn" vs "X Suites" can be different co-located
  # properties; keeping them in the core keeps signatures discriminative
  force_keep:
    - inn
    - suites
    - motel
    - lodge
  brand_tokens: []
  location_descriptors:
    - near
    - opposite
    - opp
    - "off"

reuse:
  phone_reuse_min: 5
  email_reuse_min: 5
  domain_reuse_min: 20
  top_n: 200

generic_email_locals:
  - info
  - reservation
  - reservations
  - booking
  - bookings
  - sales
  - contact
  - frontoffice
  - frontdesk
  - enquiry
  - enquiries
  - admin
  - support
  - stay
  - hotel
  - manager
  - gm

weak_domains:
  - booking.com
  - agoda.com
  - expedia.com
  - tboholidays.com
  - tripadvisor.com
  - hotels.com
  - airbnb.com
  - cleartrip.com
  - worldota.net
  - grnconnect.com
  - travelocity.com
  - orbitz.com
  - priceline.com
  - trivago.com
  - marriott.com
  - hilton.com
  - ihg.com
  - hyatt.com
  - wyndhamhotels.com
  - choicehotels.com
  - bestwestern.com
  - radissonhotels.com
  - accor.com
  - all.accor.com
"""


def ensure_country_config(country: str) -> Path:
    """Return the config for a country, generating a generic one if missing."""
    country = country.upper()
    configs = country_configs()
    if country in configs:
        return configs[country]
    path = CONFIGS_DIR / f"{country.lower()}.yaml"
    if path.exists():
        raise SystemExit(
            f"{path} exists but has no `country:`/`paths:` keys — fix it manually"
        )
    path.write_text(GENERIC_CONFIG_TEMPLATE.format(
        cc=country, date=time.strftime("%Y-%m-%d")))
    print(f"[pipeline] generated generic config {path} — review its header "
          f"comments before trusting results", flush=True)
    return path


def _stage(name: str):
    print(f"\n[pipeline] stage {name}", flush=True)
    return time.time()


def _done(t0: float) -> None:
    print(f"[pipeline] stage done in {time.time() - t0:.1f}s", flush=True)


def run_pipeline(country: str, skip_download: bool = False) -> Path:
    country = country.upper()
    if not re.fullmatch(r"[A-Z]{2}", country):
        raise SystemExit(f"country must be an ISO2 code, got {country!r}")
    config_path = str(ensure_country_config(country))
    print(f"[pipeline] country={country} config={config_path}", flush=True)

    if skip_download:
        raw_dir = REPO_ROOT / "data" / "raw" / country
        if not any(raw_dir.glob("*_property_info.ndjson")):
            raise SystemExit(
                f"--skip-download but no raw files under {raw_dir} — "
                f"run without --skip-download (needs EMBEDDING_GATEWAY_API_KEY)"
            )
        print("[pipeline] stage download (skipped)", flush=True)
    else:
        t = _stage("download")
        from hotelmap.download import download_country

        download_country(country)
        _done(t)

    t = _stage("normalize")
    from hotelmap.normalize.run import run_normalization

    run_dir = run_normalization(config_path)
    _done(t)
    normalized = str(run_dir / "normalized_hotels.parquet")

    diag = json.loads((run_dir / "diagnostics.json").read_text())
    if not diag.get("guardrails_passed"):
        failing = {k: v for k, v in diag.get("guardrails", {}).items() if not v.get("pass")}
        raise SystemExit(
            "[pipeline] normalization guardrails FAILED — not scoring garbage. "
            f"Failing: {json.dumps(failing)} (artifacts kept in {run_dir} for inspection)"
        )

    t = _stage(f"splink scoring {SCORING_VERSION}")
    from hotelmap.splink_model.run_splink_scoring import main as scoring_main

    scoring_main([
        "--normalized", normalized,
        "--version", SCORING_VERSION,
        "--config", config_path,
    ])
    _done(t)
    scored = str(
        run_dir / f"splink_{SCORING_VERSION}"
        / f"splink_scored_pairs_{SCORING_VERSION}.parquet"
    )

    t = _stage(f"threshold analysis {SCORING_VERSION}")
    from hotelmap.splink_model.run_threshold_analysis import main as threshold_main

    threshold_main([
        "--normalized", normalized,
        "--scored", scored,
        "--version", SCORING_VERSION,
        "--config", config_path,
    ])
    _done(t)
    gated = str(
        run_dir / f"splink_{SCORING_VERSION}" / "threshold"
        / f"gated_pairs_{SCORING_VERSION}.parquet"
    )

    t = _stage(f"clustering {CLUSTER_VERSION}")
    from hotelmap.splink_model.run_clustering import main as clustering_main

    clustering_main([
        "--normalized", normalized,
        "--gated", gated,
        "--version", CLUSTER_VERSION,
        "--config", config_path,
    ])
    _done(t)

    t = _stage(f"review queue {CLUSTER_VERSION}")
    from hotelmap.splink_model.run_review_queue import main as review_main

    review_main([
        "--normalized", normalized,
        "--version", CLUSTER_VERSION,
    ])
    _done(t)

    summary_path = (
        run_dir / f"splink_{SCORING_VERSION}" / "clusters"
        / f"clustering_summary_{CLUSTER_VERSION}.json"
    )
    if summary_path.exists():
        s = json.loads(summary_path.read_text())
        print(
            f"[pipeline] clusters: auto={s.get('auto_accepted'):,} "
            f"review={s.get('review'):,} singletons={s.get('singleton_unmatched'):,}",
            flush=True,
        )
    print(f"[pipeline] DONE run={run_dir}", flush=True)
    return run_dir


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="End-to-end hotel mapping pipeline")
    p.add_argument("--country", required=True, help="ISO2 country, e.g. IN / NZ / US")
    p.add_argument("--skip-download", action="store_true",
                   help="use existing data/raw/<country> files")
    args = p.parse_args(argv)
    run_pipeline(args.country, skip_download=args.skip_download)
    return 0


if __name__ == "__main__":
    sys.exit(main())
