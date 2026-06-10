"""Stage 09B — Splink v1 settings (model definition).

Pair-scoring model, dedupe_only mode over the single multi-provider table. Every
blocking rule enforces `l.provider < r.provider` (cross-provider linkage only).

Modeling notes:
- Geo evidence is carried by DISTANCE only, not by h3-cell equality. h3 and distance
  are strongly correlated; using both would double-count location under Splink's
  conditional-independence assumption and inflate match weights. h3_8 stays a
  *blocking* key; the comparison uses metres.
- Phone / blocking-token overlap rules use `arrays_to_explode` so Splink explodes the
  arrays and equi-joins on elements (efficient) instead of nested-loop list_intersect.
- Empty-string identity fields are nullified upstream (see run_splink_scoring) so
  exact-match levels don't collapse all blanks together.
"""

from __future__ import annotations

import splink.comparison_library as cl
import splink.comparison_level_library as cll
from splink import SettingsCreator, block_on
from splink.blocking_rule_library import CustomRule
from splink.comparison_library import CustomComparison

_DIALECT = "duckdb"

# --- blocking rules (the confirmed Stage 09A set) ---
BLOCKING_RULES = [
    CustomRule(
        "l.country_code_norm = r.country_code_norm "
        "AND l.h3_8 = r.h3_8 "
        "AND l.provider < r.provider",
        sql_dialect=_DIALECT,
    ),
    CustomRule(
        "l.country_code_norm = r.country_code_norm "
        "AND l.city_name_norm = r.city_name_norm "
        "AND l.property_name_signature = r.property_name_signature "
        "AND l.provider < r.provider",
        sql_dialect=_DIALECT,
    ),
    CustomRule(
        # arrays_to_explode turns the token array into scalar elements; the equality
        # below then matches on a *shared* token (must be written explicitly).
        "l.country_code_norm = r.country_code_norm "
        "AND l.city_name_norm = r.city_name_norm "
        "AND l.postal_code_final = r.postal_code_final "
        "AND l.property_name_blocking_tokens = r.property_name_blocking_tokens "
        "AND l.provider < r.provider",
        sql_dialect=_DIALECT,
        arrays_to_explode=["property_name_blocking_tokens"],
    ),
    CustomRule(
        "l.country_code_norm = r.country_code_norm "
        "AND l.phone_last10_non_reused_list = r.phone_last10_non_reused_list "
        "AND l.provider < r.provider",
        sql_dialect=_DIALECT,
        arrays_to_explode=["phone_last10_non_reused_list"],
    ),
]


def _comparisons() -> list:
    return [
        # --- identity (strong) ---
        cl.JaroWinklerAtThresholds("property_name_core", [0.92, 0.85, 0.75]),
        cl.ExactMatch("property_name_signature").configure(
            term_frequency_adjustments=True
        ),
        cl.ArrayIntersectAtSizes("phone_last10_non_reused_list", [1]),
        # --- location ---
        # distance bands (km): 25m / 75m / 200m / 500m / else
        cl.DistanceInKMAtThresholds("lat_norm", "lng_norm", [0.025, 0.075, 0.2, 0.5]),
        cl.ExactMatch("city_name_norm").configure(term_frequency_adjustments=True),
        cl.ExactMatch("postal_code_final"),
        # --- weak / context ---
        cl.ExactMatch("hotel_chain_norm"),
        cl.ExactMatch("property_type_norm"),
    ]


# ---------------------------------------------------------------------------
# v2 — de-correlated comparisons (location-only pairs must not score as matches)
# ---------------------------------------------------------------------------

# order-insensitive Jaccard over core token sets, in SQL
_JAC = (
    "len(list_intersect(property_name_core_tokens_l, property_name_core_tokens_r))::DOUBLE "
    "/ len(list_distinct(list_concat(property_name_core_tokens_l, property_name_core_tokens_r)))"
)
_MATCH_JAC = (
    "len(list_intersect(property_name_match_tokens_l, property_name_match_tokens_r))::DOUBLE "
    "/ len(list_distinct(list_concat(property_name_match_tokens_l, property_name_match_tokens_r)))"
)


def _name_comparison_v2() -> CustomComparison:
    # ONE order-insensitive name comparison. Replaces v1's correlated pair
    # (JaroWinkler-on-unsorted-core + exact-signature) that mis-scored token swaps.
    return CustomComparison(
        output_column_name="property_name_core_tokens",
        comparison_description="order-insensitive core-name agreement",
        comparison_levels=[
            cll.NullLevel("property_name_core_tokens"),
            cll.CustomLevel(
                "property_name_signature_l = property_name_signature_r", "exact signature"
            ),
            cll.CustomLevel(f"{_JAC} >= 0.85", "jaccard >= 0.85"),
            cll.CustomLevel(f"{_JAC} >= 0.65", "jaccard >= 0.65"),
            cll.CustomLevel(f"{_JAC} >= 0.45", "jaccard >= 0.45"),
            cll.CustomLevel(
                "len(list_intersect(property_name_core_tokens_l, property_name_core_tokens_r)) >= 1",
                "shared core token",
            ),
            cll.ElseLevel(),
        ],
    )


def _name_comparison_v2_1() -> CustomComparison:
    # Keep exact old core signature as a strong stable level, but use the stricter
    # match-token Jaccard for all fuzzy name evidence.
    return CustomComparison(
        output_column_name="property_name_match_tokens",
        comparison_description="strict match-token name agreement",
        comparison_levels=[
            cll.NullLevel("property_name_match_tokens"),
            cll.CustomLevel(
                "property_name_match_signature_l = property_name_match_signature_r",
                "exact match-token signature",
            ),
            cll.CustomLevel(f"{_MATCH_JAC} >= 0.85", "match-token jaccard >= 0.85"),
            cll.CustomLevel(f"{_MATCH_JAC} >= 0.65", "match-token jaccard >= 0.65"),
            cll.CustomLevel(f"{_MATCH_JAC} >= 0.45", "match-token jaccard >= 0.45"),
            cll.CustomLevel(
                "len(list_intersect(property_name_match_tokens_l, property_name_match_tokens_r)) >= 1",
                "shared match token",
            ),
            cll.ElseLevel(),
        ],
    )


def _website_comparison_v2() -> CustomComparison:
    # reuse/OTA-aware: only a shared STRONG (non-reused, non-weak) domain is evidence
    return CustomComparison(
        output_column_name="website_domain_norm",
        comparison_description="shared strong website domain (reuse/OTA-aware)",
        comparison_levels=[
            cll.NullLevel("website_domain_norm"),
            cll.CustomLevel(
                "website_domain_norm_l = website_domain_norm_r "
                "AND NOT (website_reused_flag_l OR website_reused_flag_r) "
                "AND NOT (website_weak_domain_flag_l OR website_weak_domain_flag_r)",
                "same strong domain",
            ),
            cll.ElseLevel(),
        ],
    )


def _star_comparison_v2() -> CustomComparison:
    return CustomComparison(
        output_column_name="star_rating_norm",
        comparison_description="star rating proximity (weak)",
        comparison_levels=[
            cll.NullLevel("star_rating_norm"),
            cll.AbsoluteDifferenceLevel("star_rating_norm", 0.5),
            cll.ElseLevel(),
        ],
    )


def _comparisons_v2() -> list:
    return [
        _name_comparison_v2(),                                  # identity: name
        cl.ArrayIntersectAtSizes("phone_last10_non_reused_list", [1]),  # identity: phone
        cl.ArrayIntersectAtSizes("email_norm_list", [1]),       # identity: exact email
        _website_comparison_v2(),                               # identity: strong domain
        # location: distance ONLY (city/postal/h3 dropped — correlated w/ distance+blocking)
        cl.DistanceInKMAtThresholds("lat_norm", "lng_norm", [0.025, 0.075, 0.2, 0.5]),
        # context: weak
        cl.ExactMatch("hotel_chain_norm"),
        cl.ExactMatch("property_type_norm"),
        _star_comparison_v2(),
    ]


def _comparisons_v2_1() -> list:
    return [
        _name_comparison_v2_1(),
        cl.ArrayIntersectAtSizes("phone_last10_non_reused_list", [1]),
        cl.ArrayIntersectAtSizes("email_norm_list", [1]),
        _website_comparison_v2(),
        cl.DistanceInKMAtThresholds("lat_norm", "lng_norm", [0.025, 0.075, 0.2, 0.5]),
        cl.ExactMatch("hotel_chain_norm"),
        cl.ExactMatch("property_type_norm"),
        _star_comparison_v2(),
    ]


def em_training_rules_v2() -> list:
    # Identity-anchored EM rules ONLY — never geo-only, or EM learns "nearby = match".
    #   city+signature: name-anchored -> estimates phone/email/domain/distance/context
    #   primary_phone : contact-anchored -> estimates NAME/distance/context
    # (EM cannot use exploding array rules, so we anchor on the scalar primary_phone =
    # first non-reused phone; blocking on it does not block the phone-array comparison.)
    return [
        block_on("city_name_norm", "property_name_signature"),
        block_on("primary_phone"),
    ]


def build_settings_v2() -> SettingsCreator:
    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="record_id",
        comparisons=_comparisons_v2(),
        blocking_rules_to_generate_predictions=BLOCKING_RULES,
        retain_matching_columns=False,
        retain_intermediate_calculation_columns=False,
        additional_columns_to_retain=["provider"],
        probability_two_random_records_match=1e-4,
    )


def build_settings_v2_1() -> SettingsCreator:
    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="record_id",
        comparisons=_comparisons_v2_1(),
        blocking_rules_to_generate_predictions=BLOCKING_RULES,
        retain_matching_columns=False,
        retain_intermediate_calculation_columns=False,
        additional_columns_to_retain=["provider"],
        probability_two_random_records_match=1e-4,
    )


def build_settings() -> SettingsCreator:
    # Keep predict() output slim across ~60M pairs: ids + provider + score + gamma
    # levels only. Display fields for samples are joined back from the prepped table
    # by record_id (see run_splink_scoring). "Which levels carry the model" comes from
    # the trained m/u parameters, not per-pair intermediate columns.
    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="record_id",
        comparisons=_comparisons(),
        blocking_rules_to_generate_predictions=BLOCKING_RULES,
        retain_matching_columns=False,
        retain_intermediate_calculation_columns=False,
        additional_columns_to_retain=["provider"],
        probability_two_random_records_match=1e-4,
    )
