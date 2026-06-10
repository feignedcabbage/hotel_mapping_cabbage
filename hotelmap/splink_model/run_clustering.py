"""Stage 09D — safe clustering on AUTO-MATCH edges only (NO survivorship yet).

Auto-match edge = gate reason in {signature, phone, exact_email, non_reused_domain}
AND match_probability >= 0.99. We build connected components with union-find, then
apply guardrails so a connected component is only auto-accepted if it is internally
coherent. The rest go to a review bucket. Singletons (no auto edge) are preserved.

Guardrails (auto-accept requires ALL):
  - no same-provider duplicate members
  - cluster_size <= MAX_SIZE
  - dominant name-signature share >= MIN_SIG_SHARE  (no severe name conflict)  OR contact evidence
  - geo diameter <= MAX_DIAMETER_M                                              OR contact evidence
Transitive-drift safeguard (size >= 4): geo diameter > 1km with no contact evidence
=> review, even if pairwise edges looked fine.

Outputs in <run>/splink_v2/clusters/.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import polars as pl

from hotelmap.normalize.config import load_config
from hotelmap.splink_model.run_splink_scoring import _match_token_frame

MAX_SIZE = 20
MAX_DIAMETER_M = 500
DRIFT_DIAMETER_M = 1000
HARD_DIAMETER_M = 10_000  # v2_2+: not waivable by contact evidence
MIN_SIG_SHARE = 0.6
# v2_2 benign same-provider duplicate: every same-provider pair must agree on
# name (exact match signature OR match-token jaccard >= threshold) AND sit close.
SP_BENIGN_JACCARD = 0.6
SP_BENIGN_DIST_M = 300
# v2_3 apartment building-level consolidation: unit-style listings in the SAME
# building map together (entity_level='building'); resort villas with distinct
# unit numbers never auto-merge.
BUILDING_APT_SHARE = 0.5
BUILDING_MAX_VILLA_SHARE = 0.3
BUILDING_MAX_DIAMETER_M = 250
BUILDING_MAX_SIZE = 200
BUILDING_TOP_TOKEN_SHARE = 0.8
VILLA_SHARE_GUARD = 0.3
AUTO_REASONS = ("signature", "phone", "exact_email", "non_reused_domain")
CONTACT_REASONS = ("phone", "exact_email", "non_reused_domain")
AUTO_MIN_PROB = 0.99

# representative preference (lower rank wins); not final survivorship
PROVIDER_RANK = [
    "expedia", "hotelbeds", "tbo", "agoda", "cleartrip", "ratehawk",
    "tripjack", "rezlive", "veturis", "grnc", "restel", "ioxl",
]


def _union_find(edges: pl.DataFrame) -> dict[str, str]:
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        # path compression
        while parent.get(x, x) != root:
            parent[x], x = root, parent[x]
        return root

    for a, b in zip(edges["record_id_l"].to_list(), edges["record_id_r"].to_list()):
        parent.setdefault(a, a)
        parent.setdefault(b, b)
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra
    return {node: find(node) for node in parent}


def _contact_only_auto_count(edge_path: Path, accept_path: Path) -> int:
    if not edge_path.exists() or not accept_path.exists():
        return 0
    contacts = ", ".join(f"'{r}'" for r in CONTACT_REASONS)
    con = duckdb.connect()
    return con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT e.cluster_id
            FROM read_parquet('{edge_path}') e
            JOIN read_parquet('{accept_path}') a USING (cluster_id)
            GROUP BY e.cluster_id
            HAVING bool_or(e.gate_reason IN ({contacts}))
               AND NOT bool_or(e.gate_reason = 'signature')
        )
        """
    ).fetchone()[0]


def _write_cluster_delta(normalized: str, out: Path, summary: dict, version: str) -> None:
    if version == "v2_3":
        old_summary_path = out / "clustering_summary_v2_2.json"
        old_edge_path = out / "cluster_edges_v2_2.parquet"
        old_accept_path = out / "clusters_auto_accept_v2_2.parquet"
        old_label, new_label = "v2_2", "v2_3"
        delta_name = "v2_2_vs_v2_3_cluster_delta.parquet"
    elif version == "v2_2":
        old_summary_path = out / "clustering_summary_v2_1.json"
        old_edge_path = out / "cluster_edges_v2_1.parquet"
        old_accept_path = out / "clusters_auto_accept_v2_1.parquet"
        old_label, new_label = "v2_1", "v2_2"
        delta_name = "v2_1_vs_v2_2_cluster_delta.parquet"
    else:
        old_dir = Path(normalized).parent / "splink_v2" / "clusters"
        old_summary_path = old_dir / "clustering_summary.json"
        old_edge_path = old_dir / "cluster_edges.parquet"
        old_accept_path = old_dir / "clusters_auto_accept.parquet"
        old_label, new_label = "v2", "v2_1"
        delta_name = "v2_vs_v2_1_cluster_delta.parquet"
    if not old_summary_path.exists():
        return
    old = json.loads(old_summary_path.read_text())
    old_contact = _contact_only_auto_count(old_edge_path, old_accept_path)
    rows = []
    for metric, old_key, new_key in [
        ("auto_match_edges", "auto_match_edges", "auto_match_edges"),
        ("auto_accepted_clusters", "auto_accepted", "auto_accepted"),
        ("review_clusters", "review", "review"),
        ("largest_cluster_size", "largest_cluster_size", "largest_cluster_size"),
    ]:
        rows.append(
            {
                "metric": metric,
                old_label: int(old.get(old_key, 0)),
                new_label: int(summary.get(new_key, 0)),
                "delta": int(summary.get(new_key, 0)) - int(old.get(old_key, 0)),
            }
        )
    rows.append(
        {
            "metric": "contact_only_auto_clusters",
            old_label: int(old_contact),
            new_label: int(summary.get("contact_only_auto_clusters", 0)),
            "delta": int(summary.get("contact_only_auto_clusters", 0)) - int(old_contact),
        }
    )
    pl.DataFrame(rows).write_parquet(out / delta_name)


def run(normalized: str, gated: str, out: Path, version: str, config_path: str) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    use_match_tokens = version in ("v2_1", "v2_2", "v2_3")
    cfg = load_config(config_path)
    country = str(cfg.get("country", "IN")).upper()
    # brand families (config `brand_groups:`): sub-brand tokens that are naming
    # variants of the SAME property (OYO / Capital O / Spot On ...) collapse to
    # one group before the brand-conflict disjoint test. Do NOT group families
    # whose sub-brands denote different properties (Marriott's Courtyard vs
    # Fairfield must keep conflicting).
    brand_cases = " ".join(
        f"WHEN t IN ({', '.join(repr(str(tok)) for tok in toks)}) THEN {grp!r}"
        for grp, toks in (cfg.get("brand_groups") or {}).items()
        if toks
    )
    brand_expr = (
        f"list_distinct(list_transform(brand_tokens, t -> CASE {brand_cases} ELSE t END))"
        if brand_cases
        else "brand_tokens"
    )
    con = duckdb.connect()
    con.execute(f"CREATE VIEW gated AS SELECT * FROM read_parquet('{gated}')")
    if use_match_tokens:
        con.register("_norm_source", _match_token_frame(normalized, config_path))
        con.execute("CREATE VIEW h AS SELECT * FROM _norm_source")
    else:
        con.execute(f"CREATE VIEW h AS SELECT * FROM read_parquet('{normalized}')")
    suffix = {"v2": "", "v2_1": "_v2_1", "v2_2": "_v2_2", "v2_3": "_v2_3"}[version]

    reasons = ", ".join(f"'{r}'" for r in AUTO_REASONS)
    contact_safety = ""
    if use_match_tokens:
        contact_safety = """
          AND (
              gate_reason = 'signature'
              OR COALESCE(sig_exact, FALSE)
              OR COALESCE(name_match_jaccard, 0) >= 0.30
          )
        """
        edge_extra_cols = """
               COALESCE(name_match_jaccard, NULL) AS name_match_jaccard,
               COALESCE(sig_exact, FALSE) AS sig_exact
        """
    else:
        edge_extra_cols = """
               NULL::DOUBLE AS name_match_jaccard,
               FALSE AS sig_exact
        """
    edges = con.execute(
        f"""
        SELECT record_id_l, record_id_r, provider_l, provider_r, match_probability,
               gate_reason, dist_m, phone_ov,
               {edge_extra_cols}
        FROM gated
        WHERE gate_reason IN ({reasons}) AND match_probability >= {AUTO_MIN_PROB}
        {contact_safety}
        """
    ).pl()
    n_edges = edges.height
    con.register("auto_edges", edges)
    print(f"[09D] auto-match edges: {n_edges:,}")

    roots = _union_find(edges)
    uf = pl.DataFrame({"record_id": list(roots.keys()), "root": list(roots.values())})
    con.register("uf", uf)
    print(f"[09D] nodes in clusters: {uf.height:,}; components: {uf['root'].n_unique():,}")

    # member attributes
    rank_case = " ".join(f"WHEN provider='{p}' THEN {i}" for i, p in enumerate(PROVIDER_RANK))
    match_cols = (
        "h.property_name_match_signature AS match_sig, h.property_name_match_tokens AS match_tokens,"
        if use_match_tokens
        else "h.property_name_signature AS match_sig, h.property_name_core_tokens AS match_tokens,"
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE members AS
        SELECT uf.record_id, uf.root, h.provider, h.property_name_signature AS sig,
               {match_cols}
               h.property_name_norm AS name, h.lat_norm, h.lng_norm,
               h.property_type_norm AS property_type,
               h.property_name_brand_tokens AS brand_tokens,
               CASE {rank_case} ELSE 99 END AS prov_rank
        FROM uf JOIN h ON uf.record_id = h.record_id
        """
    )

    # edge-level diagnostics per component (edge endpoints share a root)
    con.execute(
        """
        CREATE OR REPLACE TABLE edge_diag AS
        WITH e AS (
            SELECT uf.root, g.match_probability, g.gate_reason
            FROM auto_edges g JOIN uf ON g.record_id_l = uf.record_id
        )
        SELECT root,
            COUNT(*) AS edge_count,
            MIN(match_probability) AS min_edge_probability,
            bool_or(gate_reason = 'phone') AS has_phone_edge,
            bool_or(gate_reason = 'exact_email') AS has_email_edge,
            bool_or(gate_reason = 'non_reused_domain') AS has_domain_edge,
            arg_min(gate_reason, match_probability) AS weakest_edge_reason
        FROM e GROUP BY root
        """
    )

    # signature dominance
    con.execute(
        """
        CREATE OR REPLACE TABLE sig_share AS
        WITH c AS (SELECT root, sig, COUNT(*) n FROM members WHERE sig IS NOT NULL GROUP BY 1,2)
        SELECT root, COUNT(*) AS name_signature_count, MAX(n) AS top_sig_n, SUM(n) AS sig_total
        FROM c GROUP BY root
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE match_sig_share AS
        WITH c AS (SELECT root, match_sig, COUNT(*) n FROM members WHERE match_sig IS NOT NULL AND match_sig <> '' GROUP BY 1,2)
        SELECT root, COUNT(*) AS match_token_signature_count, MAX(n) AS top_match_sig_n, SUM(n) AS match_sig_total
        FROM c GROUP BY root
        """
    )
    # provider duplication
    con.execute(
        """
        CREATE OR REPLACE TABLE prov_dup AS
        WITH c AS (SELECT root, provider, COUNT(*) n FROM members GROUP BY 1,2)
        SELECT root, MAX(n) AS max_same_provider, SUM(n) - COUNT(*) AS same_provider_duplicate_count
        FROM c GROUP BY root
        """
    )

    # cluster core: size, providers, geo bbox diameter, representative, canonical id
    con.execute(
        f"""
        CREATE OR REPLACE TABLE cluster_core AS
        SELECT root,
            COUNT(*) AS cluster_size,
            COUNT(DISTINCT provider) AS provider_count,
            list_distinct(list(provider)) AS providers_present,
            6371000 * acos(LEAST(1, GREATEST(-1,
                sin(radians(MIN(lat_norm)))*sin(radians(MAX(lat_norm))) +
                cos(radians(MIN(lat_norm)))*cos(radians(MAX(lat_norm)))*
                cos(radians(MAX(lng_norm) - MIN(lng_norm)))))) AS max_geo_diameter_m,
            arg_min(record_id, prov_rank) AS representative_record_id,
            '{country}_' || substr(md5(string_agg(record_id, ',' ORDER BY record_id)), 1, 16) AS cluster_id
        FROM members GROUP BY root
        """
    )

    # median pairwise edge distance per cluster (from edges)
    con.execute(
        """
        CREATE OR REPLACE TABLE edge_dist AS
        WITH e AS (SELECT uf.root, g.dist_m FROM auto_edges g JOIN uf ON g.record_id_l = uf.record_id)
        SELECT root, ROUND(median(dist_m)) AS median_pair_distance_m FROM e GROUP BY root
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE name_pair_diag AS
        WITH pairs AS (
            SELECT l.root,
                CASE
                    WHEN len(list_distinct(list_concat(COALESCE(l.match_tokens, []), COALESCE(r.match_tokens, [])))) = 0
                    THEN 0.0
                    ELSE len(list_intersect(COALESCE(l.match_tokens, []), COALESCE(r.match_tokens, [])))::DOUBLE
                       / len(list_distinct(list_concat(COALESCE(l.match_tokens, []), COALESCE(r.match_tokens, []))))
                END AS name_match_jaccard
            FROM members l
            JOIN members r ON l.root = r.root AND l.record_id < r.record_id
        )
        SELECT root,
               COALESCE(MIN(name_match_jaccard), 0) AS min_pairwise_name_match_jaccard,
               COALESCE(median(name_match_jaccard), 0) AS median_pairwise_name_match_jaccard
        FROM pairs GROUP BY root
        """
    )

    # same-provider pair diagnostics: is each intra-provider duplicate a benign
    # double-listing (same name, same place) or genuine unit ambiguity?
    con.execute(
        f"""
        CREATE OR REPLACE TABLE sp_pair_diag AS
        WITH p AS (
            SELECT a.root,
                CASE
                    WHEN len(list_distinct(list_concat(COALESCE(a.match_tokens, []), COALESCE(b.match_tokens, [])))) = 0
                    THEN 0.0
                    ELSE len(list_intersect(COALESCE(a.match_tokens, []), COALESCE(b.match_tokens, [])))::DOUBLE
                       / len(list_distinct(list_concat(COALESCE(a.match_tokens, []), COALESCE(b.match_tokens, []))))
                END AS jac,
                (a.match_sig = b.match_sig AND a.match_sig IS NOT NULL AND a.match_sig <> '') AS sig_eq,
                2 * 6371000 * asin(sqrt(
                    pow(sin(radians(b.lat_norm - a.lat_norm) / 2), 2)
                    + cos(radians(a.lat_norm)) * cos(radians(b.lat_norm))
                      * pow(sin(radians(b.lng_norm - a.lng_norm) / 2), 2))) AS dist_m
            FROM members a
            JOIN members b ON a.root = b.root AND a.provider = b.provider AND a.record_id < b.record_id
        )
        SELECT root,
            COUNT(*) AS same_provider_pair_count,
            MAX(dist_m) AS max_same_provider_pair_dist_m,
            MIN(jac) AS min_same_provider_pair_jaccard,
            bool_and((sig_eq OR jac >= {SP_BENIGN_JACCARD})
                     AND COALESCE(dist_m, 1e9) <= {SP_BENIGN_DIST_M}) AS same_provider_pairs_benign
        FROM p GROUP BY root
        """
    )

    # apartment/villa unit diagnostics (v2_3 policy: same-building units map
    # together at building level; numbered resort villas never auto-merge).
    con.execute(
        r"""
        CREATE OR REPLACE TABLE unit_flags AS
        SELECT root, record_id,
            (regexp_matches(name, '\b(apartment|apartments|apt|studio|bedroom|bedrooms|condo|penthouse|loft|residences)\b')
             OR COALESCE(property_type, '') IN ('apartment', 'aparthotel')) AS is_apartment_style,
            regexp_matches(name, '\bvillas?\b') AS is_villa_style,
            COALESCE(
                regexp_extract(name, '\b(?:apt|apartment|unit|room|suite|studio|flat|condo|villa|no)\s*#?\s*([0-9]{1,5})\b', 1),
                CASE WHEN regexp_matches(name, '\bvillas?\b')
                     THEN NULLIF(regexp_extract(name, '([0-9]{1,4})\s*$', 1), '') END
            ) AS unit_num
        FROM members
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE unit_diag AS
        SELECT root,
            AVG(CASE WHEN is_apartment_style THEN 1.0 ELSE 0.0 END) AS apartment_style_share,
            AVG(CASE WHEN is_villa_style THEN 1.0 ELSE 0.0 END) AS villa_style_share,
            COUNT(DISTINCT unit_num) FILTER (WHERE unit_num IS NOT NULL AND unit_num <> '') AS distinct_unit_numbers
        FROM unit_flags GROUP BY root
        """
    )
    # share of members carrying the cluster's modal match token ("same building
    # name" test; tight geo alone is not enough in dense CBD towers)
    con.execute(
        """
        CREATE OR REPLACE TABLE top_token_share AS
        WITH t AS (
            SELECT root, unnest(match_tokens) AS tok, record_id FROM members
        ),
        c AS (
            SELECT root, tok, COUNT(DISTINCT record_id) AS members_with
            FROM t WHERE tok IS NOT NULL AND tok <> '' GROUP BY 1, 2
        )
        SELECT root, MAX(members_with) AS top_token_members
        FROM c GROUP BY root
        """
    )
    # franchise brand conflict: two members whose (non-empty) brand-token sets are
    # DISJOINT are different flags ("residence inn by marriott" vs "staybridge
    # suites") — catches the US highway-strip pattern where match signatures
    # collapse to a shared suburb token. Dual-brand names ("home2 suites by
    # hilton") keep an overlap, so genuine clusters aren't flagged.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE brand_diag AS
        WITH b AS (
            SELECT root, record_id, {brand_expr} AS brand_tokens FROM members
            WHERE len(COALESCE(brand_tokens, [])) > 0
        )
        SELECT a.root,
            bool_or(len(list_intersect(a.brand_tokens, c.brand_tokens)) = 0) AS brand_conflict
        FROM b a JOIN b c ON a.root = c.root AND a.record_id < c.record_id
        GROUP BY a.root
        """
    )

    # assemble diagnostics + status
    con.execute(
        f"""
        CREATE OR REPLACE TABLE cluster_diag AS
        SELECT c.*, ed.edge_count, ed.min_edge_probability,
               ed.has_phone_edge, ed.has_email_edge, ed.has_domain_edge, ed.weakest_edge_reason,
               s.name_signature_count,
               COALESCE(s.top_sig_n::DOUBLE / NULLIF(s.sig_total, 0), 0) AS dominant_name_signature_share,
               pd.max_same_provider, pd.same_provider_duplicate_count,
               (pd.max_same_provider > 1) AS same_provider_dup,
               (ed.has_phone_edge OR ed.has_email_edge OR ed.has_domain_edge) AS has_contact_evidence,
               md.median_pair_distance_m,
               COALESCE(ms.top_match_sig_n::DOUBLE / NULLIF(ms.match_sig_total, 0), 0) AS dominant_match_token_signature_share,
               COALESCE(np.min_pairwise_name_match_jaccard, 0) AS min_pairwise_name_match_jaccard,
               COALESCE(np.median_pairwise_name_match_jaccard, 0) AS median_pairwise_name_match_jaccard,
               COALESCE(spd.same_provider_pair_count, 0) AS same_provider_pair_count,
               spd.max_same_provider_pair_dist_m,
               spd.min_same_provider_pair_jaccard,
               ((pd.max_same_provider > 1) AND COALESCE(spd.same_provider_pairs_benign, FALSE))
                   AS benign_same_provider_dup,
               COALESCE(ud.apartment_style_share, 0) AS apartment_style_share,
               COALESCE(ud.villa_style_share, 0) AS villa_style_share,
               COALESCE(ud.distinct_unit_numbers, 0) AS distinct_unit_numbers,
               COALESCE(tts.top_token_members::DOUBLE / NULLIF(c.cluster_size, 0), 0)
                   AS top_match_token_member_share,
               COALESCE(bd.brand_conflict, FALSE) AS brand_conflict
        FROM cluster_core c
        JOIN edge_diag ed USING (root)
        LEFT JOIN sig_share s USING (root)
        LEFT JOIN match_sig_share ms USING (root)
        JOIN prov_dup pd USING (root)
        LEFT JOIN edge_dist md USING (root)
        LEFT JOIN name_pair_diag np USING (root)
        LEFT JOIN sp_pair_diag spd USING (root)
        LEFT JOIN unit_diag ud USING (root)
        LEFT JOIN top_token_share tts USING (root)
        LEFT JOIN brand_diag bd USING (root)
        """
    )

    name_review_sql = (
        """
                CASE WHEN NOT has_contact_evidence AND min_pairwise_name_match_jaccard < 0.45
                     THEN 'name_match_jaccard_lt_0_45_no_contact' END,
                CASE WHEN has_contact_evidence AND cluster_size > 2
                          AND min_pairwise_name_match_jaccard < 0.30
                     THEN 'contact_cluster_name_sanity' END
        """
        if use_match_tokens
        else f"""
                CASE WHEN cluster_size >= 4 AND dominant_name_signature_share < {MIN_SIG_SHARE}
                     AND NOT has_contact_evidence THEN 'name_conflict' END
        """
    )
    # v2_2: a same-provider duplicate only blocks auto-accept when some intra-provider
    # pair actually disagrees (benign double-listings are survivorship's problem, not a
    # match risk); and contact evidence can no longer waive an unbounded geo diameter
    # (catches different same-name hotels in different towns chained by contact).
    spd_review_sql = (
        "CASE WHEN same_provider_dup AND NOT benign_same_provider_dup THEN 'same_provider_duplicate' END"
        if version in ("v2_2", "v2_3")
        else "CASE WHEN same_provider_dup THEN 'same_provider_duplicate' END"
    )
    hard_geo_sql = (
        f"CASE WHEN max_geo_diameter_m > {HARD_DIAMETER_M} THEN 'geo_diameter_gt_10km' END,"
        if version in ("v2_2", "v2_3")
        else ""
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE cluster_status AS
        SELECT *,
            list_filter([
                {spd_review_sql},
                CASE WHEN cluster_size > {MAX_SIZE} THEN 'oversize' END,
                CASE WHEN max_geo_diameter_m > {MAX_DIAMETER_M} AND NOT has_contact_evidence
                     THEN 'geo_diameter_gt_500_no_contact' END,
                CASE WHEN max_geo_diameter_m > {DRIFT_DIAMETER_M} AND NOT has_contact_evidence
                     THEN 'transitive_drift_geo_gt_1km' END,
                {hard_geo_sql}
                {name_review_sql}
            ], x -> x IS NOT NULL) AS review_reasons
        FROM cluster_diag
        """
    )
    if version == "v2_3":
        # Apartment building-level consolidation + numbered-villa guard.
        # building_merge: unit-heavy, geo-tight, one dominant building name token
        # -> the cluster IS the building; intra-provider unit dups, size and name
        # variance are expected, so those reasons are waived (entity_level says
        # what was merged). villa_unit_conflict: villa-flavored cluster spanning
        # >=2 distinct unit numbers must NEVER auto-merge (near-identical names
        # differing only in the unit number are different villas).
        building_waivable = (
            "['same_provider_duplicate', 'oversize', 'contact_cluster_name_sanity', "
            "'name_match_jaccard_lt_0_45_no_contact']"
        )
        con.execute(
            f"""
            CREATE OR REPLACE TABLE cluster_status_final AS
            SELECT * EXCLUDE (review_reasons, reasons_with_brand),
                CASE
                    WHEN is_villa_unit_conflict THEN list_append(reasons_with_brand, 'villa_unit_conflict')
                    WHEN is_building_merge
                    THEN list_filter(reasons_with_brand, x -> NOT list_contains({building_waivable}, x))
                    ELSE reasons_with_brand
                END AS review_reasons,
                CASE WHEN is_building_merge AND NOT is_villa_unit_conflict
                     THEN 'building' ELSE 'property' END AS entity_level
            FROM (
                SELECT *,
                    CASE WHEN brand_conflict THEN list_append(review_reasons, 'brand_conflict')
                         ELSE review_reasons END AS reasons_with_brand,
                    (apartment_style_share >= {BUILDING_APT_SHARE}
                     AND villa_style_share < {BUILDING_MAX_VILLA_SHARE}
                     AND max_geo_diameter_m <= {BUILDING_MAX_DIAMETER_M}
                     AND cluster_size <= {BUILDING_MAX_SIZE}
                     AND top_match_token_member_share >= {BUILDING_TOP_TOKEN_SHARE}
                     AND NOT brand_conflict) AS is_building_merge,
                    (villa_style_share >= {VILLA_SHARE_GUARD}
                     AND distinct_unit_numbers >= 2) AS is_villa_unit_conflict
                FROM cluster_status
            )
            """
        )
    else:
        con.execute(
            "CREATE OR REPLACE TABLE cluster_status_final AS "
            "SELECT *, FALSE AS is_building_merge, FALSE AS is_villa_unit_conflict, "
            "'property' AS entity_level FROM cluster_status"
        )
    con.execute(
        """
        CREATE OR REPLACE TABLE clusters AS
        SELECT *, CASE WHEN len(review_reasons) = 0 THEN 'auto_accept' ELSE 'review' END AS cluster_status
        FROM cluster_status_final
        """
    )

    # ---- write artifacts ----
    clusters = con.execute("SELECT * FROM clusters").pl()
    clusters.write_parquet(out / f"clusters_auto_raw{suffix}.parquet")
    con.execute("SELECT * FROM clusters WHERE cluster_status='auto_accept'").pl().write_parquet(
        out / f"clusters_auto_accept{suffix}.parquet")
    review = con.execute("SELECT * FROM clusters WHERE cluster_status='review'").pl()
    review.write_parquet(out / f"clusters_review{suffix}.parquet")

    # members with cluster_id + status (+ singletons)
    con.execute(
        """
        CREATE OR REPLACE TABLE member_map AS
        SELECT m.record_id, m.provider, c.cluster_id, c.cluster_status,
               (m.record_id = c.representative_record_id) AS is_representative
        FROM members m JOIN clusters c USING (root)
        """
    )
    con.execute(
        """
        COPY (
            SELECT record_id, provider, cluster_id,
                   CASE WHEN cluster_status='auto_accept' THEN 'matched_cluster'
                        ELSE 'review_cluster' END AS status,
                   is_representative
            FROM member_map
            UNION ALL
            SELECT h.record_id, h.provider, NULL AS cluster_id,
                   'singleton_unmatched' AS status, TRUE AS is_representative
            FROM h LEFT JOIN uf ON h.record_id = uf.record_id
            WHERE uf.record_id IS NULL
        ) TO '{}' (FORMAT parquet)
        """.format(out / f"cluster_members{suffix}.parquet")
    )

    # edges with cluster id
    con.execute(
        f"""
        COPY (
            SELECT g.*, c.cluster_id
            FROM auto_edges g JOIN uf ON g.record_id_l = uf.record_id JOIN clusters c USING (root)
        ) TO '{out / f"cluster_edges{suffix}.parquet"}' (FORMAT parquet)
        """
    )
    clusters.write_parquet(out / f"cluster_diagnostics{suffix}.parquet")

    # review samples: members of review clusters with reasons
    con.execute(
        f"""
        COPY (
            SELECT c.cluster_id, c.cluster_status, c.review_reasons, c.cluster_size,
                   c.provider_count, ROUND(c.max_geo_diameter_m) geo_diam_m,
                   c.dominant_name_signature_share, c.has_contact_evidence,
                   m.provider, m.name, m.lat_norm, m.lng_norm
            FROM clusters c JOIN members m USING (root)
            WHERE c.cluster_status='review'
              AND c.root IN (SELECT root FROM clusters WHERE cluster_status='review' USING SAMPLE 400 ROWS)
            ORDER BY c.cluster_id
        ) TO '{out / f"cluster_review_samples{suffix}.parquet"}' (FORMAT parquet)
        """
    )

    # ---- summary ----
    n_singletons = con.execute(
        "SELECT COUNT(*) FROM h LEFT JOIN uf ON h.record_id=uf.record_id WHERE uf.record_id IS NULL"
    ).fetchone()[0]
    agg = con.execute(
        f"""
        SELECT
            COUNT(*) clusters_total,
            COUNT(*) FILTER (WHERE cluster_status='auto_accept') auto_accepted,
            COUNT(*) FILTER (WHERE cluster_status='review') review,
            MAX(cluster_size) largest_cluster_size,
            SUM(cluster_size) FILTER (WHERE cluster_status='auto_accept') records_in_auto,
            SUM(cluster_size) FILTER (WHERE cluster_status='review') records_in_review,
            COUNT(*) FILTER (WHERE same_provider_dup) same_provider_violation,
            COUNT(*) FILTER (WHERE benign_same_provider_dup) benign_same_provider_dup_clusters,
            COUNT(*) FILTER (WHERE benign_same_provider_dup AND cluster_status='auto_accept') benign_same_provider_dup_auto_accepted,
            COUNT(*) FILTER (WHERE max_geo_diameter_m > 1000 AND NOT has_contact_evidence) geo_diam_violation,
            COUNT(*) FILTER (WHERE max_geo_diameter_m > {HARD_DIAMETER_M}) geo_diam_gt_10km_clusters,
            COUNT(*) FILTER (WHERE cluster_size > 20) oversize_violation,
            COUNT(*) FILTER (WHERE min_pairwise_name_match_jaccard<0.45 AND NOT has_contact_evidence) name_conflict_violation,
            COUNT(*) FILTER (WHERE is_building_merge AND cluster_status='auto_accept') building_merged_auto,
            COUNT(*) FILTER (WHERE is_villa_unit_conflict) villa_unit_conflict_clusters,
            COUNT(*) FILTER (WHERE brand_conflict) brand_conflict_clusters
        FROM clusters
        """
    ).pl().to_dicts()[0]
    contacts = ", ".join(f"'{r}'" for r in CONTACT_REASONS)
    contact_only_auto = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT c.cluster_id
            FROM auto_edges e JOIN uf ON e.record_id_l = uf.record_id JOIN clusters c USING (root)
            WHERE c.cluster_status='auto_accept'
            GROUP BY c.cluster_id
            HAVING bool_or(e.gate_reason IN ({contacts}))
               AND NOT bool_or(e.gate_reason = 'signature')
        )
        """
    ).fetchone()[0]
    review_reason_counts = con.execute(
        "SELECT reason, COUNT(*) c FROM "
        "(SELECT unnest(review_reasons) AS reason FROM clusters) GROUP BY 1 ORDER BY c DESC"
    ).pl().to_dicts()

    summary = {
        "auto_match_edges": int(n_edges),
        "records_clustered": int(uf.height),
        "singleton_unmatched": int(n_singletons),
        **{k: (int(v) if v is not None else 0) for k, v in agg.items()},
        "contact_only_auto_clusters": int(contact_only_auto),
        "top_review_reasons": review_reason_counts,
    }
    (out / f"clustering_summary{suffix}.json").write_text(json.dumps(summary, indent=2, default=str))
    if version in ("v2_1", "v2_2", "v2_3"):
        _write_cluster_delta(normalized, out, summary, version)
    print("[09D] summary:", json.dumps(summary, indent=2, default=str))
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Safe clustering on auto-match edges (09D)")
    p.add_argument("--normalized", required=True)
    p.add_argument("--gated", required=True, help="gated_pairs.parquet from 09C")
    p.add_argument("--output-dir", default=None)
    p.add_argument("--version", default="v2", choices=["v2", "v2_1", "v2_2", "v2_3"])
    p.add_argument("--config", default="configs/india.yaml")
    args = p.parse_args(argv)
    out = Path(args.output_dir) if args.output_dir else Path(args.gated).parent.parent / "clusters"
    run(args.normalized, args.gated, out, args.version, args.config)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
