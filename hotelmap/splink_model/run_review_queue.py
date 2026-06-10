"""Stage 09E — clerical review queue from v2.1 review artifacts.

Builds an action-oriented queue, not a random dump. The queue combines:
  - review clusters from guarded v2.1 clustering
  - high-value gated review pairs that did not become auto-cluster edges

Review decisions are intended to become training/evaluation data, so this also
writes an empty decision-log template with the required fields.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import polars as pl

DEFAULT_VERSION = "v2_1"
AUTO_MIN_PROB = 0.99
REVIEW_PAIR_MIN_PROB = 0.90
MEDIUM_NAME_MIN_PROB = 0.80
TOP_CSV_LIMIT = 5000

CONTACT_REASONS = ("phone", "exact_email", "non_reused_domain")
STRONG_AUTO_REASONS = ("signature", *CONTACT_REASONS)


def _q(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _paths(
    normalized: str,
    clusters_dir: str | None,
    gated: str | None,
    output_dir: str | None,
    version: str,
) -> tuple[Path, Path, Path, Path]:
    run_dir = Path(normalized).parent
    clusters = Path(clusters_dir) if clusters_dir else run_dir / "splink_v2_1" / "clusters"
    # gated pairs are a v2_1 artifact for all cluster-rule versions (v2_2 only
    # changes clustering guardrails, not scoring/gating)
    gated_path = Path(gated) if gated else run_dir / "splink_v2_1" / "threshold" / "gated_pairs_v2_1.parquet"
    default_out = "review_queue" if version == DEFAULT_VERSION else f"review_queue_{version}"
    out = Path(output_dir) if output_dir else run_dir / "splink_v2_1" / default_out
    return run_dir, clusters, gated_path, out


def _write_decision_template(out: Path) -> None:
    schema = {
        "review_id": pl.Utf8,
        "entity_a": pl.Utf8,
        "entity_b": pl.Utf8,
        "decision": pl.Utf8,
        "reviewer": pl.Utf8,
        "reviewed_at": pl.Utf8,
        "reason": pl.Utf8,
        "notes": pl.Utf8,
    }
    pl.DataFrame(schema=schema).write_parquet(out / "review_decisions_template.parquet")


def run(
    normalized: str,
    clusters_dir: str | None = None,
    gated: str | None = None,
    output_dir: str | None = None,
    version: str = DEFAULT_VERSION,
) -> dict:
    run_dir, clusters, gated_path, out = _paths(normalized, clusters_dir, gated, output_dir, version)
    out.mkdir(parents=True, exist_ok=True)

    cluster_diag = clusters / f"cluster_diagnostics_{version}.parquet"
    cluster_edges = clusters / f"cluster_edges_{version}.parquet"
    cluster_members = clusters / f"cluster_members_{version}.parquet"

    con = duckdb.connect()
    con.execute(f"CREATE VIEW h AS SELECT * FROM read_parquet('{normalized}')")
    con.execute(f"CREATE VIEW cd AS SELECT * FROM read_parquet('{cluster_diag}')")
    con.execute(f"CREATE VIEW ce AS SELECT * FROM read_parquet('{cluster_edges}')")
    con.execute(f"CREATE VIEW cm AS SELECT * FROM read_parquet('{cluster_members}')")
    con.execute(f"CREATE VIEW gp AS SELECT * FROM read_parquet('{gated_path}')")

    # Members limited to v2.1 review clusters; singletons and auto-accepted clusters
    # are not part of the cluster review queue.
    con.execute(
        """
        CREATE OR REPLACE TABLE review_members_base AS
        SELECT cm.cluster_id, cm.record_id, h.provider, h.property_name_norm AS name,
               h.city_name_norm AS city, h.postal_code_final AS postal_code,
               h.phone_last10_non_reused_list AS phones,
               h.email_norm_list AS emails,
               h.website_domain_norm AS domain,
               h.lat_norm, h.lng_norm
        FROM cm
        JOIN cd USING (cluster_id)
        JOIN h ON cm.record_id = h.record_id
        WHERE cd.cluster_status = 'review'
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE member_text AS
        WITH names AS (
            SELECT cluster_id, name,
                   row_number() OVER (PARTITION BY cluster_id ORDER BY COUNT(*) DESC, name) AS rn
            FROM review_members_base WHERE name IS NOT NULL GROUP BY 1,2
        ),
        cities AS (
            SELECT cluster_id, city,
                   row_number() OVER (PARTITION BY cluster_id ORDER BY COUNT(*) DESC, city) AS rn
            FROM review_members_base WHERE city IS NOT NULL GROUP BY 1,2
        ),
        postals AS (
            SELECT cluster_id, postal_code,
                   row_number() OVER (PARTITION BY cluster_id ORDER BY COUNT(*) DESC, postal_code) AS rn
            FROM review_members_base WHERE postal_code IS NOT NULL GROUP BY 1,2
        )
        SELECT c.cluster_id,
               string_agg(DISTINCT c.provider, ', ' ORDER BY c.provider) AS providers,
               (SELECT string_agg(name, ' | ' ORDER BY rn) FROM names n
                WHERE n.cluster_id = c.cluster_id AND rn <= 5) AS representative_names,
               (SELECT string_agg(city, ', ' ORDER BY rn) FROM cities ct
                WHERE ct.cluster_id = c.cluster_id AND rn <= 5) AS cities,
               (SELECT string_agg(postal_code, ', ' ORDER BY rn) FROM postals p
                WHERE p.cluster_id = c.cluster_id AND rn <= 5) AS postal_codes
        FROM review_members_base c
        GROUP BY c.cluster_id
        """
    )

    # Shared contacts are values appearing on at least two records inside a cluster.
    con.execute(
        """
        CREATE OR REPLACE TABLE shared_phones AS
        WITH x AS (
            SELECT cluster_id, record_id, unnest(phones) AS phone
            FROM review_members_base
        ),
        c AS (
            SELECT cluster_id, phone, COUNT(DISTINCT record_id) n
            FROM x WHERE phone IS NOT NULL GROUP BY 1,2 HAVING n >= 2
        )
        SELECT cluster_id, string_agg(phone, ', ' ORDER BY phone) AS shared_phones
        FROM c GROUP BY 1
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE shared_emails AS
        WITH x AS (
            SELECT cluster_id, record_id, unnest(emails) AS email
            FROM review_members_base
        ),
        c AS (
            SELECT cluster_id, email, COUNT(DISTINCT record_id) n
            FROM x WHERE email IS NOT NULL GROUP BY 1,2 HAVING n >= 2
        )
        SELECT cluster_id, string_agg(email, ', ' ORDER BY email) AS shared_emails
        FROM c GROUP BY 1
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE shared_domains AS
        WITH c AS (
            SELECT cluster_id, domain, COUNT(DISTINCT record_id) n
            FROM review_members_base
            WHERE domain IS NOT NULL GROUP BY 1,2 HAVING n >= 2
        )
        SELECT cluster_id, string_agg(domain, ', ' ORDER BY domain) AS shared_domains
        FROM c GROUP BY 1
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE edge_agg AS
        SELECT cluster_id,
               MAX(match_probability) AS best_edge_probability,
               arg_max(gate_reason, match_probability) AS best_edge_reason,
               arg_min(gate_reason, match_probability) AS worst_edge_reason,
               bool_or(gate_reason = 'signature') AS has_signature_edge,
               bool_or(gate_reason = 'phone') AS has_phone_edge,
               bool_or(gate_reason = 'exact_email') AS has_exact_email_edge,
               bool_or(gate_reason = 'non_reused_domain') AS has_domain_edge,
               bool_or(gate_reason IN ('phone', 'exact_email', 'non_reused_domain')) AS has_contact_edge,
               bool_or(gate_reason = 'exact_email' AND name_match_jaccard >= 0.60) AS has_email_strong_name,
               bool_or(gate_reason = 'phone' AND name_match_jaccard >= 0.60) AS has_phone_strong_name,
               bool_or(gate_reason = 'signature' AND dist_m > 500) AS has_signature_geo_issue,
               bool_and(gate_reason IN ('phone', 'exact_email', 'non_reused_domain')) AS contact_only_edges,
               MIN(name_match_jaccard) AS min_edge_name_match_jaccard,
               median(name_match_jaccard) AS median_edge_name_match_jaccard
        FROM ce
        GROUP BY cluster_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE review_clusters AS
        SELECT
            'RQC_' || substr(md5(c.cluster_id), 1, 16) AS review_id,
            c.cluster_id,
            c.cluster_size,
            c.provider_count,
            mt.providers,
            mt.representative_names,
            mt.cities,
            mt.postal_codes,
            ROUND(c.max_geo_diameter_m) AS geo_diameter_m,
            e.best_edge_probability,
            e.best_edge_reason,
            e.worst_edge_reason,
            sp.shared_phones,
            se.shared_emails,
            sd.shared_domains,
            c.review_reasons,
            c.min_pairwise_name_match_jaccard,
            c.median_pairwise_name_match_jaccard,
            c.dominant_match_token_signature_share,
            CASE
                WHEN list_contains(c.review_reasons, 'villa_unit_conflict')
                    THEN 'villa_unit_conflict'
                WHEN list_contains(c.review_reasons, 'brand_conflict')
                    THEN 'brand_conflict'
                WHEN list_contains(c.review_reasons, 'transitive_drift_geo_gt_1km')
                    THEN 'transitive_drift'
                WHEN c.has_contact_evidence
                     AND (c.cluster_size > 20
                          OR c.max_geo_diameter_m > 1000
                          OR c.min_pairwise_name_match_jaccard < 0.45)
                    THEN 'contact_overmerge_risk'
                WHEN list_contains(c.review_reasons, 'same_provider_duplicate')
                     AND len(c.review_reasons) = 1
                     AND c.min_pairwise_name_match_jaccard >= 0.60
                     AND (c.max_geo_diameter_m <= 500 OR c.has_contact_evidence)
                    THEN 'same_provider_duplicate_only_coherent'
                WHEN (e.has_email_strong_name OR e.has_phone_strong_name)
                     AND list_contains(c.review_reasons, 'contact_cluster_name_sanity')
                     AND c.cluster_size <= 10
                    THEN 'contact_or_email_true_like_but_failed_coherence'
                WHEN e.has_email_strong_name
                     AND c.cluster_size <= 10
                     AND (c.max_geo_diameter_m <= 500 OR c.has_contact_evidence)
                    THEN 'exact_email_strong_name'
                WHEN e.has_phone_strong_name
                     AND c.cluster_size <= 10
                     AND (c.max_geo_diameter_m <= 500 OR c.has_contact_evidence)
                    THEN 'phone_strong_name'
                WHEN e.has_signature_geo_issue THEN 'signature_geo_diameter_issue'
                WHEN list_contains(c.review_reasons, 'same_provider_duplicate')
                    THEN 'same_provider_duplicates'
                ELSE 'needs_more_info'
            END AS review_reason,
            CASE
                WHEN review_reason IN (
                    'contact_or_email_true_like_but_failed_coherence',
                    'exact_email_strong_name',
                    'phone_strong_name',
                    'signature_geo_diameter_issue',
                    'same_provider_duplicate_only_coherent'
                ) THEN 'high_confidence_likely_true'
                WHEN review_reason IN ('contact_overmerge_risk', 'villa_unit_conflict', 'brand_conflict')
                    THEN 'contact_overmerge_risk'
                WHEN review_reason = 'transitive_drift' THEN 'transitive_drift'
                WHEN review_reason = 'same_provider_duplicates' THEN 'same_provider_duplicates'
                ELSE 'contact_overmerge_risk'
            END AS review_bucket,
            (
                CASE WHEN e.has_email_strong_name THEN 40 ELSE 0 END
              + CASE WHEN e.has_phone_strong_name THEN 35 ELSE 0 END
              + CASE WHEN e.has_signature_edge THEN 30 ELSE 0 END
              + LEAST(c.cluster_size, 10)
              + c.provider_count * 2
              + CASE WHEN list_contains(c.review_reasons, 'same_provider_duplicate') THEN 20 ELSE 0 END
              + CASE WHEN list_contains(c.review_reasons, 'geo_diameter_gt_500_no_contact') THEN 20 ELSE 0 END
              + CASE WHEN list_contains(c.review_reasons, 'oversize') THEN 25 ELSE 0 END
              + CASE WHEN e.contact_only_edges THEN 25 ELSE 0 END
              + CASE WHEN list_contains(c.review_reasons, 'transitive_drift_geo_gt_1km') THEN 30 ELSE 0 END
              + CASE WHEN list_contains(c.review_reasons, 'geo_diameter_gt_10km') THEN 30 ELSE 0 END
              + CASE WHEN list_contains(c.review_reasons, 'villa_unit_conflict') THEN 30 ELSE 0 END
              + CASE WHEN list_contains(c.review_reasons, 'brand_conflict') THEN 30 ELSE 0 END
              + CASE WHEN review_reason = 'contact_or_email_true_like_but_failed_coherence' THEN 15 ELSE 0 END
            )::INTEGER AS priority,
            CASE
                WHEN review_bucket = 'high_confidence_likely_true'
                     AND review_reason = 'same_provider_duplicate_only_coherent'
                    THEN 'provider_duplicate'
                WHEN review_bucket = 'high_confidence_likely_true' THEN 'accept_cluster'
                WHEN review_bucket IN ('contact_overmerge_risk', 'transitive_drift') THEN 'split_cluster'
                WHEN review_bucket = 'same_provider_duplicates' THEN 'provider_duplicate'
                ELSE 'needs_more_info'
            END AS suggested_action
        FROM cd c
        JOIN member_text mt USING (cluster_id)
        LEFT JOIN edge_agg e USING (cluster_id)
        LEFT JOIN shared_phones sp USING (cluster_id)
        LEFT JOIN shared_emails se USING (cluster_id)
        LEFT JOIN shared_domains sd USING (cluster_id)
        WHERE c.cluster_status = 'review'
        """
    )

    # Pair-level queue items: high-confidence contact/email pairs below auto
    # threshold, plus medium-name-plus-geo review/training pool.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE auto_edge_keys AS
        SELECT record_id_l, record_id_r FROM ce
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE review_pair_edges AS
        SELECT
            'RQE_' || substr(md5(g.record_id_l || '|' || g.record_id_r || '|' || g.gate_reason), 1, 16) AS review_id,
            'PAIR_' || substr(md5(g.record_id_l || '|' || g.record_id_r), 1, 16) AS cluster_id,
            g.record_id_l, g.record_id_r, g.provider_l, g.provider_r,
            g.match_probability, g.gate_reason, g.name_level, g.name_match_jaccard,
            g.sig_exact, g.dist_m, g.phone_ov, g.same_postal,
            l.property_name_norm AS name_l, r.property_name_norm AS name_r,
            l.city_name_norm AS city_l, r.city_name_norm AS city_r,
            l.postal_code_final AS postal_l, r.postal_code_final AS postal_r,
            list_intersect(COALESCE(l.phone_last10_non_reused_list, []), COALESCE(r.phone_last10_non_reused_list, [])) AS shared_phone_list,
            list_intersect(COALESCE(l.email_norm_list, []), COALESCE(r.email_norm_list, [])) AS shared_email_list,
            CASE WHEN l.website_domain_norm = r.website_domain_norm THEN l.website_domain_norm ELSE NULL END AS shared_domain
        FROM gp g
        LEFT JOIN auto_edge_keys a USING (record_id_l, record_id_r)
        JOIN h l ON g.record_id_l = l.record_id
        JOIN h r ON g.record_id_r = r.record_id
        WHERE a.record_id_l IS NULL
          AND (
              (g.gate_reason IN ({_q(STRONG_AUTO_REASONS)})
               AND g.match_probability >= {REVIEW_PAIR_MIN_PROB}
               AND g.match_probability < {AUTO_MIN_PROB})
              OR (g.gate_reason = 'medium_name_plus_geo'
                  AND g.match_probability >= {MEDIUM_NAME_MIN_PROB})
          )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE review_pair_queue AS
        SELECT
            review_id,
            cluster_id,
            2 AS cluster_size,
            2 AS provider_count,
            provider_l || ', ' || provider_r AS providers,
            name_l || ' | ' || name_r AS representative_names,
            CASE WHEN city_l IS NOT DISTINCT FROM city_r THEN city_l
                 ELSE COALESCE(city_l, '') || ', ' || COALESCE(city_r, '') END AS cities,
            CASE WHEN postal_l IS NOT NULL AND postal_l = postal_r THEN postal_l
                 ELSE COALESCE(postal_l, '') || ', ' || COALESCE(postal_r, '') END AS postal_codes,
            dist_m AS geo_diameter_m,
            match_probability AS best_edge_probability,
            gate_reason AS best_edge_reason,
            gate_reason AS worst_edge_reason,
            list_string_agg(shared_phone_list) AS shared_phones,
            list_string_agg(shared_email_list) AS shared_emails,
            shared_domain AS shared_domains,
            []::VARCHAR[] AS review_reasons,
            name_match_jaccard AS min_pairwise_name_match_jaccard,
            name_match_jaccard AS median_pairwise_name_match_jaccard,
            CASE
                WHEN gate_reason = 'exact_email' AND dist_m <= 500
                    THEN 'contact_or_email_true_like_but_failed_coherence'
                WHEN gate_reason = 'phone' AND name_match_jaccard >= 0.60 AND dist_m <= 500
                    THEN 'contact_or_email_true_like_but_failed_coherence'
                WHEN gate_reason = 'exact_email' AND name_match_jaccard >= 0.60
                    THEN 'exact_email_strong_name'
                WHEN gate_reason = 'phone' AND name_match_jaccard >= 0.60
                    THEN 'phone_strong_name'
                WHEN gate_reason = 'signature' THEN 'signature_review_pair'
                WHEN gate_reason = 'medium_name_plus_geo' THEN 'medium_name_plus_geo'
                ELSE 'needs_more_info'
            END AS review_reason,
            CASE
                WHEN review_reason IN (
                    'contact_or_email_true_like_but_failed_coherence',
                    'exact_email_strong_name',
                    'phone_strong_name',
                    'signature_review_pair'
                ) THEN 'high_confidence_likely_true'
                WHEN review_reason = 'medium_name_plus_geo' THEN 'medium_name_plus_geo'
                ELSE 'medium_name_plus_geo'
            END AS review_bucket,
            (
                CASE WHEN gate_reason = 'exact_email'
                          AND (name_match_jaccard >= 0.60 OR dist_m <= 500)
                     THEN 40 ELSE 0 END
              + CASE WHEN gate_reason = 'phone' AND name_match_jaccard >= 0.60 THEN 35 ELSE 0 END
              + CASE WHEN gate_reason = 'signature' THEN 30 ELSE 0 END
              + CASE WHEN gate_reason = 'medium_name_plus_geo' THEN 10 ELSE 0 END
              + 2 + 4
              + CASE WHEN review_reason = 'contact_or_email_true_like_but_failed_coherence' THEN 140 ELSE 0 END
            )::INTEGER AS priority,
            CASE
                WHEN review_bucket = 'high_confidence_likely_true' THEN 'accept_pair_only'
                WHEN review_bucket = 'medium_name_plus_geo' THEN 'accept_pair_only'
                ELSE 'needs_more_info'
            END AS suggested_action
        FROM review_pair_edges
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE review_queue_top_all AS
        SELECT review_id, priority, review_bucket, cluster_id, cluster_size, providers,
               representative_names, cities, postal_codes, geo_diameter_m,
               best_edge_probability, best_edge_reason, worst_edge_reason,
               shared_phones, shared_emails, shared_domains, suggested_action,
               review_reason, 'cluster' AS review_entity_type
        FROM review_clusters
        UNION ALL
        SELECT review_id, priority, review_bucket, cluster_id, cluster_size, providers,
               representative_names, cities, postal_codes, geo_diameter_m,
               best_edge_probability, best_edge_reason, worst_edge_reason,
               shared_phones, shared_emails, shared_domains, suggested_action,
               review_reason, 'pair' AS review_entity_type
        FROM review_pair_queue
        """
    )

    # Output cluster / edge / member support tables.
    con.execute(
        f"COPY (SELECT * FROM review_clusters ORDER BY priority DESC, cluster_size DESC) "
        f"TO '{out / 'review_queue_clusters.parquet'}' (FORMAT parquet)"
    )
    con.execute(
        f"""
        COPY (
            SELECT rc.review_id, 'cluster' AS review_entity_type,
                   ce.cluster_id, ce.record_id_l, ce.record_id_r, ce.provider_l, ce.provider_r,
                   ce.match_probability, ce.gate_reason, ce.name_match_jaccard,
                   ce.sig_exact, ce.dist_m, ce.phone_ov
            FROM ce JOIN review_clusters rc USING (cluster_id)
            UNION ALL
            SELECT review_id, 'pair' AS review_entity_type,
                   cluster_id, record_id_l, record_id_r, provider_l, provider_r,
                   match_probability, gate_reason, name_match_jaccard,
                   sig_exact, dist_m, phone_ov
            FROM review_pair_edges
        ) TO '{out / 'review_queue_edges.parquet'}' (FORMAT parquet)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT rc.review_id, 'cluster' AS review_entity_type, m.cluster_id,
                   m.record_id, m.provider, m.name, m.city, m.postal_code,
                   m.lat_norm, m.lng_norm
            FROM review_members_base m JOIN review_clusters rc USING (cluster_id)
            UNION ALL
            SELECT review_id, 'pair' AS review_entity_type, cluster_id,
                   record_id_l AS record_id, provider_l AS provider, name_l AS name,
                   city_l AS city, postal_l AS postal_code, NULL::DOUBLE AS lat_norm,
                   NULL::DOUBLE AS lng_norm
            FROM review_pair_edges
            UNION ALL
            SELECT review_id, 'pair' AS review_entity_type, cluster_id,
                   record_id_r AS record_id, provider_r AS provider, name_r AS name,
                   city_r AS city, postal_r AS postal_code, NULL::DOUBLE AS lat_norm,
                   NULL::DOUBLE AS lng_norm
            FROM review_pair_edges
        ) TO '{out / 'review_queue_members.parquet'}' (FORMAT parquet)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT * FROM (
                SELECT *, row_number() OVER (PARTITION BY review_bucket ORDER BY priority DESC, best_edge_probability DESC) AS bucket_rank
                FROM review_queue_top_all
            )
            WHERE bucket_rank <= 200
            ORDER BY review_bucket, priority DESC, best_edge_probability DESC
        ) TO '{out / 'review_queue_samples.parquet'}' (FORMAT parquet)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT review_id, priority, review_bucket, cluster_id, cluster_size, providers,
                   representative_names, cities, postal_codes, geo_diameter_m,
                   best_edge_probability, best_edge_reason, worst_edge_reason,
                   shared_phones, shared_emails, shared_domains, suggested_action
            FROM review_queue_top_all
            ORDER BY priority DESC, best_edge_probability DESC
            LIMIT {TOP_CSV_LIMIT}
        ) TO '{out / 'review_queue_top.csv'}' (HEADER, DELIMITER ',')
        """
    )

    _write_decision_template(out)

    summary = {
        "version": version,
        "inputs": {
            "normalized": normalized,
            "cluster_diagnostics": str(cluster_diag),
            "cluster_edges": str(cluster_edges),
            "gated_pairs": str(gated_path),
        },
        "outputs": {
            "review_queue_clusters": str(out / "review_queue_clusters.parquet"),
            "review_queue_edges": str(out / "review_queue_edges.parquet"),
            "review_queue_members": str(out / "review_queue_members.parquet"),
            "review_queue_samples": str(out / "review_queue_samples.parquet"),
            "review_queue_top_csv": str(out / "review_queue_top.csv"),
            "review_decisions_template": str(out / "review_decisions_template.parquet"),
        },
        "counts": con.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM review_clusters) AS cluster_review_items,
                (SELECT COUNT(*) FROM review_pair_queue) AS pair_review_items,
                (SELECT COUNT(*) FROM review_queue_top_all) AS total_review_items,
                (SELECT COUNT(*) FROM review_pair_queue WHERE review_reason='contact_or_email_true_like_but_failed_coherence') AS true_like_contact_email_pairs
            """
        ).pl().to_dicts()[0],
        "bucket_counts": con.execute(
            """
            SELECT review_bucket, review_entity_type, COUNT(*) AS item_count,
                   ROUND(AVG(priority), 1) AS avg_priority,
                   MAX(priority) AS max_priority
            FROM review_queue_top_all
            GROUP BY 1,2
            ORDER BY max_priority DESC, item_count DESC
            """
        ).pl().to_dicts(),
        "suggested_action_counts": con.execute(
            """
            SELECT suggested_action, review_entity_type, COUNT(*) AS item_count
            FROM review_queue_top_all
            GROUP BY 1,2
            ORDER BY item_count DESC
            """
        ).pl().to_dicts(),
        "decision_values": [
            "same_hotel",
            "different_hotel",
            "same_property_group_not_same_hotel",
            "unclear",
        ],
    }
    (out / "review_queue_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print("[09E] review queue summary:", json.dumps(summary["counts"], indent=2, default=str))
    print(f"[09E] artifacts -> {out}")
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build 09E clerical review queue")
    p.add_argument("--normalized", required=True)
    p.add_argument("--clusters-dir", default=None)
    p.add_argument("--gated", default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--version", default=DEFAULT_VERSION, choices=["v2_1", "v2_2", "v2_3"])
    args = p.parse_args(argv)
    run(args.normalized, args.clusters_dir, args.gated, args.output_dir, args.version)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
