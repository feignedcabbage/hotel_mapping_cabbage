"""URL / website-domain normalization.

Extracts the registered domain from weburl. Domains are a useful identity signal,
but OTA/aggregator/supplier domains (booking.com, agoda.com, tboholidays.com, ...)
are weak and get flagged, as are domains reused across many hotels (chain sites).
"""

from __future__ import annotations

from typing import Any

import polars as pl
import tldextract

# offline extractor: use the bundled public-suffix snapshot, no network fetch
_EXTRACT = tldextract.TLDExtract(suffix_list_urls=())

_STRUCT = pl.Struct({"host": pl.Utf8, "registered": pl.Utf8})


def _parse_url(raw: str | None) -> dict[str, str | None]:
    if not raw:
        return {"host": None, "registered": None}
    text = raw.strip().lower()
    if not text or text in {"-", "na", "n/a"}:
        return {"host": None, "registered": None}
    ext = _EXTRACT(text)
    if not ext.domain:
        return {"host": None, "registered": None}
    registered = ext.registered_domain or None
    host = ".".join(p for p in (ext.subdomain, ext.domain, ext.suffix) if p) or None
    return {"host": host, "registered": registered}


def add_url_features(df: pl.DataFrame, config: dict[str, Any]) -> pl.DataFrame:
    weak_domains = set(config.get("weak_domains", []))
    reuse_min = config.get("reuse", {}).get("domain_reuse_min", 20)

    parsed = pl.col("weburl").map_elements(_parse_url, return_dtype=_STRUCT)
    df = df.with_columns(
        pl.col("weburl").alias("weburl_raw"),
        parsed.alias("_u"),
    ).with_columns(
        pl.col("_u").struct.field("host").alias("website_host_norm"),
        pl.col("_u").struct.field("registered").alias("website_domain_norm"),
    ).drop("_u")

    df = df.with_columns(
        pl.col("website_domain_norm").is_in(list(weak_domains)).alias("website_weak_domain_flag")
    )

    counts = (
        df.select("record_id", "website_domain_norm")
        .drop_nulls("website_domain_norm")
        .unique(["record_id", "website_domain_norm"])
        .group_by("website_domain_norm")
        .agg(pl.len().alias("n"))
    )
    reused = counts.filter(pl.col("n") >= reuse_min).get_column("website_domain_norm").to_list()

    return df.with_columns(
        pl.col("website_domain_norm").is_in(reused).fill_null(False).alias("website_reused_flag")
    )
