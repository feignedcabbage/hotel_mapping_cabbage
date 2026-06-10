# AGENT.md — Hotel Mapping Normalization Pipeline

Working notes for the normalization pipeline (everything **before** Splink matching).
This file is the durable record of *what was built, why, and what the data actually
looks like*. Update it as each stage lands.

---

## Current resume checkpoint (Codex read-through, 2026-06-09)

- Workspace root: `/home/hyprganesh/Documents/hotel_mapping`.
- There is **no `.git` repository here**, so no `git log` history is available from
  this checkout. Project history is in this `AGENT.md` plus run artifacts. `.claude/`
  only contains local permission settings, not substantive project notes.
- Active/latest run baseline: `data/artifacts/runs/2026-06-09_005`.
- Environment verified: `.venv/bin/python` = Python 3.12.11; polars 1.41.2,
  duckdb 1.5.3, splink 4.0.16.
- Normalization guardrails pass on run 005: 764,686 IN records,
  `empty_name_core_rate` 3.17%, `country_norm_failure_rate` 0%,
  `name_norm_missing_rate` ~0.0014%.
- Splink scoring **v2.1** + clustering rules **v2.2** are the production baseline;
  do not roll back. v1/v2 exist as cautionary baselines: v1 over-counted correlated
  signals, and v2 was less conservative before match-token Jaccard/coherence.
  v2.2 changes CLUSTERING GUARDRAILS ONLY (same v2.1 scoring + gated pairs):
  benign same-provider duplicates no longer block auto-accept, and contact evidence
  can no longer waive a >10km geo diameter. See "Stage 09D-v2.2".
- The **identity gate is mandatory** before accepting matches. v2 still gives some
  location-only pairs high probabilities (`loc_only_max_score` ~0.9997), so raw
  probability alone is not safe.
- 09C threshold output says: gated pairs >=0.80 = 1,567,310; auto-match hypothesis
  (gated >=0.99) = 1,479,431; failed gate >=0.80 = 981,547.
- 09D clustering uses only strong auto-match edges
  `{signature, phone, exact_email, non_reused_domain}` at prob >=0.99.
  Current v2.2 results (run 005): 116,785 auto-accepted clusters / 468,128 records,
  9,440 review clusters / 85,031 records, 211,527 singletons.
- **Multi-country live (2026-06-10):** NZ (`configs/nz.yaml`, run `2026-06-10_NZ_001`)
  and US (`configs/us.yaml`, run `2026-06-10_US_001`) ran end-to-end (01→09E) with
  clustering **v2_3** (apartment building-level merge + villa-unit guard +
  franchise brand-conflict guard — see "Stage 09D-v2.3"). India still on v2_2
  artifacts; rerun with v2_3 when revisiting IN.
- **Clustering v2_4 live for IN + NZ + US (2026-06-10):** name-subgroup
  auto-SPLIT — cuts contact edges between conflicting core-token-signature
  subgroups (geo-split chains, OYO/unit id conflicts, rare-token conflicts)
  instead of routing to review. See "Stage 09D-v2.4". All runs have `*_v2_4`
  cluster artifacts + `review_queue_v2_4/`; dashboard prefers v2_4.
- Known next refinement candidates:
  1. Token geo-affinity ("local uniqueness"): use h3-level token stats (already
     computed, unused) to weight disagreeing tokens by distance to the token's
     geographic mass; separates binsar-class alien tokens from renamed listings.
  2. Tighten contact-only clustering further: small shared-phone/email clusters can still
     represent multiple properties with shared back-office contacts.
  3. Build survivorship/canonical records (09F), persistent canonical ID registry
     (09G), then incremental reruns (09H). Building-level clusters make this more
     urgent (clusters intentionally contain many unit listings).

Primary commands:

```
.venv/bin/python -m hotelmap.normalize.run --config configs/india.yaml
.venv/bin/python -m hotelmap.splink_model.run_blocking_diagnostics \
  --normalized data/artifacts/runs/<run>/normalized_hotels.parquet
.venv/bin/python -m hotelmap.splink_model.run_splink_scoring \
  --normalized data/artifacts/runs/<run>/normalized_hotels.parquet --version v2
.venv/bin/python -m hotelmap.splink_model.run_threshold_analysis \
  --normalized data/artifacts/runs/<run>/normalized_hotels.parquet \
  --scored data/artifacts/runs/<run>/splink_v2/splink_scored_pairs_v2.parquet
.venv/bin/python -m hotelmap.splink_model.run_clustering \
  --normalized data/artifacts/runs/<run>/normalized_hotels.parquet \
  --gated data/artifacts/runs/<run>/splink_v2/threshold/gated_pairs.parquet
```

---

## North star

Map as many provider hotel records to each other as possible **without wrong
mappings**. Normalization's job is to prepare clean, observable, feature-rich columns
so that Splink (later) can score combinations. Normalization never *decides* matches.

Core invariants:

```
Raw data is immutable.
Normalized data is derived.
Frequency reports are generated every run.
Weak-token lists are generated automatically but versioned.
Manual overrides are allowed through config.
```

We do **not** hand-decide "OYO is useless". We *measure* token usefulness from the
dataset, generate candidate weak/brand/location token lists, write diagnostics, and
let config tune it over time. Low-value never means deleted — weak tokens are
*separated*, not discarded.

---

## Scope of this phase

Build pipeline stages **01 → 08** (normalized parquet output). Do **not** start
address parsers beyond v1 abbreviations, and do **not** write Splink settings yet.

```
raw provider data
  01 ingest + schema standardization
  02 safe normalization
  03 country/city/state normalization
  04 geo normalization
  05 name tokenization
  06 automated frequency profiling
  07 automated token policy generation
  08 normalized parquet output    <-- stop here for now
  09 Splink ...                   (future)
```

First meaningful deliverable: `token_policy_generated.yaml`, `diagnostics.json`, and
sample rows where `property_name_core` looks good/bad.

---

## Environment

- Python **3.12** (system has 3.14 but wheel coverage is thin; we pin 3.12).
- venv at `.venv` (created with `uv venv --python 3.12`).
- Deps: polars, h3, phonenumbers, Unidecode, pyyaml, tldextract.
- Run scripts with `.venv/bin/python` from project root.
- Use **Polars LazyFrame** for scan/normalize; materialize once to compute token
  stats (needs a full pass), then finish in eager. Prefer native polars string
  expressions over `map_elements` for speed; reserve UDFs for ascii-fold, phone
  parsing, and h3.

---

## Raw data — ground truth (inspected 2026-06-09)

Location: `data/raw/{COUNTRY}/{provider}_property_info.ndjson` for COUNTRY in
`IN, US, NZ`. 13 providers. Downloaded via `download_property_info.py`.

**Good news:** the download already coerced every provider into the *same* base
column set, so schema standardization is mostly null-handling + a few provider
quirks rather than per-provider column maps.

IN row counts (total ≈ 835k):

| provider   | rows    | notes |
|------------|---------|-------|
| agoda      | 75,810  | `country_code='35'` (numeric); has `country_code_iso2='IN'` |
| cleartrip  | 182,511 | `property_code` often `'0'`; biggest file |
| expedia    | 43,153  | empty-string nulls; `hotel_chain='Independent'` |
| gogobal    | 0       | **empty file** — skip |
| grnc       | 29,477  | `country_code=None`; has `country_code_iso2='IN'`; geohash6 may be None |
| hotelbeds  | 7,813   | `property_type` is coded (`W`,`H`,`G`...); null-like `'NAS'`; has `hotel_id` |
| ioxl       | 2,024   | has `source_credential`, `hotel_chain='MARRIOTT'` |
| ratehawk   | 129,049 | empty-string nulls; `hotel_chain='No chain'`; `phone_numbers='0'` |
| restel     | 1,458   | **leaks non-IN rows** (e.g. Aberdeen UK); `property_type` coded (`1`,`8`); chain `'HA#KP#'` |
| rezlive    | 71,137  | has `source_credential` |
| tbo        | 68,799  | weburl often `booking.com`; has `source_credential` |
| tripjack   | 120,737 | has `hotel_id`; big amenities blob |
| veturis    | 32,718  | has `source_credential`; phone like `'0091 0 80...'` |

### Unified input columns (present on all providers)

```
property_code, property_name, lat, lng, address_lines, city_name, city_code,
state, country_code, country_name, postal_code, star_rating, property_type,
phone_numbers, emails, fax_numbers, hotel_chain, amenities, check_in_time,
check_out_time, number_of_rooms, average_of_rating, total_reviews, thumbnail,
weburl, land_mark, area, geohash6, updated_at
```

Provider-only extras (kept if present, otherwise null):
`country_code_iso2` (agoda, grnc), `hotel_id` (hotelbeds, tripjack),
`source_credential` (ioxl, rezlive, tbo, veturis).

### Observed quirks that drive normalization rules

- **country_code**: numeric (`'35'` agoda) or null (grnc) → must fall back to
  `country_code_iso2`, then `country_name`. Emit diagnostics, don't silently fix.
- **null-likes seen in the wild**: `''`, `'NAS'`, `'0'` (codes/phones), `None`,
  `'No chain'`, `'Independent'`, `'India'`/`'INDIA'`/`'india'` case variants.
- **phone_numbers**: free text, many formats: `'+919372868698, +919372868698'`,
  `'91-471-2212068'`, `'090990 58189'`, `'0091 0 8028544444'`, `'0'`,
  `'0333 777 3653--01224'`. Parse with `phonenumbers`, default region per country.
- **property_type**: human (`'Hotel'`,`'Resort'`) OR provider codes (`hotelbeds` W/H/G,
  `restel` 1/8). Need provider code maps.
- **weburl**: can be OTA/aggregator (`agoda.com`, `booking.com`, `tboholidays.com`)
  → weak/ignore as identity signal.
- **restel country leak**: country filter at source wasn't perfect; trust our own
  `country_code_norm`, flag mismatches.

---

## Project layout

```
hotelmap/
  pyproject.toml
  AGENT.md                  <- this file
  configs/
    india.yaml              run config (paths, thresholds, manual token overrides)
    providers.yaml          per-provider quirks (country maps, type maps, iso2 source)
    property_type_maps.yaml coarse property-type category maps
    country_maps.yaml       numeric/name -> ISO country code
  data/
    raw/{IN,US,NZ}/*.ndjson
    normalized/
    artifacts/runs/<run_id>/   one folder per run (observability)
  hotelmap/normalize/
    run.py          orchestrator
    config.py       config loading/merge
    schema.py       stage 1 ingest + standardize
    text.py         stage 2 safe text cleanup + null-like
    country.py      stage 3 country/city/state
    geo.py          stage 4 lat/lng/geohash/h3
    names.py        stage 5 name tokenization + policy split
    tokens.py       stage 6 token frequency stats
    token_policy.py stage 7 generated + effective policy
    phones.py       phone normalization
    emails.py       email normalization
    urls.py         url/domain normalization
    address.py      address v1 normalization + PIN extract
    property_type.py property type / star / rating
    diagnostics.py  quality rates + guardrails
    io.py           parquet/yaml helpers
```

### Per-run artifacts (every run writes all of these)

```
normalized_hotels.parquet
name_token_stats_global.parquet
name_token_stats_provider.parquet
name_token_stats_city.parquet
name_token_stats_h3_8.parquet
token_policy_generated.yaml
token_policy_effective.yaml
diagnostics.json
provider_quality.parquet
top_reused_phones.parquet
top_reused_emails.parquet
top_reused_domains.parquet
```

---

## Quality guardrails (computed every run; hard-fail thresholds)

```
country_code_norm missing      > 1%
property_name_norm missing     > 0.5%
property_name_core empty       > 20%   (means token policy too aggressive)
```

Also tracked (soft): empty_city_rate, invalid_coord_rate, low_precision_coord_rate,
postal_code_conflict_rate, phone/email/domain reuse rates,
records_with_only_weak_name_tokens.

---

## Decisions log

- **2026-06-09**: Fresh rebuild in Polars/Parquet (prior pipeline was DuckDB cloud;
  see memory). Raw is per-provider NDJSON, already column-aligned by the downloader,
  so stage-1 is concat + null-handling, not heavy column mapping.
- Pin Python 3.12 (3.14 wheels too thin for h3/phonenumbers at time of build).
- `record_id = provider + "::" + property_code + "::" + source_row_number`
  (don't trust property_code alone; cleartrip uses `'0'`).
- gogobal IN file is empty → skipped at ingest.
- **Brand auto-detection is a REVIEW QUEUE, not auto-applied.** The hotel_chain
  field is too noisy to trust: (a) `"No chain"`/`"Independent"` sentinels made
  `no`/`chain`/`independent` look like top brands — fixed by nulling sentinels
  before computing `chain_field_count`; (b) person names (`amit`, `abdul`, ...)
  recur in chain fields and got auto-flagged as brands, inflating the list to 1427
  and pushing `empty_name_core_rate` to 20% (guardrail breach). Decision:
  `token_policy_generated.yaml` lists candidates for human review; the **effective**
  brand set = curated `manual_tokens.brand_tokens` only. Promote a candidate by
  adding it to config. This is the spec's "don't blindly trust generated policy".
- Country leak (restel Aberdeen) resolves to IN (source claims IN) but is caught by
  `lat_lng_out_of_range` against the India bbox, not `country_off_target`. Both
  flags are kept; out_of_range is the one that fires here (1,799 rows).

---

## Progress

- [x] Stage 0: scaffold, venv, deps, configs, AGENT.md
- [x] Stage 1-2: ingest + safe normalization
- [x] Stage 3-4: country + geo
- [x] Stage 5: name tokenization
- [x] Stage 6-7: token stats + policy
- [x] Stage 5b: name policy split
- [x] Stage 8-10: phones/emails/urls/address/property_type
- [x] Diagnostics + orchestration + first IN run  ✅ **all stages 1-8 done**

### First IN run — `data/artifacts/runs/2026-06-09_002` (GUARDRAILS PASS)

Run with: `.venv/bin/python -m hotelmap.normalize.run --config configs/india.yaml`
~41s end-to-end for 764,686 records, 90,553 distinct global tokens.

Headline rates (all healthy):
- `empty_name_core_rate` = **3.17%** (was 20% before the brand fix)
- `name_norm_missing_rate` = 0.001%, `country_norm_failure_rate` = 0%
- `invalid_coord_rate` = 0.16%, `low_precision_coord_rate` = 1.07%,
  `out_of_range_coord_rate` = 0.24% (≈1,799 rows incl. restel Aberdeen leak)
- `postal_code_conflict_rate` = 5.5%
- `phone_reuse_rate` = 32%, `email_reuse_rate` = 14%, `domain_reuse_rate` = 15%
- effective policy: low_value=64, brand=23 (manual), location=13

Validated signals:
- `geohash6_norm` reproduces agoda's own `geohash6` (encoder correct).
- Top reused phone `9313931393` → 25,645 records / 9 providers = OYO customer-care
  line (matches prior-pipeline note about +919313931393). Reuse flag working.
- Top reused domains: agoda.com (86k), booking.com (28k) → correctly weak/reused.
- Empty cores are genuinely all-generic names ("hotel royal heritage", "hotel shree
  palace"); tokens preserved in `property_name_low_value_tokens`, recoverable.

### Tuning knobs for next iteration (when we look at matching quality)
- `configs/india.yaml tokens.low_value_global_doc_freq_ratio` (0.005) — lower =
  fewer low-value words = bigger cores; raise if cores too noisy.
- Promote real brands from `token_policy_generated.yaml:generated_brand_candidates`
  into `manual_tokens.brand_tokens`.
- `manual_tokens.location_descriptors` — review which weak-but-local words to keep.

### Not yet built (future)
- US / NZ runs (same code, different config; need a us.yaml / nz.yaml with bbox).
- City alias table (Bengaluru<->Bangalore) — deliberately deferred.

---

## Stage 09A — blocking / candidate diagnostics (DONE)

Package `hotelmap/splink_model/`: `blocking_rules.py`, `candidate_diagnostics.py`,
`run_blocking_diagnostics.py`. Backend = DuckDB (Splink will run on DuckDB too).

Run with:
```
.venv/bin/python -m hotelmap.splink_model.run_blocking_diagnostics \
    --normalized data/artifacts/runs/<run>/normalized_hotels.parquet
```
Artifacts land in `<run>/blocking_diagnostics/`: blocking_rule_counts,
provider_pair_counts, blocking_block_sizes, large_blocks, candidate_pair_samples
(*.parquet) + blocking_diagnostics.json.

**Design note — counts are analytic, not enumerated.** Each rule projects records to
(record_id, provider, block_key); cross-provider pairs in a block of size S with
per-provider counts c_i = (S^2 - sum c_i^2)/2. Never enumerates pairs, so a giant
block is *measured* cheaply, not exploded. The self-join is used only for bounded
human-readable samples (small/medium blocks, capped). All rules require
`l.provider < r.provider` (cross-provider linkage first; intra-provider dedupe later).

### First IN results (run 2026-06-09_002, 764,686 records)

| rule | pairs | covered | max block pairs | verdict |
|------|-------|---------|-----------------|---------|
| same_country_h3_8 | 60.4M | 741,021 (97%) | 4.73M | PASS (warn: big cells) |
| same_country_geohash6 | 52.7M | 739,679 | 2.97M | PASS (warn) — ~dup of h3_8 |
| same_country_city_postal | **247.9M** | 688,847 | **25.0M** | **FAIL** (>100M guardrail) |
| same_country_city_name_signature | 1.30M | 509,404 | 19.7k | PASS — clean/high-precision |
| shared_non_reused_phone | 89.8k | 99,423 | 6 | PASS — surgical |

No provider pair dominates any rule (max ~9–18%, well under the 50% guard).

**Findings / decisions:**
- **city_postal is unusable standalone in India.** Indian PIN codes blanket whole
  tourist towns: `IN|udaipur|313001` = 7,560 hotels across 11 providers = 25M pairs
  (members are all *different* hotels). It's just "same town". Drop it, or only use
  as city+postal+name-token. Its coverage (688k) is also below h3_8.
- **h3_8 and geohash6 are ~redundant** (both ~ several-hundred-metre cells, 97%
  coverage, heavy overlap). Keep ONE — prefer **h3_8** (clean neighbor traversal for
  later expansion). The 50-60M cost is concentrated in a few ultra-dense cells, e.g.
  H3 `883da1145dfffff` = 3,343 records = Paharganj (Delhi railway budget-hotel
  district) — genuinely many distinct hotels packed tight, not a bug.
- **city_name_signature** (1.3M) and **shared_non_reused_phone** (90k) are the
  precise, bad-coordinate-tolerant rules. Sample pairs verified as real same-hotel
  matches; phone even catches fuzzy-name truths name-signature misses
  (`srichackra international` ↔ `hotel srichakra international`).

**CONFIRMED blocking set for Splink v1** (user sign-off):
1. `same_country_h3_8`
2. `same_country_city_name_signature`
3. `same_country_city_postal_nametoken` (refined fallback — see below)
4. `shared_non_reused_phone`
Dropped: `geohash6` (redundant w/ h3_8), raw `city_postal` (too coarse).
Dense h3_8 cells: do NOT subdivide yet — score full h3_8, optimize only if Splink
runtime demands it (subdivision is a recall tradeoff; only acceptable as a measured
optimization, preferred order: h3_9 split > h3_9 neighbors > name-assisted).

**Refined fallback rule (replaces raw city_postal).** Gated on a new column
`property_name_blocking_tokens` (normalize stage): core tokens, length >= 4, minus a
global-overcommon stoplist (doc_freq_ratio >= 0.001, configurable) and minus any
token equal to a word in the record's own city name (kills self-referential
"Hotel Mahabaleshwar in Mahabaleshwar" blocks). Rule blocks on
country+city+postal+shared_blocking_token via UNNEST. Measured on run 005:
**2.44M pairs**, worst block 46,995, top token share 2.15% (<25% guard), 593,706
covered — passes the <30M / <500k-block / <25%-token guards. Diagnostic file
`blocking_token_contributions.parquet` lists per-token pair contribution.

Also added `phone_last10_non_reused_list` (normalize stage) so Splink blocks/compares
on phones with reused (shared-line) numbers already stripped — never makes the
matcher re-derive reuse.

Latest normalized base for Stage 09B: **`data/artifacts/runs/2026-06-09_005`**.

### Guardrail thresholds (overridable via config `blocking:` section)
block_pairs_warn=1e6, rule_pairs_fail=1e8, invalid_geo_share_warn=0.05,
provider_pair_dominance_warn=0.50.

---

## Stage 09B — Splink v1 scoring trial (pair-level, NO clustering)

Splink 4.0.16, DuckDB backend (shares our duckdb connection via
`DuckDBAPI(connection=con)`). Modules: `splink_settings.py` (model),
`run_splink_scoring.py` (orchestrator).

Run:
```
.venv/bin/python -m hotelmap.splink_model.run_splink_scoring \
    --normalized data/artifacts/runs/<run>/normalized_hotels.parquet
```
Artifacts in `<run>/splink_v1/`: splink_settings_v1.json (trained model),
splink_scored_pairs_v1.parquet (ids+provider+score+gamma, prob >= 0.01 floor),
splink_score_distribution.parquet, splink_pairs_band_*.parquet (review samples),
splink_runtime.json.

**Model (v1):** dedupe_only, unique_id=record_id. Comparisons:
- property_name_core — JaroWinkler [0.92, 0.85, 0.75]
- property_name_signature — ExactMatch + TF
- phone_last10_non_reused_list — ArrayIntersect size>=1
- distance — DistanceInKM bands [25m, 75m, 200m, 500m]
- city_name_norm — ExactMatch + TF
- postal_code_final — ExactMatch
- hotel_chain_norm, property_type_norm — ExactMatch (weak)

**Key modeling decisions:**
- **Distance is the only geo comparison; h3 is NOT a comparison.** h3-cell equality
  and distance are strongly correlated — feeding both double-counts location under
  Splink's conditional-independence assumption and overinflates weights. h3_8 stays a
  blocking key only.
- Empty-string identity fields are NULLIF'd in the prepped table so exact-match
  levels don't merge all blanks.
- predict() output kept slim (retain_matching/intermediate=False, additional=
  ["provider"]) — display fields for samples are joined back from prepped by id.
- u via random sampling (2M pairs); m via EM on block_on("h3_8") and
  block_on(city_name_norm, property_name_signature).

**BUG fixed during build — exploding blocking rules.** With `arrays_to_explode`,
Splink explodes the array to scalar elements but you MUST still write the element
equality (`l.col = r.col`) in the rule SQL. Omitting it made the phone/token rules
cross-join within country/city/postal: a 70k sample blew up to 80.8M pairs. With the
equality added, the same sample yields 531k pairs. Lesson baked into splink_settings.

### 09B first-run findings (run 2026-06-09_005/splink_v1) — MODEL OVERCONFIDENT

Performance is a non-issue: 62,195,572 pairs scored in **57s total** (u 2.9s, EM
16.8s, predict 37.3s). Blocking ~8s, predict ~29s internally. pairs_warn=False.

But **scores do NOT separate matches from non-matches.** Score distribution put
**23.2M pairs at >= 0.99** — implausibly many. Audit of that band (join back to
prepped):
- 96.8% came from the h3_8 blocking rule (match_key 0).
- median distance 245m, 97.5% within 500m — i.e. same dense cell, not same hotel.
- **84.9% have very different names** (signature differs AND core levenshtein > 5);
  only ~7.7% share a name. So ~19.6M of the 23.2M are location-only FALSE POSITIVES
  (same h3_8 + city + postal in a dense market, totally different hotel).
- Conversely, exact-signature true matches can score as low as **0.015**
  (`tiger tadoba` vs `tadoba tiger`) — see name problem below.

**Root cause = two sets of correlated features double/triple-counted** (violates
Splink conditional independence → overconfident):
1. **Location:** distance + city_name_norm + postal_code_final all agree together in
   a dense cell (and h3_8 already blocked them into it). Three correlated geo
   agreements multiply as if independent → push to 0.99 with no name evidence.
2. **Name:** property_name_core (JaroWinkler, ORDER-SENSITIVE) and
   property_name_signature (exact on SORTED tokens) are the *same tokens*. They
   conflict on token-order-swapped pairs and double-count otherwise. Order-sensitive
   JaroWinkler on unsorted core tanks `tiger tadoba`/`tadoba tiger` to 0.015 despite
   identical signature.

Phones behave well (84.5k/89k phone-sharing pairs >= 0.99) — strong, as expected.

**Verdict: the BLOCKING is fine; the SCORER needs redesign before any clustering.**
No single threshold is safe on this model (0.99 full of FPs; some true matches <0.1).

### Recommended model fixes for next iteration (09B-v2, before 09C/clustering)
1. Collapse correlated **location** signals to ONE geo comparison: keep distance
   (metres) as the geo evidence; DROP city + postal as separate exact comparisons
   (or fold postal into a single custom geo comparison / keep only as a weak
   tie-breaker). City is already implied by blocking.
2. Collapse correlated **name** signals to ONE order-insensitive name comparison:
   e.g. Jaccard/array-intersect on core token sets, or fuzzy levels on the SORTED
   signature — never JaroWinkler-on-unsorted-core alongside exact-signature.
3. Re-check that name disagreement is decisive: a pair with neither name nor phone
   agreement must not reach high prob on location alone.

---

## Stage 09B-v2 — de-correlated comparisons + identity gate

Same blocking set; comparisons redesigned (`build_settings_v2`, `--version v2`):
- **name:** ONE order-insensitive `CustomComparison` — Jaccard over
  property_name_core_tokens (levels: exact signature > jac0.85 > 0.65 > 0.45 >
  shared-token > else). Replaces v1's correlated JaroWinkler-on-unsorted + exact-sig.
- **geo:** distance only. city / postal / h3 dropped as comparisons (correlated with
  distance + already in blocking).
- **contact:** phone array-intersect, email array-intersect, website domain
  (reuse/OTA-aware custom level).
- **context (weak):** chain exact, type exact, star abs-diff <= 0.5.

**Identity gate (diagnostic, not yet applied to clustering):**
`passes_identity_gate = meaningful_name OR phone OR exact_email OR strong_domain`
where meaningful_name = exact signature OR Jaccard core >= 0.45. Enforces the hard
invariant "location/context alone must not create a match".

**Gotchas fixed:**
- EM training does NOT support exploding (arrays_to_explode) blocking rules
  (`ValueError: Exploding blocking rules are not supported`). EM phone-anchoring uses
  a scalar `primary_phone = list_extract(phone_non_reused, 1)` via block_on instead.
- EM rules must stay identity-anchored: `block_on(city, signature)` +
  `block_on(primary_phone)`. NEVER geo-only (h3) — EM would learn "nearby = match".
- Audit identity_gate must COALESCE every term to FALSE (phone/email/domain terms go
  NULL when a list/domain is missing; `NOT gate` then undercounts location-only).

### v2 results (run 2026-06-09_005/splink_v2) — much better, gate is MANDATORY

Same 62,195,572 pairs scored, ~113s incl. audit.

| metric | v1 | v2 |
|--------|----|----|
| pairs >= 0.99 | 23,155,922 | **1,639,916** |
| pairs >= 0.95 | ~26.8M | 1,984,344 |
| order-swapped same-sig pair score | 0.02 | **1.0** (fixed) |
| >= 0.95 with identity evidence | ~15% | **85.2%** |

v2 fixed both v1 root causes: order-insensitive name comparison scores token swaps
(`tiger tadoba`/`tadoba tiger`) at 1.0; ≥0.99 collapsed 14x to a plausible 1.64M.

**BUT the raw probability still violates the hard location-only invariant.** With
distance as the only geo signal, dense-India shared coordinates still push
location-only pairs high: **loc_only_max_score = 0.9997**, **2,215,135** location-only
pairs (no name, no phone, no email, no strong domain) score >= 0.50 and **381,268**
score >= 0.90. Distance <=25m is intrinsically weak in India (different hotels
geocode to the same point) yet carries strong weight.

**=> The identity gate is MANDATORY, not optional.** band x gate shows it
discriminates well and is the enforcement mechanism:
- >=0.99: gate true 1,509,859 (92%) | false 130,057
- 0.95-0.99: true 181,666 | false 162,762 (~47% location-only)
- 0.80-0.90: true 16,351 | false 397,411 (~96% location-only)

Production rule = `match_probability >= threshold AND passes_identity_gate`. This
satisfies the invariant by construction (location-only excluded). After the gate,
~1.51M identity-backed pairs sit at >=0.99 — a plausible high-confidence set for 764k
records.

Note: sig_exact_min_score ~0.02 is fine — those are generic signatures
("palace residency") shared by *different* far-apart hotels, correctly scored low;
the order-swap TRUE matches (same coords) score 1.0.

**Recommendation:** adopt v2 + mandatory identity gate. Optionally down-weight the
distance <=25m level (secondary; the gate already enforces safety). Next: 09C
threshold analysis on the GATED set, then clustering.

---

## Stage 09C — gated threshold analysis (DONE)

Module `run_threshold_analysis.py`. Applies the identity gate WITH a reason label
(priority: phone > exact_email > non_reused_domain > signature > strong_name(jac>=0.85)
> medium_name_plus_geo(jac in [0.45,0.85) AND dist<=200m) > none). Medium name alone
does NOT pass — name-only needs geo. Profiles GATED pairs by fine band; persists
`gated_pairs.parquet` (edges + reason) for 09D.

Run:
```
.venv/bin/python -m hotelmap.splink_model.run_threshold_analysis \
  --normalized <run>/normalized_hotels.parquet \
  --scored <run>/splink_v2/splink_scored_pairs_v2.parquet
```

**Results (run 2026-06-09_005):** of pairs >= 0.80, the gate rejects 981,547
(location-only / weak-name-no-geo). Gated breakdown by reason × tier:

| reason | total | >= 0.99 | note |
|--------|-------|---------|------|
| signature | 1,019,678 | 1,019,564 | exact sorted-core name; ~99% at >=0.995 |
| medium_name_plus_geo | 416,585 | 341,156 | name 0.45-0.85 + <=200m |
| phone | 82,290 | 76,876 | strong, independent |
| exact_email | 48,236 | 41,548 | |
| non_reused_domain | 330 | 287 | |
| strong_name | 191 | 0 | jac>=0.85, no exact sig (rare) |

`ge_0.995` band: median distance **9m**, p95 261m — tight, signature-dominated,
obvious true matches (`trimurti heights hotel` x2 @0m). `0.80-0.90` band p95 8.3km =
far-apart email-backed pairs (shared reservation email across a chain) — review.

**Key finding — name Jaccard is computed on core_tokens, which still contain
city/location tokens.** So `medium_name_plus_geo` has real FPs from a *shared location
token* + close distance: `cochin homes` / `holiday inn cochin`,
`goroomgo krishna residency` / `goroomgo chandan resort`. FP rate falls as score rises
(>=0.995 medium pairs are mostly true) but is non-zero. => medium_name is the weakest
gated evidence.

**Reason-aware threshold policy (recommended):**
- **Auto-match:** reason in {signature, phone, exact_email, non_reused_domain} AND
  prob >= 0.99  (~1.14M pairs — independent strong identity).
- **Review:** all `medium_name_plus_geo` (any score) + `strong_name` + anything in
  0.90-0.99  (~0.42M+).
- **Reject:** gate fails, or prob < 0.90.

**Model-refinement opportunity (09B-v3 / future):** compute the name Jaccard on
`property_name_blocking_tokens` (city/location-stripped) instead of core_tokens — would
cut medium_name FPs and shrink the review queue. Defer; reason-aware policy is safe now.

---

## Stage 09D — safe clustering on auto-match edges (DONE)

Module `run_clustering.py`. Union-find over auto-match edges
(reason in {signature, phone, exact_email, non_reused_domain} AND prob >= 0.99) →
connected components → guardrails → auto_accept vs review. Singletons preserved.

Run:
```
.venv/bin/python -m hotelmap.splink_model.run_clustering \
  --normalized <run>/normalized_hotels.parquet \
  --gated <run>/splink_v2/threshold/gated_pairs.parquet
```
Outputs in `<run>/splink_v2/clusters/`: clusters_auto_raw, clusters_auto_accept,
clusters_review, cluster_members (incl singletons, status matched/review/singleton),
cluster_edges, cluster_diagnostics, cluster_review_samples, clustering_summary.json.

Auto-accept iff: no same-provider duplicate AND size <= 20 AND (sig_share >= 0.6 OR
contact) AND (diameter <= 500m OR contact). Review reasons: same_provider_duplicate,
oversize(>20), geo_diameter_gt_500_no_contact, transitive_drift_geo_gt_1km,
name_conflict. cluster_id = 'IN_'||md5(sorted record_ids)[:16]; representative by
provider preference (NOT survivorship — deferred).

### Results (run 2026-06-09_005)
- auto-match edges: 1,138,275 → 141,686 clusters over 573,963 records; 190,723 singletons.
- **auto_accept: 112,071 clusters / 359,718 records**; review: 29,615 / 214,245; largest 184.
- review reasons: same_provider_duplicate 29,340; geo_diameter_gt_500_no_contact 627;
  oversize 225; transitive_drift_geo_gt_1km 20; name_conflict 0.

### Validation
- **Auto-accept is high precision.** Samples are clean cross-provider same-hotels
  (`lotus feet inn` x3, `the continentti whitefield` x5 incl Bangalore/Bengaluru,
  `iroomz hotel sbr` x6). avg size 3.2, all <= 11, avg sig_share 0.98.
- The 2,827 auto-accept clusters with sig_share < 0.6 are NOT false merges — they're
  true matches where one feed appended a city/area token (`shahenshah hotel` /
  `shahenshah hotel prayagraj`). sig_share (exact-signature equality) UNDERSTATES
  coherence; a Jaccard-based coherence metric (the deferred blocking_tokens refinement)
  would stop over-flagging these.
- **Guardrails caught the real failure mode — contact over-merge.** The 184-member
  cluster is dozens of *different* Calangute hotels chained via shared
  management/reservation phone/email (has_contact_evidence=true, sig_share 0.55,
  diameter 3.2km). Non-reused phone/email still over-merges different properties that
  share a back-office contact (reuse threshold 5 misses contacts shared by 2-4 props).
  Oversize/diameter guardrails routed all of these to review. ✅
- same_provider_duplicate review = mix of benign intra-provider dupes
  (`fabexpress sreenivasa home stay` listed twice by cleartrip) and genuine unit
  ambiguity (`la vera boutique hotel` vs `la vera villa`).

### Residual risks / next refinements
- Small (<20) contact-only clusters of different properties can still auto-accept
  (rule allows diameter > 500m if contact). Consider: contact-only edges should
  require minimal name agreement, or tighten the phone/email reuse threshold.
- Coherence metric should use core/blocking-token Jaccard, not exact-signature share.

### Then (future): blocking_tokens name-Jaccard refinement → 09E clerical review queue
→ survivorship / canonical record. Do NOT auto-cluster medium_name_plus_geo.

---

## Stage 09D-v2.1 — match-token Jaccard refinement (DONE)

Implemented after the initial 09D clustering, before 09E. New feature:
`property_name_match_tokens` / `property_name_match_signature`, stricter than core
tokens. It removes city/area/state/country context, location descriptors,
brand/franchise tokens, obvious accommodation words, global/city-overcommon tokens,
numeric-only tokens, and tokens shorter than 4 chars. Important detail: area text is
noisy and can contain the property name, so rare identity-like tokens are preserved
even when present in `area_clean` (e.g. `shahenshah` in Cleartrip area text).

v2.1 writes new artifacts under `data/artifacts/runs/2026-06-09_005/splink_v2_1/`:
`splink_scored_pairs_v2_1.parquet`, `threshold/gated_pairs_v2_1.parquet`,
`clusters/cluster_diagnostics_v2_1.parquet`, `clusters/clustering_summary_v2_1.json`,
plus `v2_vs_v2_1_threshold_delta.parquet` and `v2_vs_v2_1_cluster_delta.parquet`.
v2 artifacts were not overwritten.

**Critical fix during v2.1:** exact "signature" must mean exact
`property_name_match_signature`, not old `property_name_signature`. Old core
signature could collapse location-only names to the same token, e.g. `cochin homes`
and `holiday inn cochin` both became core signature `cochin`. After the correction,
those pairs disappear from v2.1 gated output.

Token sanity examples:
- `cochin homes` → `homes`
- `holiday inn cochin by ihg` → `holiday`
- `shahenshah hotel prayagraj` → `shahenshah`
- `red lollipop hostel chennai` → `lollipop`

Final v2.1 scoring (same 62,195,572 candidate pairs):
- pairs >=0.99: **1,411,467** (v2: 1,639,916 raw scored)
- location-only >=0.90: **96,658** (v2: 381,268)
- pct >=0.95 with identity evidence: **95.4%** (v2: 85.2%)
- scoring runtime: 210s total; prep is now the slow part (~76s) because match tokens
  are backfilled from the existing normalized parquet.

Threshold delta (v2 → v2.1):
- `medium_name_plus_geo`: **416,585 → 248,734** (-167,851)
- gated >=0.99: **1,479,431 → 1,405,525** (-73,906)
- gated >=0.80: **1,567,310 → 1,515,924** (-51,386)
- gated review 0.90-0.99: **84,971 → 96,538** (+11,567)

Clustering delta (v2 → v2.1):
- auto-match edges: **1,138,275 → 1,229,547** (+91,272). This rose because exact
  match-token signatures recover true same-name pairs with appended city/area tokens
  (`shahenshah hotel` / `shahenshah hotel prayagraj`, etc.).
- auto-accepted clusters: **112,071 → 91,022** (-21,049)
- review clusters: **29,615 → 35,203** (+5,588)
- contact-only auto clusters: **9,395 → 7,414** (-1,981)
- largest cluster: **184 → 78**

New cluster coherence metrics are written:
`min_pairwise_name_match_jaccard`, `median_pairwise_name_match_jaccard`,
`dominant_match_token_signature_share`. v2.1 review rules add:
- no strong contact evidence AND `min_pairwise_name_match_jaccard < 0.45`
- contact-backed cluster with size >2 AND `min_pairwise_name_match_jaccard < 0.30`
- contact-backed auto edges require `name_match_jaccard >= 0.30` OR exact
  match-token signature; distance is not sufficient.

Validation:
- Fixed false positives: `cochin homes` vs `holiday inn cochin*` old
  `medium_name_plus_geo` and old false `signature` pairs are absent in v2.1.
- Contact-backed auto edges now have minimum Jaccard >=0.30 by construction; medians
  are 1.0 for phone/email/domain edge reasons.
- Oversize contact clusters still exist (257, max size 78), but are review-routed.
- Potential recall cost: some exact-email pairs that look true moved from auto to
  review (e.g. `gostops palampur rooms and dorms` / `gostops palampur`,
  `ginger pune wakad` / `ginger pune wakad`, `vivanta ernakulam marine drive` /
  `vivanta ernakulam marine drive`). They are not lost from gated output, but no
  longer auto-cluster at the >=0.99 threshold.

Do NOT promote `medium_name_plus_geo` to auto-match yet. Next: 09E clerical review
queue using v2.1 gated/review artifacts.

---

## Stage 09E — clerical review queue (DONE)

Module: `hotelmap/splink_model/run_review_queue.py`.

Run:
```
.venv/bin/python -m hotelmap.splink_model.run_review_queue \
  --normalized data/artifacts/runs/2026-06-09_005/normalized_hotels.parquet
```

Outputs under `data/artifacts/runs/2026-06-09_005/splink_v2_1/review_queue/`:
- `review_queue_clusters.parquet`
- `review_queue_edges.parquet`
- `review_queue_members.parquet`
- `review_queue_samples.parquet`
- `review_queue_summary.json`
- `review_queue_top.csv`
- `review_decisions_template.parquet`

The queue combines v2.1 review clusters plus pair-level gated review items that did
not become auto-cluster edges. Buckets are action-oriented:
1. `high_confidence_likely_true`
2. `contact_overmerge_risk`
3. `transitive_drift`
4. `same_provider_duplicates`
5. `medium_name_plus_geo`

Final queue counts:
- cluster review items: **35,203**
- pair review items: **269,369**
- total review items: **304,572**
- special exact-email/contact true-like review pairs:
  **3,892** tagged `contact_or_email_true_like_but_failed_coherence`

Bucket counts:
- `high_confidence_likely_true` / pair: 19,464
- `medium_name_plus_geo` / pair: 249,905
- `contact_overmerge_risk` / cluster: 3,783
- `high_confidence_likely_true` / cluster: 29,361
- `same_provider_duplicates` / cluster: 2,029
- `transitive_drift` / cluster: 30

Important queue tuning decisions:
- Risk-first precedence for clusters: large/wide shared-contact components stay in
  `contact_overmerge_risk` even if they contain some convincing edges.
- Exact-email close-distance pairs below auto threshold get
  `review_reason=contact_or_email_true_like_but_failed_coherence`,
  `review_bucket=high_confidence_likely_true`, high priority, and
  `suggested_action=accept_pair_only`. This surfaces examples such as
  `gostops palampur rooms and dorms / gostops palampur`,
  `ginger pune wakad / ginger pune wakad`, and
  `vivanta ernakulam marine drive / vivanta ernakulam marine drive` near the top
  of `review_queue_top.csv` without auto-promoting them.
- `medium_name_plus_geo` remains review-only; suggested action is `accept_pair_only`
  so reviewer decisions become pair-level training/evaluation data.

Decision log template fields:
`review_id, entity_a, entity_b, decision, reviewer, reviewed_at, reason, notes`.
Decision values to use:
`same_hotel`, `different_hotel`, `same_property_group_not_same_hotel`, `unclear`.

Next: **09F survivorship / canonical hotel record**. Do not start survivorship logic
until review workflow expectations are stable enough to feed evaluation data back
into matching decisions.

---

## Stage 09D-v2.2 — benign same-provider-dup promotion + hard geo cap (DONE)

Motivation (review-cluster audit, 2026-06-10): v2.1 review queue was dominated by
noise. 33,253 of 35,203 review clusters had `same_provider_duplicate` as the ONLY
reason, and sampling showed the overwhelming majority were TRUE cross-provider
matches where one feed simply lists the hotel twice (tripjack x2 `munnar tea hills
resort`, cleartrip x2 `hotel the park gate` — same coords, same name). Measured:
26,102 of those clusters (172k records) had EVERY same-provider pair name-coherent
(exact match signature or match-token jaccard >= 0.6) AND within 300m. Routing them
to humans was pure waste; a benign double-listing is a survivorship problem, not a
match risk.

The audit also found a real hole in the other direction: contact evidence waived the
geo-diameter guardrail UNBOUNDED, so different same-name hotels in different towns
chained by a shared back-office contact could auto-accept: `hotel aatithya`
(Guwahati + Pathsala, 74km, auto-accepted in v2.1!) and `elphinstone` (Nainital
hotel + Almora resort, 30km, in review only by luck of an SPD flag).

**v2.2 clustering rule changes** (`run_clustering.py --version v2_2`; same v2.1
match-token frame and `gated_pairs_v2_1.parquet` input — scoring untouched):
1. New per-cluster same-provider pair diagnostics (`same_provider_pair_count`,
   `max_same_provider_pair_dist_m`, `min_same_provider_pair_jaccard`,
   `benign_same_provider_dup`). The `same_provider_duplicate` review reason now
   fires only when some intra-provider pair is NOT benign
   (benign = (sig_eq OR jaccard >= 0.6) AND dist <= 300m; NULL coords = not benign).
2. New review reason `geo_diameter_gt_10km`: fires regardless of contact evidence
   (constant `HARD_DIAMETER_M = 10km`). Catches aatithya/elphinstone-class merges.
   2-10km contact-backed clusters stay auto (samples = same hotel with divergent
   geocodes: `hotel rallentino` Kota 6km, `sarla resort` 2.3km).

Artifacts: same `splink_v2_1/clusters/` dir with `_v2_2` suffix +
`v2_1_vs_v2_2_cluster_delta.parquet`; review queue via
`run_review_queue.py --version v2_2` -> `splink_v2_1/review_queue_v2_2/`.
Dashboard understands `--version v2_2` (maps to the v2_1 tree) and defaults to it.

### Results (run 2026-06-09_005, v2.1 -> v2.2)
- auto-accepted clusters: **91,022 -> 116,785**; records in auto: **297,398 -> 468,128**
- review clusters: **35,203 -> 9,440**; records in review: **255,761 -> 85,031**
- 26,045 benign-dup clusters promoted to auto-accept; 957 clusters now carry
  `geo_diameter_gt_10km` (review), incl. both known bad merges.
- Review queue is now action-oriented: 3,280 SPD-coherent likely-true, 3,228
  contact-overmerge-risk, 1,504 genuinely ambiguous SPD, 758 needs-more-info,
  640 strong-name contact, 30 drift. Top item = `svenska design hotel`
  Kakinada+Mumbai phone-chained across 1,024km (correct #1 for a human).
- Residual conservative tail: borderline true dups stay in review when the dup pair
  misses thresholds (`munnar tea hills resort` vs `... mthr` jac 0.5; ratehawk dup
  330m apart). Deliberate: lowering jac to 0.5 would mis-promote
  `la vera boutique hotel` / `la vera villa` style unit ambiguity.

---

## Multi-country: NZ + US runs and country-specific normalization (DONE 2026-06-10)

NZ is the small western canary (57,864 records, full pipeline in seconds); US is
the big run (1,335,094 records). New country plumbing (all config-driven, India
defaults preserved):

- **Postal module** (`address.py` + `postal:` config): per-country `field_pattern`
  / `address_pattern` / `validation_pattern` (the last one used by 09A blocking
  eligibility via a `{postal_regex}` placeholder in `blocking_rules.py`).
  US = ZIP5 (+4 normalized to ZIP5), NZ = 4-digit. ADDRESS-text extraction is
  END-ANCHORED for US/NZ because 4/5-digit runs collide with street numbers.
- **State maps** (`configs/state_maps.yaml`, applied in `country.py`): US full
  names -> USPS codes (lowercase); NZ region variants -> canonical region.
- `address_abbreviations:` per-country extension (US ave/blvd/ste/pkwy...).
- `country_maps.yaml`: agoda numeric codes 25=NZ, 181=US.
- Run ids country-tagged: `2026-06-10_NZ_001`. Cluster ids prefixed by config
  country (was hardcoded `IN_`).
- **Token policy decisions for the West:** `force_keep: [inn, suites, motel,
  lodge]` (franchise variants "X Inn" vs "X Suites" are different co-located
  properties; auto low-value would collapse their cores). `location_descriptors`
  kept MINIMAL (near/opposite/opp/off) because area words (downtown, airport,
  central) ARE the identity of US/NZ chain properties. Brand lists modest and
  curated; generic-English brand words (best, western, days, quality, comfort,
  holiday) deliberately NOT brand tokens. Chain portal domains (marriott.com,
  hilton.com, ihg.com...) pre-listed in weak_domains.
- Data quality findings: US phone_reuse = **60.6%** (franchise 1-800 lines; only
  7.7% of records keep a non-reused phone -> phones are weak identity in US;
  postal 92.7% + names + geo carry it). NZ phone_reuse 40%, postal 87.8%.
  cleartrip NZ leaks UK rows (AB54 7XE) — caught by bbox flags like restel IN.
  Raw `city_postal` blocking fails in the US too (Kissimmee 34747 = 22k records,
  47M-pair block) — production blocking set unchanged (h3_8, city+signature,
  city+postal+nametoken, non-reused phone).

### Results
| | NZ | US |
|---|---|---|
| records | 57,864 | 1,335,094 |
| auto-matched records (v2_3) | 38,617 (66.7%) | 674,507 (50.5%) |
| review clusters | 174 | 6,628 |
| singletons | 17,595 | 570,643 |
| pairs scored | 5.4M | 88M (~6 min) |
| pct >=0.95 with identity | 99.7% | (gate rejected 3.1M loc-only >=0.80) |

---

## Stage 09D-v2.3 — apartment buildings, villa units, brand conflicts (DONE)

Business rule (user, 2026-06-10): apart-hotel/condo unit listings (often
mislabeled as hotels) that are in the SAME building belong in the SAME mapping —
the entity is the building. Different buildings stay separate. Resort-style
villas must NOT be merged: near-identical names differing only in unit numbers
are different villas.

`run_clustering.py --version v2_3` (overlay on v2_2 rules; same gated input):
- Record flags: `is_apartment_style` (apartment|apt|studio|bedroom|condo|
  penthouse|loft|residences (PLURAL ONLY — singular matches the Residence Inn
  brand) or property_type apartment/aparthotel), `is_villa_style` (villas?),
  `unit_num` (keyword+number; for villas also a trailing number).
- **building_merge** (-> `entity_level='building'`, waives same_provider_duplicate
  /oversize/name-sanity reasons): apartment_style_share >= 0.5, villa_share < 0.3,
  diameter <= 250m, size <= 200, top_match_token_member_share >= 0.8 (every
  member carries the building's modal name token), AND no brand conflict.
- **villa_unit_conflict** (forced review, never auto): villa_share >= 0.3 AND
  >= 2 distinct unit numbers. Catches `club villas unit 2b/8c/24b`,
  `springwood villa 22/53/74` (Hilton Head).
- **brand_conflict** (forced review; also blocks building merge): two members
  with non-empty DISJOINT brand-token sets. Catches the US highway-strip pattern
  where match signatures collapse to a shared suburb token: `springhill suites
  wrentham` + `towneplace suites wrentham` (two different Marriott flags on one
  lot) were AUTO-merged before this guard — 788 US clusters / 5,848 records were
  pure silent false merges (review_reasons = [brand_conflict] only). Dual-brand
  names ("home2 suites by hilton") keep overlapping sets and are not flagged.
  **Brand groups** (config `brand_groups:`): sub-brand tokens that are naming
  variants of the SAME property collapse to one family before the disjoint test.
  India needs this for the OYO family (oyo/capital/o/collection/townhouse/spot —
  `capital o 81526 X` and `X by oyo` are the same hotel; flagging them dropped
  ~3.5k true clusters to review) + fab/lemon families. Do NOT group families
  whose sub-brands are different properties (Marriott Courtyard vs Fairfield).
  Genuine rebrand ambiguity (treebo X ↔ oyo X) still conflicts -> review.
  Known miss: two different flags both suffixed "by hilton" keep an overlap and
  don't conflict — future refinement.

Validation highlights:
- Polo Beach Club (Maui): 21 condo-unit listings -> ONE building cluster. ✅
- Auckland Metropolis tower: 35 listings, was oversize-review in v2_2 -> now
  auto building cluster. ✅
- Rapid City Home2+Residence Inn+Courtyard false "building" caught by brand
  guard (was the worst v2_3-draft regression; fixed by dropping singular
  'residence' from the apartment regex + the brand-conflict rule). ✅
- US counts: building_merged_auto 31,235; villa_unit_conflict 109;
  brand_conflict 1,618. NZ: 1,405 / 4 / 1.
- **India rerun as v2_3 (2026-06-10, run 005):** auto 115,168 clusters /
  453,409 records (v2_2: 116,785 / 468,128 — the delta is brand-conflict and
  villa clusters moved to review); review 11,057 / 99,750; building_merged
  2,897; villa_unit_conflict 8; brand_conflict 2,471 (post-brand-groups; was
  4,706 before OYO-family grouping). Review queue v2_3: 280,426 items.
  India is now on the same v2_3 baseline as NZ/US.

Review queue (`run_review_queue.py --version v2_3`) adds reasons
`villa_unit_conflict` / `brand_conflict` (bucket contact_overmerge_risk,
action split_cluster, +30 priority). Dashboard resolves v2_3 (maps to the
splink_v2_1 tree) and prefers it by default.

---

## Stage 09D-v2.4 — name-subgroup auto-SPLIT (DONE 2026-06-10)

Motivation (centroid-distance audit of IN auto clusters, 2026-06-10): contact
edges (shared back-office phone/email) chained DIFFERENT properties of small
chains into one auto cluster. The jaccard >= 0.30 contact floor passes via
shared chain/rare tokens while the distinctive disagreeing tokens carry no
weight: `kyzen` + `kyzen hitech your zenly` (two Hyderabad hotels 745m apart,
one phone edge at jac 0.333), `club mahindra binsar` inside the `danish ooty`
cluster (exact_email + wrong provider coords, 14m), `summit`/`summit gangtok`,
`pamposh`/`pamposh gurgaon` (4.2km), whoopers hostels Anjuna/Manali/Palolem.

Key design decision: **auto-split instead of review**. A false split is a
missed mapping (recoverable in 09F/09G survivorship); a false merge is a wrong
mapping (forbidden). So conflicting subgroups are SPLIT automatically and each
coherent side stays auto-accepted — review is only for cases where neither
keeping nor splitting is defensible.

`run_clustering.py --version v2_4` (overlay on v2_3; same gated input): after
the first union-find, components are decomposed into **core-token signature
subgroups** and direct contact edges between conflicting subgroups are cut,
then union-find reruns on surviving edges. Core tokens (NOT match tokens) are
the subgroup key: match tokens strip city/area words (gurgaon, nubra, ooty),
which erases exactly the suffix distinguishing branch properties. Signature-
gated edges connect identical core signatures by construction, so cuts only
ever remove contact edges. Artifact: `v2_4_split_log.parquet`.

Conflict rules (constants at top of run_clustering.py):
1. **geo_split**: both subgroups have >=2 located records, each tight
   (spread < 150m), separated > 350m AND > 3x max spread. Single-record
   subgroups NEVER geo-split — a lone far record is usually a bad geocode
   (hotelbeds is systematically 1-6km off), not a second place. (First draft
   without the >=2 rule cut 22,912 edges and pushed +13k records to
   singletons; with it, cuts drop to 2,723 and singletons to +272.)
2. **numeric_id_conflict**: both sides carry rare numeric ID tokens (OYO
   property numbers) AND are separated (>350m, >2x spread). Co-located ID
   conflicts (768 of 852 in IN) are rebrand/relistings of the SAME hotel and
   stay merged.
3. **rare_token_conflict**: both sides carry live rare alpha tokens
   (doc_freq <= 300, len >= 4) the other lacks — fires regardless of geo
   because the conflicting record often has wrong coords (binsar@Ooty).
   Threshold 300 chosen because real places are not ultra-rare: binsar=139,
   nubra=290; needs BOTH sides, so common-word variants can't fire.
   IMPORTANT: unit codes like `100b`/`216c`/`5br` (regex `\d+[a-z]{0,2}`)
   count as NUMERIC, not alpha — the US audit showed the geo-free alpha rule
   tearing same-building `seaspray condos` units apart at 0m separation,
   exactly what the v2_3 building policy wants merged. ID-shaped tokens must
   always use the geo-gated numeric rule. (Same audit: apartment-style regex
   was missing plural `condos` — fixed, US building merges +374.)

Waivers (never conflict): misspelling twins (levenshtein <= 2: otty/ooty,
cupids/cupid, theekana/theekaana) and containment twins (seacoin/coin,
sproutsbodhivann/bodhivann, truncated numeric ids). A pair whose every
disagreement is twin-waived is also exempt from geo_split (fern
ranthambhore-class name noise with divergent geocodes stays merged).

### Results (run 2026-06-09_005, v2_3 -> v2_4)
- 1,041 conflict components, 2,723 edges cut (738 geo_split pairs, 176
  numeric_id, 870 rare_token).
- auto-accepted: **115,168 -> 115,840** (+672 clusters); records in auto
  **453,409 -> 456,260** (+2,851) — splitting poisoned review clusters
  (>10km diameter chains etc.) yields clean parts that auto-accept.
- review: **11,057 -> 10,756** (-301); records in review -3,123.
- singletons: +272 only. building_merged_auto 2,897 -> 2,920.
- Review queue v2_4: 280,125 items (`review_queue_v2_4/`).

Validation: kyzen -> two auto clusters (6+4) ✅; binsar record out of the ooty
cluster (singleton; two real binsar clusters intact) ✅; `danish otty`
misspelled hotelbeds record REJOINED the ooty cluster (recall recovered by the
core-token fuzzy waiver) ✅; pamposh gurgaon own 4-record auto cluster ✅;
shangrila nubra (7) split from tih shangrila ladakh (9) ✅; whoopers/lindsay/
sonar-bangla multi-city chains split per city ✅; co-located OYO rebrands stay
merged ✅.

### NZ + US reruns (2026-06-10, same day)
All three countries now share the v2_4 baseline; review queues rebuilt
(`review_queue_v2_4/`: IN 280,125 / NZ 27,460 / US 885,506 items).
- **NZ**: 76 components / 127 edges cut; auto 8,056 -> 8,091 clusters
  (records 38,617 -> 38,775); review 174 -> 157; singletons +36.
- **US**: 1,053 components / 10,080 edges cut (2,863 geo_split, 9,644
  numeric_id — vacation-rental unit listings: SummitCove/Windsor/Bella Vida
  units >350m apart are genuinely different properties — 1,198 rare_token);
  auto 151,761 -> 152,352 clusters (records 674,507 -> 677,580); review
  6,628 -> 6,379 (records -3,246); singletons +173; building_merged_auto
  31,235 -> 31,609.

Known residual tail: single-record-vs-single-record rare_token cuts at ~0m can
split true matches with renamed listings (`jalore mansarovar` /
`mansarovar rajasthan`) — recall-only cost, bounded. Future refinement =
token geo-affinity ("local uniqueness"): use `name_token_stats_h3_8.parquet`
(computed in stage 06 but unused by scoring) to weight a disagreeing token by
the distance between the record and the token's geographic mass — separates
binsar-class alien tokens from benign renamed listings, and would let scoring
discount a rare token shared by multiple distinct nearby properties (kyzen).

`run_review_queue.py` and the dashboard accept/prefer v2_4.

---

## Single-entry pipeline + dashboard runner + name search (DONE 2026-06-10)

**`hotelmap/pipeline.py`** — one command runs everything for a country:
```
.venv/bin/python -m hotelmap.pipeline --country NZ [--skip-download]
```
Chains download (resumable; skips non-empty raw files) → normalize → splink
scoring v2_1 → threshold v2_1 → clustering v2_4 → review queue v2_4. Stage
versions are constants at the top (`SCORING_VERSION` / `CLUSTER_VERSION`) —
bump there when baselines move. Country -> config resolved by scanning
`configs/*.yaml` for a `country:` key, so a new country = one new yaml.
Emits `[pipeline] stage <name>` lines and a final `[pipeline] DONE run=<dir>`
(the dashboard parses these). Download logic moved to **`hotelmap/download.py`**
(`download_country(cc)`); root `download_property_info.py` is now a thin
wrapper. NDJSON download needs `EMBEDDING_GATEWAY_API_KEY` in env.
Validated end-to-end on NZ (raw -> review queue ~20s, results match NZ_001).

**Dashboard pipeline view** (`/api/pipeline/configs|start|status` + sidebar
"Pipeline"): country dropdown (flags missing raw data), fresh-download vs
use-existing-raw selector (download option disabled when the API key is absent
from the server env), run button, live stage + log tail (2.5s poll), and an
"Open run →" button on success that switches the dashboard to the new run.
One job at a time (subprocess of `python -m hotelmap.pipeline`; logs under
`data/artifacts/pipeline_logs/<ts>_<cc>.log`, gitignored dir).

**New-country support (2026-06-10):** `--country XX` for ANY ISO2 code — if no
config has that `country:`, `ensure_country_config()` writes a generic
`configs/xx.yaml` from `GENERIC_CONFIG_TEMPLATE` (pipeline.py). The dashboard
country field is free-text with a datalist of known configs and shows an
"auto-generated config" hint for unknown codes. The generated config's header
comments list what's degraded until hand-tuned:
- no geo bbox -> wrong-country leak flags disabled (restel/cleartrip leak rows);
- generic postal pattern (field-only, no address-text extraction) -> the
  city+postal blocking rule is weak;
- empty brand_tokens/brand_groups -> brand-conflict guard inert (springhill/
  towneplace-class chain false merges possible) — promote candidates from
  token_policy_generated.yaml after run 1;
- no state_maps entry; no agoda/grnc numeric code in country_maps.yaml
  (iso2 fallback covers agoda/grnc);
- non-Latin scripts: ascii-fold + len-4 token thresholds are Latin-tuned;
- providers may not use ISO2 in country_code for every country (e.g. 'UK' vs
  'GB') -> download WHERE clause can miss rows; check counts after download.
Validated: AU config generated, loads via load_config, postal regexes compile
in polars + duckdb; clean error when --skip-download with no raw files.

**First real new-country run (NP, 2026-06-10) found 3 latent bugs — all fixed:**
1. `country_maps.yaml iso2_passthrough` was a hardcoded IN/US/NZ whitelist →
   Nepal's perfectly valid `NP` codes resolved to NULL for 100% of records →
   every blocking rule (all key on `country_code_norm`) produced ZERO pairs.
   Fix: `country.py` now passes through any ISO2-shaped code (`^[A-Z]{2}$`);
   garbage ('0', 'NAS', numerics) doesn't match the shape and still falls to
   the numeric/name maps. The whitelist key is gone.
2. Normalization guardrails reported FAIL (`country_norm_failure_rate=1.0`)
   but nothing STOPPED — scoring ran on garbage and "succeeded" with 0 pairs.
   Fix: `pipeline.py` reads diagnostics.json after normalize and aborts with
   the failing guardrails when `guardrails_passed` is false (artifacts kept).
3. Zero auto-match edges crashed v2_4 clustering: empty python list ->
   `pl.Series` infers dtype Null -> `filter()` raises. Fix: explicit Boolean
   dtype + explicit Utf8 schema on the union-find frames.
NP results (23,892 records): 7.76M pairs scored, 3,798 auto clusters /
16,656 records (69.7%), 53 review, 6,763 singletons, 200 building merges,
0 off-target leaks. Generic-config caveat visible in practice: weak name cores
("thamel", "arts", 5% empty) — locality words carry identity in Kathmandu;
tune force_keep/location_descriptors before trusting NP deeply.

**Small-country recall audit (SI, 2026-06-10) — two match-token bugs fixed:**
User observed many obviously-identical singletons in SI. Measured: 439 groups
(1,509 records) of IDENTICAL full name + same city + <500m, cross-provider,
ALL singletons ("ramada resort by wyndham kranjska gora" x13 providers).
Root causes (both in `names.py` match-token construction; failures CASCADE:
empty match tokens -> NULL name comparison -> EM never observes strong-name
levels (the "m probability not trained" warnings) -> identical-name pairs
score < 0.01 persist floor -> invisible):
1. `build_city_overcommon_token_map` counted RECORDS: one 13-provider hotel in
   a small town pushed its own brand tokens ('ramada','wyndham') over the 2%
   city ratio -> stripped as "local geography". Fix: a true city alias appears
   in many DISTINCT names — added `match_city_overcommon_min_distinct_
   signatures` (default 10) gate. ('cochin' in Kochi still strips: hundreds of
   distinct names. The distinct-name key falls back from sorted-token string to
   core signature depending on pipeline stage — the normalize-path df has no
   signature yet, and sorted_tokens there is a STRING not a list.)
2. Degenerate names built ENTIRELY of weak/short/contextual tokens
   ("vila bled", "hotel city maribor", "cha cha rooms") end up with EMPTY match
   tokens. Per-token filtering cannot see combination identity. Fix: when the
   filtered set is empty, fall back to full name tokens (len>=3, non-numeric).
   Identical full names then hit the exact match-token-signature level; mixed
   fallback-vs-filtered comparisons overlap weakly -> conservative direction.
SI rerun (SI_002 replaced SI_001): singletons 7,337 -> 5,786 (-21%), auto
4,095 -> 4,425 clusters; identical-name singleton groups 439/1,509 -> 79/163
(-89%). Safety verified: the three DIFFERENT Maribor hotels ("hotel city
maribor" / "b and b hotel maribor" / "hotel maribor and garden rooms", all
core-collapsed to 'maribor') stayed in three separate clusters; v2_4 splits
healthy; all EM name levels now train.
NOTE: both fixes change the match-token frame for EVERY country — IN/NZ/US
artifacts predate them and should be rerun for consistency (expect recall
gains in small towns, no precision regression mechanism identified).

**MK recall audit (2026-06-10, same day) — seven more fixes, traced from two
user-reported clusters ("by the lake apartments" / "accommodation tanja",
both Ohrid, with obviously-identical singletons left unmapped):**
1. **Exact-sig pairs missing the 0.99 bar on weak-field noise**: identical
   name, 0m apart, p=0.982 because property_type 'unknown' vs 'hotel'.
   New auto-edge clause (run_clustering): gate 'signature' + sig_exact +
   dist<=100m + prob>=0.95 (`SIG_EXACT_AUTO_*`).
2. **'unknown' property_type is a sentinel, not a type** (21% of MK records):
   comparing it manufactures evidence both ways. Now NULLed in scoring prep.
3. **Fallback signatures keep city suffixes**: "by the lake apartments ohrid"
   != "by the lake apartments". Fallback now subtracts the record's own
   city/state/country context tokens (names.py).
4. **TF adjustment on the exact match-token-signature level**
   (splink_settings): exact match on a RARE signature now weighs more than on
   a generic one; directionally safer at both ends.
5. **New v2_1 blocking rule on the MATCH signature** (city+country+match_sig):
   records whose name carries a city suffix AND whose coords land in another
   h3 cell were invisible to every existing rule.
6. **lat_lng_low_precision honored config min_decimal_places only on paper**:
   flag fired solely on whole-degree coords; 2dp (~±700m) passed as located
   and tripped the 300-500m geo guards. Flag now covers any precision below
   `geo.min_decimal_places` (geo.py). Low-precision coords are NULLed in the
   scoring distance comparison AND in clustering members/split-subgroup geo
   (fake precision must not produce negative evidence or inflate diameters);
   h3/blocking keys keep using them.
7. **v2_4 sig+city adoption pass** (`_adopt_sig_city_singletons`): name-only
   records (no geo, no contacts) score 0.1-0.7 forever — probability cannot
   carry them. When EVERY clustered record with the same match signature +
   city sits in ONE geo-tight (<=300m) component, the singleton is adopted via
   a synthetic 'sig_city_adoption' edge (prob 0.95, sig_exact). Guards learned
   the hard way: good-coords singletons must be within 300m (1km tolerance
   flipped 67 clusters to geo-diameter review), same-provider adoptions are
   skipped (tripped the SPD guard), cap 10/group.
MK final: auto 1,582 clusters, review 12, singletons 2,477 -> 1,703 (-31%),
497 adoptions, all four user-reported records mapped, zero new review noise.
ALL countries rerun with the consolidated ruleset the same day.

**Fragment consolidation + SM microstate session (2026-06-10, cont.):**
- MK 'ajro rooms': ONE hotel split into TWO 2-record clusters (provider
  geocode groups 156m apart; cross pairs <0.80 — distance evidence fades past
  75m) which then blocked adoption ("two targets = ambiguous"). The adoption
  pass now also MERGES sibling components: all components+singletons sharing
  (match sig) whose good coords fit a 250m radius are one place
  (`v2_4_sig_city_merges`). Merged same-provider fragments correctly land in
  same_provider_duplicate review, not silent merge.
- SM (San Marino, 365 records) exposed the microstate failure stack:
  (1) EM on 365 records is GARBAGE — exact-sig identical-coords pairs scored
  0.02; only 44/365 records clustered. The pass now BOOTSTRAPS clusters with
  no edge-backed anchor: >=2 providers + exact match sig + good coords within
  250m is sufficient evidence without any Splink score
  (`v2_4_sig_city_bootstraps`).
  (2) City labels are provider noise in microstates ('hotel rio re' at
  IDENTICAL coords labeled san marino/acquaviva/dogana) — grouping is now
  signature-FIRST; only geo-incoherent sigs (real multi-place names) fall
  back to per-city subgroups.
  (3) Fallback sigs differ on type words ("hotel rio re" vs "rio re") — the
  fallback is now two-tier: prefer dropping default match-low-value type
  words; keep them only if that empties the name.
  SM result: 44 -> 290/365 records auto-mapped (12% -> 79%), singletons
  321 -> 53, all 'rio re'/'antica colombaia' records in single clusters.
- **OOM lesson:** the first match-sig blocking rule was keyed city+match_sig;
  on US-scale data, generic fallback signatures within big cities blew
  scoring past 12GB RSS — kernel OOM-killed the run. Re-keyed to
  **h3_7+match_sig** (~2.4km cells, tolerant of 2-decimal coords, bounded
  blocks). h3_7 added to scoring `prepped`.
- Stage-rerun rule of thumb: scoring changes need full pipeline reruns;
  clustering/adoption changes only need the cheap in-place
  run_clustering+run_review_queue pass.

**DB export (09F-write) + multi-country runs (2026-06-10):**
`hotelmap/export/run_export.py` — write logic mirrored from
hotel_mapping_modified: refreshes `tripgain_hotel_info_v6` (one row/cluster,
first-non-null merge across members, representative first),
`tripgain_hotel_mappings_v6` (one row/provider record, mapping_type
auto/review, confidence = min_edge_probability), `cluster_summary_v6`
(supplier_breakdown + members JSON) via the Embedding Gateway WRITE API
(X-Write-API-Key; key = WRITE_API_KEY or EMBEDDING_GATEWAY_WRITE_API_KEY from
env or .env). Deliberate divergences from the mirror: refresh is
COUNTRY-scoped (`DELETE where tripgain_id LIKE 'CC\\_%'`, NOT delete-all —
sequential multi-country exports must not wipe each other) and singleton ids
are country-prefixed (`IN_GH6-<md5>`), else singleton rows are unattributable.
CLI: `python -m hotelmap.export.run_export --run <dir> [--dry-run]`; defaults
v2_4 / splink_v2_1. Pipeline flag `--export-db` runs it after the review
queue. Live-verified 2026-06-10: WRITE_API_KEY in .env (the key routes to
db=demo_v5 server-side — WRITE_DB/USER/PASSWORD in .env are informational),
LIKE-where scoped delete accepted, insert+delete round trip OK.
Pipeline `--country` accepts a CSV (`IN,US,...`, no limit) and runs
sequentially; a failed country is logged (`COUNTRY FAILED CC:`) and skipped,
summary `ALL DONE (n/m, FAILED: ...)`, exit 1 if any failed. Dashboard run
page: comma-separated country input, "write to DB" checkbox (disabled until
WRITE_API_KEY exists), per-country stage display ("US 2/3 · scoring"), and an
Open button per completed run. Verified end-to-end with SM,LU via the API.

**Cluster search by hotel name**: `/api/clusters?search=` now matches ANY
member hotel name (new `dashboard_cluster_names` temp table: cluster_id ->
string_agg of distinct member names) as well as cluster id; clusters table
gained Hotel (rep_name) + City columns (LEFT JOIN dashboard_cluster_centroids;
api_clusters now calls ensure_city_tables, so first hit pays the temp-table
build like Cities/Unmapped). Searching "kyzen" surfaces all 4 kyzen clusters.

---

## Dashboard frontend v2 — "Winning"-style console (DONE 2026-06-10)

Full UI rebuild of `hotelmap/dashboard/static/` (still dependency-free, inline
SVG charts): rounded sidebar + cards, white highlighted KPI card, dual-bar
cluster-size chart, mapping-distribution donut, light/dark toggle (localStorage).
New views + endpoints:
- **Cities** (`/api/cities?search=`, `/api/city-clusters?city=`): per-city
  records/clusters/mapped-share/unmapped; city drawer lists clusters + unmapped.
- **Unmapped** (`/api/unmapped?city=&provider=&search=`): singleton records;
  row click → `/api/nearest-clusters?record_id=` = haversine vs materialized
  cluster centroids (temp tables `dashboard_city_stats`,
  `dashboard_cluster_centroids`; built once per run, reset on run switch;
  falls back to same-city clusters when the record has no coords).
- **Run switcher** (`/api/runs`, `/api/switch-run?run=`): topbar dropdown to hop
  between IN / NZ / US runs in one server process (clears lru caches + temp
  tables, page reloads). India resolves to v2_2 until rerun as v2_3.
Building-level clusters show a "building" tag in tables/drawers.

## Dashboard frontend — dark review/cluster console (DONE baseline)

Module: `hotelmap/dashboard/server.py`, static frontend in
`hotelmap/dashboard/static/`.

Run locally:
```
.venv/bin/python -m hotelmap.dashboard.server \
  --run data/artifacts/runs/2026-06-09_005 --version v2_1 --port 8000
```

Current URL while this note was written: `http://127.0.0.1:8000`.

Implemented:
- Dependency-free stdlib HTTP server serving `index.html`, `style.css`, `app.js`.
- JSON APIs backed by DuckDB over parquet/CSV artifacts:
  `/api/overview`, `/api/providers`, `/api/clusters`,
  `/api/clusters/<cluster_id>`, `/api/review-summary`,
  `/api/review-queue`, `/api/review-queue/<review_id>`.
- `/api/review-queue` materializes a temp DuckDB table from the full 09E parquet
  queue on first request, so the UI can page/filter all **304,572** review items
  instead of only the 5,000-row `review_queue_top.csv`. Cold first hit takes a few
  seconds; cached bucket filtering measured around 0.075s.
- Dark operational UI with four views:
  1. Overview: v2.1 run KPIs, record flow, quality rates, provider counts, review buckets.
  2. Clusters: sortable/filterable cluster table plus drawer member/edge detail.
  3. Review Queue: prioritized 09E queue with bucket/action/search filters and detail drawer.
  4. Providers: feed quality/coverage table.

Validation completed:
- `node --check hotelmap/dashboard/static/app.js`
- `.venv/bin/python -m compileall -q hotelmap/dashboard`
- API smoke checks for overview, providers, clusters, cluster detail, review summary,
  review queue, and review detail all passed against run `2026-06-09_005`.
- Headless Firefox rendered the dashboard shell to `/tmp/hotelmap_dashboard.png`.
- Full review queue smoke: `/api/review-queue?page_size=1` returns total 304,572;
  `bucket=medium_name_plus_geo` returns total 249,905; review detail endpoint works
  for `RQE_1a8f630207ed802d`.

Notes for next frontend pass:
- The current UI is intentionally dense and work-focused; no landing page.
- Review decisions are not yet writable from the UI. The backend currently exposes
  queue/detail data only; decision capture can be added once the reviewer workflow
  fields are final.
- Full pair queue rows reconstruct priority/bucket/name/provider/city fields from
  `review_queue_edges.parquet` + `review_queue_members.parquet`; pair-level
  `shared_phones/shared_emails/shared_domains` are not persisted in those support
  tables, so they are null outside the compact top CSV.
- Browser screenshot tooling in this environment captures before async API data fully
  paints, so manual browser inspection at `http://127.0.0.1:8000` is still useful.
