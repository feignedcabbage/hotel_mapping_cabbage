/* HotelMap console — vanilla JS, no deps. */

const state = {
  overview: null,
  providers: null,
  reviewSummary: null,
  sizeHist: null,
  reviewReasons: null,
  clusters: { page: 1, sort: "cluster_size", dir: "desc", last: null },
  clusterDetail: { id: null, data: null },
  reviews: { page: 1 },
  cities: { page: 1 },
  unmapped: { page: 1 },
  pipeline: { timer: null, configs: null },
  view: "overview",
};

const VIEW_TITLES = {
  overview: ["Welcome back", "Mapping run health at a glance"],
  clusters: ["Clusters", "Auto-accepted and review-routed connected components"],
  clusterDetail: ["Cluster detail", "Provider records, matching evidence and geo spread"],
  cities: ["Cities", "Search any city — coverage, clusters and unmapped hotels"],
  unmapped: ["Unmapped hotels", "Singletons with no cluster — find their closest clusters"],
  reviews: ["Review queue", "Prioritized clerical review items"],
  providers: ["Providers", "Feed quality and coverage"],
  pipeline: ["Pipeline", "Download → normalize → match → cluster, end to end"],
};

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const C = {
  indigo: "#7c6cf6", orange: "#ff8a5c", green: "#3ddc97",
  yellow: "#ffc555", red: "#ff6b6b",
};

function fmt(n, digits = 0) {
  if (n === null || n === undefined || Number.isNaN(Number(n))) return "—";
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: digits });
}
function compact(n) {
  if (n === null || n === undefined) return "";
  const v = Number(n);
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
  return String(v);
}
function pct(v, digits = 1) {
  if (v === null || v === undefined) return "—";
  return `${(Number(v) * 100).toFixed(digits)}%`;
}
function km(m) {
  if (m === null || m === undefined) return "—";
  const v = Number(m);
  return v >= 1000 ? `${(v / 1000).toFixed(1)} km` : `${Math.round(v)} m`;
}
function esc(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}
function trunc(v, n = 72) {
  const s = String(v ?? "—");
  return s.length > n ? `${s.slice(0, n - 1)}…` : s;
}
async function api(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  const data = await res.json();
  if (data.error) throw new Error(data.error);
  return data;
}
function tag(text, cls = "muted") { return `<span class="tag ${cls}">${esc(text)}</span>`; }
function loading() { return `<div class="loading"><div class="spinner"></div><div>Loading</div></div>`; }
function empty(msg) { return `<div class="empty">${esc(msg)}</div>`; }

/* ---------------- charts (inline SVG) ---------------- */

function sparkline(series, color, w = 96, h = 38) {
  if (!series?.length) return "";
  const max = Math.max(...series, 1);
  const min = Math.min(...series, 0);
  const span = max - min || 1;
  const pts = series.map((v, i) => [
    (i / (series.length - 1)) * (w - 4) + 2,
    h - 4 - ((v - min) / span) * (h - 10),
  ]);
  const line = pts.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ");
  const area = `${line} L${pts[pts.length - 1][0].toFixed(1)},${h - 1} L${pts[0][0].toFixed(1)},${h - 1} Z`;
  return `<svg class="k-spark" viewBox="0 0 ${w} ${h}">
    <path d="${area}" fill="${color}" opacity="0.13"/>
    <path d="${line}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>`;
}

function dualBars(rows, aKey, bKey, labelKey) {
  const W = 760, H = 250, padL = 44, padB = 26, padT = 10;
  const innerW = W - padL - 12, innerH = H - padT - padB;
  const max = Math.max(1, ...rows.flatMap((r) => [Number(r[aKey] || 0), Number(r[bKey] || 0)]));
  const group = innerW / rows.length;
  const bw = Math.min(13, group * 0.26);
  const y = (v) => padT + innerH - (v / max) * innerH;
  const gridLines = 4;
  let svg = "";
  for (let i = 0; i <= gridLines; i++) {
    const gy = padT + (innerH / gridLines) * i;
    const val = Math.round(max - (max / gridLines) * i);
    svg += `<line x1="${padL}" y1="${gy}" x2="${W - 8}" y2="${gy}" stroke="currentColor" opacity="0.07"/>`;
    svg += `<text class="axis-label" x="${padL - 8}" y="${gy + 3}" text-anchor="end">${compact(val)}</text>`;
  }
  rows.forEach((r, i) => {
    const cx = padL + group * i + group / 2;
    const a = Number(r[aKey] || 0), b = Number(r[bKey] || 0);
    svg += `<rect x="${cx - bw - 1.5}" y="${y(a)}" width="${bw}" height="${Math.max(2, padT + innerH - y(a))}" rx="3" fill="${C.indigo}"/>`;
    svg += `<rect x="${cx + 1.5}" y="${y(b)}" width="${bw}" height="${Math.max(2, padT + innerH - y(b))}" rx="3" fill="${C.orange}"/>`;
    svg += `<text class="axis-label" x="${cx}" y="${H - 8}" text-anchor="middle">${esc(r[labelKey])}</text>`;
  });
  return `<div class="chart-box"><svg viewBox="0 0 ${W} ${H}">${svg}</svg></div>`;
}

function donut(parts) {
  const total = parts.reduce((s, p) => s + p.value, 0) || 1;
  const R = 56, SW = 17, CX = 75, CY = 75;
  const circ = 2 * Math.PI * R;
  let offset = circ * 0.25;
  let rings = "";
  parts.forEach((p) => {
    const frac = p.value / total;
    rings += `<circle cx="${CX}" cy="${CY}" r="${R}" fill="none" stroke="${p.color}"
      stroke-width="${SW}" stroke-linecap="round"
      stroke-dasharray="${Math.max(0.01, frac * circ - 3)} ${circ}"
      stroke-dashoffset="${offset}" transform="rotate(-90 ${CX} ${CY})"/>`;
    offset -= frac * circ;
  });
  const legend = parts.map((p) => `
    <div class="dl-row"><span class="dl-dot" style="background:${p.color}"></span>
    ${esc(p.label)}&nbsp;<b>${fmt(p.value)}</b>&nbsp;<span class="faint">${pct(p.value / total)}</span></div>
  `).join("");
  return `
    <div class="donut-wrap">
      <svg width="150" height="150" viewBox="0 0 150 150">${rings}
        <text x="${CX}" y="${CY - 3}" text-anchor="middle" font-size="20" font-weight="700" fill="currentColor">${compact(total)}</text>
        <text x="${CX}" y="${CY + 15}" text-anchor="middle" font-size="10" fill="currentColor" opacity="0.5">records</text>
      </svg>
      <div class="donut-legend">${legend}</div>
    </div>`;
}

function hbars(rows, labelKey, valueKey, color = C.indigo) {
  const max = Math.max(1, ...rows.map((r) => Number(r[valueKey] || 0)));
  return rows.map((r) => `
    <div class="rate-row">
      <span class="rl" title="${esc(r[labelKey])}">${esc(trunc(r[labelKey], 34))}</span>
      <span style="flex:1;margin:0 14px"><span class="progress" style="width:100%"><div style="width:${Math.max(2, (Number(r[valueKey] || 0) / max) * 100)}%;background:${color}"></div></span></span>
      <span class="rv">${fmt(r[valueKey])}</span>
    </div>`).join("");
}

/* ---------------- shell ---------------- */

function setView(name) {
  state.view = name;
  $$(".nav-item").forEach((b) => b.classList.toggle("active", b.dataset.view === (name === "clusterDetail" ? "clusters" : name)));
  $$(".view").forEach((v) => v.classList.toggle("hidden", v.id !== name));
  const [t, s] = VIEW_TITLES[name] || ["", ""];
  $("#pageTitle").textContent = t;
  $("#pageSub").textContent = s;
  if (name === "clusters") renderClusters();
  if (name === "clusterDetail") renderClusterDetailPage();
  if (name === "cities") renderCities();
  if (name === "unmapped") renderUnmapped();
  if (name === "reviews") renderReviews();
  if (name === "providers") renderProviders();
  if (name === "pipeline") renderPipeline();
}

function kpiCard({ label, value, delta, deltaCls = "up", spark, sparkColor = C.indigo, hi = false }) {
  return `
    <div class="card kpi ${hi ? "hi" : ""}">
      <div class="k-label">${esc(label)}</div>
      <div class="k-row">
        <div>
          <div class="k-value">${value}</div>
          ${delta ? `<span class="delta ${deltaCls}">${delta}</span>` : ""}
        </div>
        ${spark ? sparkline(spark, sparkColor) : ""}
      </div>
    </div>`;
}

/* ---------------- overview ---------------- */

function sizeBuckets(hist) {
  const buckets = [];
  for (let s = 2; s <= 13; s++) {
    const row = hist.find((h) => Number(h.size) === s) || {};
    buckets.push({ label: String(s), auto: Number(row.auto || 0), review: Number(row.review || 0) });
  }
  const tailA = hist.filter((h) => Number(h.size) >= 14).reduce((s, h) => s + Number(h.auto || 0), 0);
  const tailR = hist.filter((h) => Number(h.size) >= 14).reduce((s, h) => s + Number(h.review || 0), 0);
  buckets.push({ label: "14+", auto: tailA, review: tailR });
  return buckets;
}

async function renderOverview() {
  const ov = state.overview;
  const c = ov.clustering || {};
  const total = ov.total_records || 0;
  const autoRecords = c.records_in_auto || 0;
  const reviewRecords = c.records_in_review || 0;
  const singles = c.singleton_unmatched || 0;
  const provSeries = Object.values(ov.records_per_provider || {}).sort((a, b) => a - b);

  if (!state.sizeHist) state.sizeHist = (await api("/api/size-histogram")).histogram || [];
  if (!state.reviewReasons) state.reviewReasons = (await api("/api/review-reasons")).reasons || [];
  let topReview = [];
  try { topReview = (await api("/api/review-queue?page_size=6")).items || []; } catch { /* optional */ }

  const provRows = Object.entries(ov.records_per_provider || {})
    .map(([provider, records]) => ({ provider, records }))
    .sort((a, b) => b.records - a.records).slice(0, 8);

  $("#overview").innerHTML = `
    <div class="grid cols-4">
      ${kpiCard({
        label: "Total records", value: fmt(total), hi: true,
        delta: `${esc(ov.country)} · ${esc(ov.version)}`, deltaCls: "info",
        spark: provSeries, sparkColor: C.indigo,
      })}
      ${kpiCard({
        label: "Auto-matched records", value: fmt(autoRecords),
        delta: `↗ ${pct(autoRecords / Math.max(total, 1))} of feed`, deltaCls: "up",
        spark: state.sizeHist.slice(0, 14).map((h) => Number(h.auto || 0)), sparkColor: C.green,
      })}
      ${kpiCard({
        label: "Records in review", value: fmt(reviewRecords),
        delta: `${fmt(c.review)} clusters`, deltaCls: "warn",
        spark: state.sizeHist.slice(0, 14).map((h) => Number(h.review || 0)), sparkColor: C.yellow,
      })}
      ${kpiCard({
        label: "Unmapped singletons", value: fmt(singles),
        delta: `${pct(singles / Math.max(total, 1))} of feed`, deltaCls: "down",
        spark: provSeries.slice().reverse(), sparkColor: C.orange,
      })}
    </div>

    <div class="grid cols-21 mt">
      <div class="card">
        <div class="card-title">Cluster sizes
          <span class="legend">
            <span class="legend-dot" style="--c:${C.indigo}">auto</span>
            <span class="legend-dot" style="--c:${C.orange}">review</span>
          </span>
        </div>
        ${dualBars(sizeBuckets(state.sizeHist), "auto", "review", "label")}
      </div>
      <div class="card">
        <div class="card-title">Mapping distribution</div>
        ${donut([
          { label: "Auto-matched", value: autoRecords, color: C.indigo },
          { label: "In review", value: reviewRecords, color: C.orange },
          { label: "Unmapped", value: singles, color: C.yellow },
        ])}
        ${c.building_merged_auto ? `<div class="rate-row"><span class="rl">Building-level clusters</span><span class="rv">${fmt(c.building_merged_auto)}</span></div>` : ""}
        ${c.largest_cluster_size ? `<div class="rate-row"><span class="rl">Largest cluster</span><span class="rv">${fmt(c.largest_cluster_size)}</span></div>` : ""}
      </div>
    </div>

    <div class="grid cols-21 mt">
      <div class="card table-card">
        <div class="card-title" style="padding:12px 14px 0">Top review items</div>
        ${topReview.length ? `
        <div class="table-wrap"><table>
          <thead><tr><th>Names</th><th>Bucket</th><th>Cities</th><th class="num">Priority</th><th class="num">Prob</th></tr></thead>
          <tbody>${topReview.map((r) => `
            <tr class="clickable" data-review="${esc(r.review_id)}">
              <td title="${esc(r.representative_names)}">${esc(trunc(r.representative_names, 58))}</td>
              <td>${tag(r.review_bucket, r.review_bucket?.includes("risk") ? "risk" : "review")}</td>
              <td>${esc(trunc(r.cities, 26))}</td>
              <td class="num">${fmt(r.priority)}</td>
              <td class="num mono">${fmt(r.best_edge_probability, 3)}</td>
            </tr>`).join("")}
          </tbody></table></div>` : empty("No review queue available")}
      </div>
      <div class="card">
        <div class="card-title">Providers</div>
        ${hbars(provRows, "provider", "records")}
      </div>
    </div>

    <div class="grid cols-2 mt">
      <div class="card">
        <div class="card-title">Review reasons</div>
        ${hbars(state.reviewReasons.slice(0, 8), "reason", "clusters", C.orange)}
      </div>
      <div class="card">
        <div class="card-title">Quality rates</div>
        ${[["empty_name_core_rate", "Empty name core"], ["invalid_coord_rate", "Invalid coordinates"],
           ["out_of_range_coord_rate", "Out-of-range coordinates"], ["phone_reuse_rate", "Phone reuse"],
           ["email_reuse_rate", "Email reuse"], ["domain_reuse_rate", "Domain reuse"]]
          .map(([k, l]) => `<div class="rate-row"><span class="rl">${l}</span><span class="rv">${pct((ov.rates || {})[k], 2)}</span></div>`).join("")}
      </div>
    </div>
  `;
  $$("#overview tr.clickable").forEach((tr) => tr.addEventListener("click", () => openReview(tr.dataset.review)));
}

/* ---------------- clusters ---------------- */

async function renderClusters() {
  const el = $("#clusters");
  if (!el.dataset.ready) {
    el.innerHTML = `
      <div class="filterbar">
        <div class="field"><label>Status</label><select id="clusterStatus"><option value="">All</option><option value="auto_accept">Auto</option><option value="review">Review</option></select></div>
        <div class="field"><label>Provider</label><select id="clusterProvider"><option value="">All</option></select></div>
        <div class="field"><label>Contact</label><select id="clusterContact"><option value="">All</option><option value="yes">Yes</option><option value="no">No</option></select></div>
        <div class="field"><label>Min size</label><input id="clusterMinSize" type="number" value="2" min="2"></div>
        <div class="field"><label>Search</label><input id="clusterSearch" class="wide" type="text" placeholder="hotel name or cluster id…"></div>
        <button class="btn primary" id="clusterApply">Apply</button>
      </div>
      <div id="clusterTable" class="card table-card">${loading()}</div>
    `;
    const providers = Object.keys(state.overview.records_per_provider || {}).sort();
    $("#clusterProvider").insertAdjacentHTML("beforeend", providers.map((p) => `<option>${esc(p)}</option>`).join(""));
    $("#clusterApply").addEventListener("click", () => { state.clusters.page = 1; loadClusters(); });
    ["clusterStatus", "clusterProvider", "clusterContact", "clusterMinSize"].forEach((id) =>
      $(`#${id}`).addEventListener("change", () => { state.clusters.page = 1; loadClusters(); }));
    $("#clusterSearch").addEventListener("keydown", (e) => { if (e.key === "Enter") { state.clusters.page = 1; loadClusters(); } });
    el.dataset.ready = "1";
  }
  await loadClusters();
}

async function loadClusters() {
  const qs = clusterParams();
  const data = await api(`/api/clusters?${qs}`);
  state.clusters.last = data;
  $("#clusterTable").innerHTML = clusterTable(data);
  bindClusterTable();
}

function clusterParams(overrides = {}) {
  return new URLSearchParams({
    status: $("#clusterStatus").value, provider: $("#clusterProvider").value,
    contact: $("#clusterContact").value, min_size: $("#clusterMinSize").value || "2",
    search: $("#clusterSearch").value, sort: state.clusters.sort, dir: state.clusters.dir,
    page: state.clusters.page, page_size: 50,
    ...overrides,
  });
}

function clusterTh(sort, label) {
  const active = state.clusters.sort === sort;
  const arr = active ? (state.clusters.dir === "desc" ? "↓" : "↑") : "";
  return `<th class="sortable num" data-sort="${sort}">${esc(label)} ${arr}</th>`;
}

function clusterTable(data) {
  if (!data.clusters?.length) return empty("No clusters");
  return `
    <div class="table-wrap"><table>
      <thead><tr>
        <th>Hotel</th><th>City</th>
        ${clusterTh("cluster_size", "Size")}
        ${clusterTh("provider_count", "Prov")}
        <th>Status</th><th>Evidence</th>
        ${clusterTh("geo", "Geo")}
        ${clusterTh("prob", "Min prob")}
        <th>Review reasons</th><th>Cluster</th>
      </tr></thead>
      <tbody>${data.clusters.map((c) => `
        <tr class="clickable" data-cluster="${esc(c.cluster_id)}">
          <td title="${esc(c.rep_name)}">${esc(trunc(c.rep_name, 44))}</td>
          <td>${esc(trunc(c.city, 20))}</td>
          <td class="num">${fmt(c.cluster_size)}</td>
          <td class="num">${fmt(c.provider_count)}</td>
          <td>${tag(c.cluster_status === "auto_accept" ? "auto" : "review", c.cluster_status === "auto_accept" ? "auto" : "review")}</td>
          <td>${evidence(c)}</td>
          <td class="num">${km(c.max_geo_diameter_m)}</td>
          <td class="num mono">${fmt(c.min_edge_probability, 3)}</td>
          <td>${reasonChips(c.review_reasons)}</td>
          <td class="mono faint">${esc(c.cluster_id)}</td>
        </tr>`).join("")}
      </tbody></table></div>
    ${pager(data, "clusters")}
  `;
}

function bindClusterTable() {
  $$("#clusterTable th.sortable").forEach((th) => th.addEventListener("click", () => {
    const sort = th.dataset.sort;
    if (state.clusters.sort === sort) state.clusters.dir = state.clusters.dir === "desc" ? "asc" : "desc";
    else { state.clusters.sort = sort; state.clusters.dir = "desc"; }
    loadClusters();
  }));
  $$("#clusterTable tr.clickable").forEach((tr) => tr.addEventListener("click", () => openClusterPage(tr.dataset.cluster)));
  bindPager("clusters", loadClusters);
}

function reasonChips(items) {
  const arr = Array.isArray(items) ? items : [];
  if (!arr.length) return `<span class="faint">—</span>`;
  return `<div class="chip-row">${arr.slice(0, 2).map((r) => tag(r, "review")).join("")}${arr.length > 2 ? tag(`+${arr.length - 2}`) : ""}</div>`;
}

function evidence(c) {
  return `<span class="evidence">
    <span class="ev ${c.has_phone_edge ? "on" : ""}" title="phone">P</span>
    <span class="ev ${c.has_email_edge ? "on" : ""}" title="email">E</span>
    <span class="ev ${c.has_domain_edge ? "on" : ""}" title="domain">D</span>
  </span>`;
}

function pager(data, key) {
  return `
    <div class="pager">
      <div>${fmt(data.total)} rows · page ${fmt(data.page)} / ${fmt(data.pages)}</div>
      <div class="nav">
        <button class="btn small pager-btn" data-key="${key}" data-step="-1" ${data.page <= 1 ? "disabled" : ""}>← Prev</button>
        <button class="btn small pager-btn" data-key="${key}" data-step="1" ${data.page >= data.pages ? "disabled" : ""}>Next →</button>
      </div>
    </div>`;
}
function bindPager(key, fn) {
  $$(`.pager-btn[data-key="${key}"]`).forEach((b) => b.addEventListener("click", () => {
    state[key].page += Number(b.dataset.step); fn();
  }));
}

/* ---------------- cities ---------------- */

async function renderCities() {
  const el = $("#cities");
  if (!el.dataset.ready) {
    el.innerHTML = `
      <div class="filterbar">
        <div class="field"><label>Search city</label><input id="citySearch" class="wide" type="text" placeholder="udaipur, queenstown, kissimmee…"></div>
        <button class="btn primary" id="cityApply">Search</button>
      </div>
      <div id="cityTable" class="card table-card">${loading()}</div>
    `;
    $("#cityApply").addEventListener("click", () => { state.cities.page = 1; loadCities(); });
    $("#citySearch").addEventListener("keydown", (e) => { if (e.key === "Enter") { state.cities.page = 1; loadCities(); } });
    el.dataset.ready = "1";
  }
  await loadCities();
}

async function loadCities() {
  const qs = new URLSearchParams({
    search: $("#citySearch").value, page: state.cities.page, page_size: 30,
  });
  const data = await api(`/api/cities?${qs}`);
  if (!data.cities?.length) { $("#cityTable").innerHTML = empty("No cities match"); return; }
  $("#cityTable").innerHTML = `
    <div class="table-wrap"><table>
      <thead><tr>
        <th>City</th><th class="num">Records</th><th class="num">Clusters</th>
        <th>Mapped</th><th class="num">In review</th><th class="num">Unmapped</th><th class="num">Providers</th>
      </tr></thead>
      <tbody>${data.cities.map((r) => `
        <tr class="clickable" data-city="${esc(r.city)}">
          <td style="font-weight:600">${esc(r.city)}</td>
          <td class="num">${fmt(r.records)}</td>
          <td class="num">${fmt(r.clusters)}</td>
          <td><span class="progress"><div style="width:${Math.round((r.matched_share || 0) * 100)}%"></div></span>
              <span class="faint" style="margin-left:8px">${pct(r.matched_share)}</span></td>
          <td class="num">${fmt(r.review_records)}</td>
          <td class="num" style="color:${r.unmapped_records ? "var(--orange)" : "inherit"}">${fmt(r.unmapped_records)}</td>
          <td class="num">${fmt(r.providers)}</td>
        </tr>`).join("")}
      </tbody></table></div>
    ${pager(data, "cities")}
  `;
  $$("#cityTable tr.clickable").forEach((tr) => tr.addEventListener("click", () => openCity(tr.dataset.city)));
  bindPager("cities", loadCities);
}

async function openCity(city) {
  openDrawer("City", city, loading());
  const [cl, un] = await Promise.all([
    api(`/api/city-clusters?city=${encodeURIComponent(city)}`),
    api(`/api/unmapped?city=${encodeURIComponent(city)}&page_size=30`),
  ]);
  const clusters = cl.clusters || [];
  const unmapped = un.items || [];
  openDrawer("City", city, `
    <div class="meta-grid">
      <div class="meta-item"><div class="ml">Clusters</div><div class="mv">${fmt(clusters.length)}${clusters.length === 200 ? "+" : ""}</div></div>
      <div class="meta-item"><div class="ml">Unmapped</div><div class="mv">${fmt(un.total)}</div></div>
      <div class="meta-item"><div class="ml">Biggest cluster</div><div class="mv">${fmt(clusters[0]?.cluster_size)}</div></div>
    </div>
    <div class="section-label">Clusters in ${esc(city)}</div>
    ${clusters.length ? clusters.slice(0, 30).map((c) => `
      <div class="near-row" data-cluster="${esc(c.cluster_id)}">
        <span class="nr-name">${esc(c.rep_name || c.cluster_id)}</span>
        ${c.entity_level === "building" ? tag("building", "building") : ""}
        ${tag(c.cluster_status === "auto_accept" ? "auto" : "review", c.cluster_status === "auto_accept" ? "auto" : "review")}
        ${tag(`${c.cluster_size} recs`)}
      </div>`).join("") : empty("No clusters in this city")}
    <div class="section-label">Unmapped hotels ${un.total > 30 ? `(30 of ${fmt(un.total)})` : ""}</div>
    ${unmapped.length ? unmapped.map((u) => `
      <div class="near-row" data-record="${esc(u.record_id)}">
        <span class="nr-name">${esc(u.name || u.record_id)}</span>
        ${tag(u.provider, "prov")}
        <span class="nr-dist">nearest →</span>
      </div>`).join("") : empty("Everything in this city is mapped 🎉")}
  `);
  $$("#drawerBody .near-row[data-cluster]").forEach((r) => r.addEventListener("click", () => openCluster(r.dataset.cluster)));
  $$("#drawerBody .near-row[data-record]").forEach((r) => r.addEventListener("click", () => openNearest(r.dataset.record)));
}

/* ---------------- unmapped ---------------- */

async function renderUnmapped() {
  const el = $("#unmapped");
  if (!el.dataset.ready) {
    el.innerHTML = `
      <div class="filterbar">
        <div class="field"><label>Search name / city / id</label><input id="unmSearch" class="wide" type="text" placeholder="hotel name…"></div>
        <div class="field"><label>City (exact)</label><input id="unmCity" type="text" placeholder="auckland"></div>
        <div class="field"><label>Provider</label><select id="unmProvider"><option value="">All</option></select></div>
        <button class="btn primary" id="unmApply">Apply</button>
      </div>
      <div id="unmTable" class="card table-card">${loading()}</div>
    `;
    const providers = Object.keys(state.overview.records_per_provider || {}).sort();
    $("#unmProvider").insertAdjacentHTML("beforeend", providers.map((p) => `<option>${esc(p)}</option>`).join(""));
    $("#unmApply").addEventListener("click", () => { state.unmapped.page = 1; loadUnmapped(); });
    $("#unmProvider").addEventListener("change", () => { state.unmapped.page = 1; loadUnmapped(); });
    ["unmSearch", "unmCity"].forEach((id) =>
      $(`#${id}`).addEventListener("keydown", (e) => { if (e.key === "Enter") { state.unmapped.page = 1; loadUnmapped(); } }));
    el.dataset.ready = "1";
  }
  await loadUnmapped();
}

async function loadUnmapped() {
  const qs = new URLSearchParams({
    search: $("#unmSearch").value, city: $("#unmCity").value,
    provider: $("#unmProvider").value, page: state.unmapped.page, page_size: 50,
  });
  const data = await api(`/api/unmapped?${qs}`);
  if (!data.items?.length) { $("#unmTable").innerHTML = empty("No unmapped hotels match"); return; }
  $("#unmTable").innerHTML = `
    <div class="table-wrap"><table>
      <thead><tr><th>Hotel</th><th>Provider</th><th>City</th><th>Postal</th><th>Type</th><th class="num">Coords</th><th></th></tr></thead>
      <tbody>${data.items.map((u) => `
        <tr class="clickable" data-record="${esc(u.record_id)}">
          <td style="font-weight:600" title="${esc(u.record_id)}">${esc(trunc(u.name, 56))}</td>
          <td>${tag(u.provider, "prov")}</td>
          <td>${esc(u.city || "—")}</td>
          <td class="mono faint">${esc(u.postal_code || "—")}</td>
          <td>${esc(u.property_type || "—")}</td>
          <td class="num mono faint">${u.lat != null ? `${u.lat}, ${u.lng}` : "—"}</td>
          <td><button class="btn small">Nearest →</button></td>
        </tr>`).join("")}
      </tbody></table></div>
    ${pager(data, "unmapped")}
  `;
  $$("#unmTable tr.clickable").forEach((tr) => tr.addEventListener("click", () => openNearest(tr.dataset.record)));
  bindPager("unmapped", loadUnmapped);
}

async function openNearest(recordId) {
  openDrawer("Unmapped hotel", recordId, loading());
  const data = await api(`/api/nearest-clusters?record_id=${encodeURIComponent(recordId)}&limit=10`);
  const r = data.record || {};
  const near = data.nearest || [];
  openDrawer("Unmapped hotel", recordId, `
    <div class="member rep">
      <div class="m-top"><div class="m-name">${esc(r.name)}</div>${tag(r.provider, "prov")}</div>
      <div class="m-sub">
        <span>${esc(r.city || "—")}</span><span>${esc(r.postal_code || "—")}</span>
        <span>${r.lat != null ? `${Number(r.lat).toFixed(5)}, ${Number(r.lng).toFixed(5)}` : "no coords"}</span>
      </div>
    </div>
    <div class="section-label">${data.geo ? "Closest clusters" : "Clusters in the same city (no usable coords)"}</div>
    ${near.length ? near.map((n) => `
      <div class="near-row" data-cluster="${esc(n.cluster_id)}">
        <span class="nr-name" title="${esc(n.cluster_id)}">${esc(n.rep_name || n.cluster_id)}</span>
        ${n.entity_level === "building" ? tag("building", "building") : ""}
        ${tag(n.cluster_status === "auto_accept" ? "auto" : "review", n.cluster_status === "auto_accept" ? "auto" : "review")}
        ${tag(`${n.cluster_size} recs`)}
        ${n.same_city ? tag("same city", "prov") : ""}
        <span class="nr-dist">${n.dist_m != null ? km(n.dist_m) : "—"}</span>
      </div>`).join("") : empty("No clusters found nearby")}
  `);
  $$("#drawerBody .near-row[data-cluster]").forEach((row) =>
    row.addEventListener("click", () => openCluster(row.dataset.cluster)));
}

/* ---------------- reviews ---------------- */

async function renderReviews() {
  const el = $("#reviews");
  if (!el.dataset.ready) {
    const summary = state.reviewSummary;
    el.innerHTML = `
      <div class="grid cols-4">
        ${kpiCard({ label: "Total items", value: fmt(summary?.counts?.total_review_items), delta: `${fmt(summary?.counts?.cluster_review_items)} clusters`, deltaCls: "info" })}
        ${kpiCard({ label: "True-like pairs", value: fmt(summary?.counts?.true_like_contact_email_pairs), delta: "likely accepts", deltaCls: "up" })}
        ${kpiCard({ label: "Pair items", value: fmt(summary?.counts?.pair_review_items), delta: "training pool", deltaCls: "info" })}
        ${kpiCard({ label: "Cluster items", value: fmt(summary?.counts?.cluster_review_items), delta: "split or accept", deltaCls: "warn" })}
      </div>
      <div class="filterbar mt">
        <div class="field"><label>Bucket</label><select id="reviewBucket"><option value="">All</option></select></div>
        <div class="field"><label>Action</label><select id="reviewAction"><option value="">All</option><option>accept_pair_only</option><option>accept_cluster</option><option>split_cluster</option><option>provider_duplicate</option></select></div>
        <div class="field"><label>Search</label><input id="reviewSearch" class="wide" type="text" placeholder="name, city, review id"></div>
        <button class="btn primary" id="reviewApply">Apply</button>
      </div>
      <div id="reviewTable" class="card table-card">${loading()}</div>
    `;
    const buckets = [...new Set((summary?.bucket_counts || []).map((b) => b.review_bucket))].sort();
    $("#reviewBucket").insertAdjacentHTML("beforeend", buckets.map((b) => `<option>${esc(b)}</option>`).join(""));
    $("#reviewApply").addEventListener("click", () => { state.reviews.page = 1; loadReviews(); });
    ["reviewBucket", "reviewAction"].forEach((id) => $(`#${id}`).addEventListener("change", () => { state.reviews.page = 1; loadReviews(); }));
    $("#reviewSearch").addEventListener("keydown", (e) => { if (e.key === "Enter") { state.reviews.page = 1; loadReviews(); } });
    el.dataset.ready = "1";
  }
  await loadReviews();
}

async function loadReviews() {
  const qs = new URLSearchParams({
    bucket: $("#reviewBucket").value, action: $("#reviewAction").value,
    search: $("#reviewSearch").value, page: state.reviews.page, page_size: 50,
  });
  const data = await api(`/api/review-queue?${qs}`);
  if (!data.items?.length) { $("#reviewTable").innerHTML = empty("No review items"); return; }
  $("#reviewTable").innerHTML = `
    <div class="table-wrap"><table>
      <thead><tr><th class="num">Priority</th><th>Bucket</th><th>Action</th><th>Names</th><th>Cities</th><th>Best edge</th><th class="num">Geo</th></tr></thead>
      <tbody>${data.items.map((r) => `
        <tr class="clickable" data-review="${esc(r.review_id)}">
          <td class="num">${fmt(r.priority)}</td>
          <td>${tag(r.review_bucket, r.review_bucket?.includes("risk") ? "risk" : "review")}</td>
          <td>${tag(r.suggested_action)}</td>
          <td title="${esc(r.representative_names)}">${esc(trunc(r.representative_names, 64))}</td>
          <td title="${esc(r.cities)}">${esc(trunc(r.cities, 30))}</td>
          <td><span class="mono">${fmt(r.best_edge_probability, 3)}</span> ${tag(r.best_edge_reason)}</td>
          <td class="num">${km(r.geo_diameter_m)}</td>
        </tr>`).join("")}
      </tbody></table></div>
    ${pager(data, "reviews")}
  `;
  $$("#reviewTable tr.clickable").forEach((tr) => tr.addEventListener("click", () => openReview(tr.dataset.review)));
  bindPager("reviews", loadReviews);
}

/* ---------------- providers ---------------- */

async function renderProviders() {
  const el = $("#providers");
  if (!state.providers) state.providers = await api("/api/providers");
  const rows = state.providers.providers || [];
  el.innerHTML = `
    <div class="card table-card">
      <div class="table-wrap"><table>
        <thead><tr><th>Provider</th><th class="num">Records</th><th class="num">Name missing</th><th class="num">Empty core</th><th class="num">Invalid geo</th><th class="num">Phone</th><th class="num">Email</th><th class="num">Website</th></tr></thead>
        <tbody>${rows.map((r) => `
          <tr>
            <td>${tag(r.provider, "prov")}</td>
            <td class="num">${fmt(r.records)}</td>
            <td class="num">${pct(r.name_missing_rate, 2)}</td>
            <td class="num">${pct(r.empty_core_rate, 2)}</td>
            <td class="num">${pct(r.invalid_coord_rate, 2)}</td>
            <td class="num">${pct(r.phone_present_rate, 1)}</td>
            <td class="num">${pct(r.email_present_rate, 1)}</td>
            <td class="num">${pct(r.website_present_rate, 1)}</td>
          </tr>`).join("")}
        </tbody></table></div>
    </div>
  `;
}

/* ---------------- drawers / details ---------------- */

async function openCluster(clusterId) {
  return openClusterPage(clusterId);
}

async function openClusterPage(clusterId) {
  closeDrawer();
  state.clusterDetail = {
    id: clusterId, data: null,
    tab: state.clusterDetail?.tab, diffOnly: state.clusterDetail?.diffOnly,
  };
  setView("clusterDetail");
  $("#clusterDetail").innerHTML = loading();
  try {
    const data = await api(`/api/clusters/${encodeURIComponent(clusterId)}`);
    state.clusterDetail = { ...state.clusterDetail, id: clusterId, data };
    renderClusterDetailPage();
  } catch (err) {
    $("#clusterDetail").innerHTML = `
      ${empty(`Failed to load cluster: ${err.message}`)}
      <div style="text-align:center"><button class="btn" id="clusterBack">← Back to clusters</button></div>`;
    $("#clusterBack").addEventListener("click", () => setView("clusters"));
  }
}

function renderClusterDetailPage() {
  const el = $("#clusterDetail");
  const data = state.clusterDetail.data;
  if (!state.clusterDetail.id) {
    el.innerHTML = empty("Pick a cluster first");
    return;
  }
  if (!data) {
    el.innerHTML = loading();
    return;
  }
  const m = data.meta || {};
  const members = data.members || [];
  const edges = data.edges || [];
  const reasons = Array.isArray(m.review_reasons) ? m.review_reasons : [];
  const tab = state.clusterDetail.tab || "compare";
  el.innerHTML = `
    <div class="detail-toolbar">
      <button class="btn" id="clusterBack">← Back to clusters</button>
      <div class="detail-nav">
        <button class="btn small cluster-step" data-step="-1" ${canStepCluster(-1) ? "" : "disabled"}>← Prev cluster</button>
        <button class="btn small cluster-step" data-step="1" ${canStepCluster(1) ? "" : "disabled"}>Next cluster →</button>
      </div>
    </div>

    <div class="page-head detail-head">
      <div>
        <h2>${esc(m.representative_record_id || state.clusterDetail.id)}</h2>
        <div class="desc mono">${esc(state.clusterDetail.id)}</div>
      </div>
      <div class="head-meta">
        ${m.entity_level === "building" ? tag("building-level", "building") : ""}
        ${tag(m.cluster_status === "auto_accept" ? "auto" : "review", m.cluster_status === "auto_accept" ? "auto" : "review")}
        ${reasons.map((r) => tag(r, "risk")).join("")}
      </div>
    </div>

    <div class="meta-grid detail-meta">
      <div class="meta-item"><div class="ml">Records</div><div class="mv">${fmt(m.cluster_size || members.length)}</div></div>
      <div class="meta-item"><div class="ml">Providers</div><div class="mv">${fmt(m.provider_count || new Set(members.map((x) => x.provider)).size)}</div></div>
      <div class="meta-item"><div class="ml">Geo diameter</div><div class="mv">${km(m.max_geo_diameter_m)}</div></div>
      <div class="meta-item"><div class="ml">Min edge prob</div><div class="mv">${fmt(m.min_edge_probability, 3)}</div></div>
      <div class="meta-item"><div class="ml">Name share</div><div class="mv">${pct(m.dominant_name_signature_share, 1)}</div></div>
      <div class="meta-item"><div class="ml">Evidence</div><div class="mv">${evidence(m)}</div></div>
    </div>

    <div class="tabbar" id="detailTabs">
      <button class="tab-btn ${tab === "compare" ? "active" : ""}" data-tab="compare">Compare <span class="tab-count">${members.length}</span></button>
      <button class="tab-btn ${tab === "records" ? "active" : ""}" data-tab="records">Full records</button>
      <button class="tab-btn ${tab === "edges" ? "active" : ""}" data-tab="edges">Edges <span class="tab-count">${edges.length}</span></button>
    </div>

    <div id="detailTabBody">
      ${tab === "compare" ? compareTab(members) : ""}
      ${tab === "records" ? recordsTab(members) : ""}
      ${tab === "edges" ? edgesTab(edges) : ""}
    </div>
  `;
  $("#clusterBack").addEventListener("click", () => setView("clusters"));
  $$(".cluster-step").forEach((btn) =>
    btn.addEventListener("click", () => stepCluster(Number(btn.dataset.step))));
  $$("#detailTabs .tab-btn").forEach((b) => b.addEventListener("click", () => {
    state.clusterDetail.tab = b.dataset.tab;
    renderClusterDetailPage();
  }));
  const diffToggle = $("#diffOnly");
  if (diffToggle) diffToggle.addEventListener("change", () => {
    state.clusterDetail.diffOnly = diffToggle.checked;
    renderClusterDetailPage();
  });
}

/* --- compare matrix: fields as rows, provider records as columns --- */

const COMPARE_FIELDS = [
  ["Name (raw)", (x) => x.property_name],
  ["Name (normalized)", (x) => x.property_name_norm],
  ["Match signature", (x) => x.property_name_match_signature],
  ["Brand tokens", (x) => x.property_name_brand_tokens],
  ["City", (x) => x.city_name],
  ["City (norm)", (x) => x.city_name_norm],
  ["State (norm)", (x) => x.state_norm],
  ["Postal (final)", (x) => x.postal_code_final],
  ["Address", (x) => x.address_lines],
  ["Coords", (x) => (x.lat != null ? `${Number(x.lat).toFixed(5)}, ${Number(x.lng).toFixed(5)}` : null)],
  ["Dist from center", (x) => (x.dist_from_cluster_m != null ? km(x.dist_from_cluster_m) : null)],
  ["Star rating", (x) => x.star_rating],
  ["Type (norm)", (x) => x.property_type_norm],
  ["Hotel chain", (x) => x.hotel_chain],
  ["Phones (raw)", (x) => x.phone_numbers],
  ["Phones (E164)", (x) => x.phone_e164_list],
  ["Emails", (x) => x.emails],
  ["Domain", (x) => x.website_domain_norm],
  ["Website", (x) => x.weburl],
  ["Property code", (x) => x.property_code],
  ["Rooms", (x) => x.number_of_rooms],
  ["Updated", (x) => x.updated_at],
];

function compareTab(members) {
  if (!members.length) return empty("No members");
  const diffOnly = state.clusterDetail.diffOnly ?? (members.length > 2);
  state.clusterDetail.diffOnly = diffOnly;
  const rows = COMPARE_FIELDS.map(([label, get]) => {
    const values = members.map((x) => valueText(get(x)));
    const nonEmpty = values.filter((v) => v !== "—");
    const distinct = new Set(nonEmpty);
    const agree = distinct.size <= 1;
    // modal value = most frequent non-empty
    let modal = null, best = 0;
    distinct.forEach((v) => {
      const n = nonEmpty.filter((x) => x === v).length;
      if (n > best) { best = n; modal = v; }
    });
    return { label, values, agree, modal, hasData: nonEmpty.length > 0 };
  }).filter((r) => r.hasData);
  const visible = diffOnly ? rows.filter((r) => !r.agree) : rows;
  const agreeCount = rows.filter((r) => r.agree).length;
  return `
    <div class="cmp-controls">
      <label class="check"><input type="checkbox" id="diffOnly" ${diffOnly ? "checked" : ""}>
        Differences only</label>
      <span class="faint">${rows.length - agreeCount} fields differ · ${agreeCount} agree across all ${members.length} records</span>
    </div>
    <div class="card table-card">
      <div class="cmp-wrap">
        <table class="cmp">
          <thead><tr>
            <th class="cmp-field">Field</th>
            ${members.map((x) => `
              <th>
                <div class="cmp-prov">${tag(x.provider, "prov")} ${x.is_representative ? tag("rep", "auto") : ""}</div>
                <div class="mono faint cmp-rid" title="${esc(x.record_id)}">${esc(shortRecord(x.record_id))}</div>
              </th>`).join("")}
          </tr></thead>
          <tbody>
            ${visible.length ? visible.map((r) => `
              <tr>
                <td class="cmp-field"><span class="agree-dot ${r.agree ? "ok" : "warn"}"></span>${esc(r.label)}</td>
                ${r.values.map((v) => `
                  <td class="${v === "—" ? "faint" : (!r.agree && v !== r.modal ? "diff" : "")}"
                      title="${esc(v)}">${esc(trunc(v, 90))}</td>`).join("")}
              </tr>`).join("") : `<tr><td colspan="${members.length + 1}">${empty("All compared fields agree — toggle off ‘differences only’ to see them")}</td></tr>`}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function recordsTab(members) {
  return `
    <div class="provider-records">
      ${members.map((x, i) => `
        <details class="provider-fold ${x.is_representative ? "rep" : ""}" ${members.length === 1 ? "open" : ""}>
          <summary>
            <span class="pf-name">${esc(x.property_name || x.property_name_norm || x.record_id)}</span>
            <span class="pf-tags">
              ${tag(x.provider, "prov")}
              ${x.is_representative ? tag("representative", "auto") : ""}
              ${x.dist_from_cluster_m != null ? tag(km(x.dist_from_cluster_m)) : ""}
              <span class="pf-chevron">▾</span>
            </span>
          </summary>
          ${memberDetailCard(x)}
        </details>`).join("")}
    </div>
  `;
}

function edgesTab(edges) {
  if (!edges.length) return empty("No edges");
  return `
    <div class="card table-card">
      <div class="table-wrap"><table>
        <thead><tr><th>Providers</th><th>Records</th><th class="num">Prob</th><th>Reason</th><th class="num">Name J</th><th class="num">Between</th><th>Phone</th></tr></thead>
        <tbody>${edges.map((e) => `
          <tr>
            <td>${tag(e.provider_l, "prov")} ${tag(e.provider_r, "prov")}</td>
            <td class="mono faint">${esc(shortRecord(e.record_id_l))}<br>${esc(shortRecord(e.record_id_r))}</td>
            <td class="num mono">${fmt(e.match_probability, 3)}</td>
            <td>${tag(e.gate_reason)}</td>
            <td class="num">${fmt(e.name_match_jaccard, 2)}</td>
            <td class="num">${km(e.dist_m)}</td>
            <td>${e.phone_ov ? tag("yes", "auto") : tag("no")}</td>
          </tr>`).join("")}
        </tbody>
      </table></div>
    </div>
  `;
}

function clusterPosition() {
  const last = state.clusters.last;
  const rows = last?.clusters || [];
  return { last, rows, index: rows.findIndex((c) => c.cluster_id === state.clusterDetail.id) };
}

function canStepCluster(step) {
  const { last, rows, index } = clusterPosition();
  if (!last || index < 0) return false;
  if (rows[index + step]) return true;
  return step > 0 ? last.page < last.pages : last.page > 1;
}

async function stepCluster(step) {
  const { last, rows, index } = clusterPosition();
  if (!last || index < 0) return;
  const inPage = rows[index + step];
  if (inPage) {
    await openClusterPage(inPage.cluster_id);
    return;
  }
  if (!canStepCluster(step)) return;
  state.clusters.page = last.page + step;
  const data = await api(`/api/clusters?${clusterParams()}`);
  state.clusters.last = data;
  if ($("#clusterTable")) {
    $("#clusterTable").innerHTML = clusterTable(data);
    bindClusterTable();
  }
  const next = step > 0 ? data.clusters?.[0] : data.clusters?.[data.clusters.length - 1];
  if (next) await openClusterPage(next.cluster_id);
}

function shortRecord(recordId) {
  const s = String(recordId || "");
  const parts = s.split("::");
  return parts.length >= 2 ? `${parts[0]}::${parts[1]}` : s;
}

function valueText(value) {
  if (value === null || value === undefined || value === "") return "—";
  if (Array.isArray(value)) return value.length ? value.join(", ") : "—";
  if (typeof value === "boolean") return value ? "yes" : "no";
  return String(value);
}

function detailField(label, value, cls = "") {
  return `
    <div class="detail-field ${cls}">
      <div class="df-label">${esc(label)}</div>
      <div class="df-value">${esc(valueText(value))}</div>
    </div>`;
}

function memberDetailCard(x) {
  const rawFields = [
    ["Property code", x.property_code], ["Hotel ID", x.hotel_id], ["Source credential", x.source_credential],
    ["Source row", x.source_row_number], ["Original name", x.property_name], ["Address", x.address_lines],
    ["City", x.city_name], ["City code", x.city_code], ["State", x.state], ["Country code", x.country_code],
    ["Country", x.country_name], ["Postal", x.postal_code], ["Latitude", x.lat], ["Longitude", x.lng],
    ["Star rating", x.star_rating], ["Property type", x.property_type], ["Phone numbers", x.phone_numbers],
    ["Emails", x.emails], ["Fax", x.fax_numbers], ["Hotel chain", x.hotel_chain], ["Website", x.weburl],
    ["Landmark", x.land_mark], ["Area", x.area], ["Geohash", x.geohash6], ["Updated", x.updated_at],
    ["Check-in", x.check_in_time], ["Check-out", x.check_out_time], ["Rooms", x.number_of_rooms],
    ["Average rating", x.average_of_rating], ["Reviews", x.total_reviews], ["Thumbnail", x.thumbnail],
    ["Amenities", x.amenities, "wide"],
  ];
  const normalizedFields = [
    ["Normalized name", x.property_name_norm], ["Core name", x.property_name_core],
    ["Name signature", x.property_name_signature], ["Match signature", x.property_name_match_signature],
    ["Match tokens", x.property_name_match_tokens], ["Brand tokens", x.property_name_brand_tokens],
    ["Low-value tokens", x.property_name_low_value_tokens], ["Location tokens", x.property_name_location_tokens],
    ["Address norm", x.address_norm], ["City norm", x.city_name_norm], ["State norm", x.state_norm],
    ["Country norm", x.country_code_norm], ["Postal final", x.postal_code_final],
    ["Lat norm", x.lat_norm], ["Lng norm", x.lng_norm], ["Coord quality", x.coord_precision_bucket],
    ["H3 8", x.h3_8], ["H3 9", x.h3_9], ["Phone E164", x.phone_e164_list],
    ["Non-reused phones", x.phone_last10_non_reused_list], ["Phone reused", x.phone_reused_flag],
    ["Email norm", x.email_norm_list], ["Email generic", x.email_generic_flag],
    ["Email reused", x.email_reused_flag], ["Domain", x.website_domain_norm],
    ["Weak domain", x.website_weak_domain_flag], ["Domain reused", x.website_reused_flag],
    ["Type norm", x.property_type_norm], ["Star norm", x.star_rating_norm], ["Chain norm", x.hotel_chain_norm],
  ];
  return `
    <article class="provider-card ${x.is_representative ? "rep" : ""}">
      <div class="provider-head">
        <div>
          <div class="provider-name">${esc(x.property_name || x.property_name_norm || x.record_id)}</div>
          <div class="provider-sub mono">${esc(x.record_id)}</div>
        </div>
        <div class="provider-tags">
          ${tag(x.provider, "prov")}
          ${x.is_representative ? tag("representative", "auto") : ""}
          ${tag(`cluster ${km(x.dist_from_cluster_m)}`)}
        </div>
      </div>
      <div class="detail-columns">
        <div>
          <div class="section-label tight">Original provider fields</div>
          <div class="detail-grid">${rawFields.map(([label, value, cls]) => detailField(label, value, cls)).join("")}</div>
        </div>
        <div>
          <div class="section-label tight">Normalized matching fields</div>
          <div class="detail-grid">${normalizedFields.map(([label, value, cls]) => detailField(label, value, cls)).join("")}</div>
        </div>
      </div>
    </article>`;
}

async function openReview(reviewId) {
  openDrawer("Review item", reviewId, loading());
  const data = await api(`/api/review-queue/${encodeURIComponent(reviewId)}`);
  openDrawer("Review item", reviewId, detailHtml(data));
}

function detailHtml(data) {
  const m = data.meta || {};
  const members = data.members || [];
  const edges = data.edges || [];
  return `
    <div class="meta-grid">
      <div class="meta-item"><div class="ml">Size</div><div class="mv">${fmt(m.cluster_size)}</div></div>
      <div class="meta-item"><div class="ml">Providers</div><div class="mv">${fmt(m.provider_count || new Set(members.map((x) => x.provider)).size)}</div></div>
      <div class="meta-item"><div class="ml">Status</div><div class="mv">${esc(m.cluster_status || m.review_entity_type || "—")}</div></div>
    </div>
    <div class="chip-row" style="margin-bottom:6px">
      ${m.entity_level === "building" ? tag("building-level", "building") : ""}
      ${m.review_bucket ? tag(m.review_bucket, "review") : ""}
      ${m.suggested_action ? tag(m.suggested_action) : ""}
      ${(Array.isArray(m.review_reasons) ? m.review_reasons : []).map((r) => tag(r, "risk")).join("")}
    </div>
    <div class="section-label">Members</div>
    ${members.map((x) => `
      <div class="member ${x.is_representative ? "rep" : ""}">
        <div class="m-top"><div class="m-name">${esc(x.property_name || x.name)}</div>${tag(x.provider, "prov")}</div>
        <div class="m-sub">
          <span>${esc(x.city_name || x.city || "—")}</span>
          <span>${esc(x.postal_code || "—")}</span>
          <span class="rid">${esc(x.record_id)}</span>
        </div>
      </div>`).join("")}
    ${edges.length ? `
    <div class="section-label">Edges</div>
    <div class="table-wrap"><table>
      <thead><tr><th>Providers</th><th class="num">Prob</th><th>Reason</th><th class="num">Name J</th><th class="num">Geo</th><th>Phone</th></tr></thead>
      <tbody>${edges.slice(0, 60).map((e) => `
        <tr>
          <td>${tag(e.provider_l, "prov")} ${tag(e.provider_r, "prov")}</td>
          <td class="num mono">${fmt(e.match_probability, 3)}</td>
          <td>${tag(e.gate_reason)}</td>
          <td class="num">${fmt(e.name_match_jaccard, 2)}</td>
          <td class="num">${km(e.dist_m)}</td>
          <td>${e.phone_ov ? tag("yes", "auto") : tag("no")}</td>
        </tr>`).join("")}
      </tbody></table></div>` : ""}
  `;
}

function openDrawer(title, sub, body) {
  $("#drawerTitle").textContent = title;
  $("#drawerSub").textContent = sub;
  $("#drawerBody").innerHTML = body;
  $("#overlay").classList.add("open");
  $("#drawer").classList.add("open");
  $("#drawer").setAttribute("aria-hidden", "false");
}
function closeDrawer() {
  $("#overlay").classList.remove("open");
  $("#drawer").classList.remove("open");
  $("#drawer").setAttribute("aria-hidden", "true");
}

/* ---------------- pipeline ---------------- */

async function renderPipeline() {
  const el = $("#pipeline");
  if (!el.dataset.ready) {
    el.innerHTML = `
      <div class="filterbar">
        <div class="field"><label>Country</label><select id="pipeCountry"></select></div>
        <div class="field"><label>Source data</label>
          <select id="pipeSkipDownload">
            <option value="1">Use existing raw files</option>
            <option value="0">Download fresh from gateway</option>
          </select>
        </div>
        <button class="btn primary" id="pipeRun">Run pipeline</button>
      </div>
      <div class="card" id="pipeStatus">${loading()}</div>
      <div class="card table-card" style="margin-top:14px">
        <pre id="pipeLog" class="mono" style="margin:0;padding:14px;max-height:420px;overflow:auto;font-size:12px;line-height:1.5;white-space:pre-wrap"></pre>
      </div>
    `;
    state.pipeline.configs = await api("/api/pipeline/configs");
    const cfgs = state.pipeline.configs;
    $("#pipeCountry").innerHTML = cfgs.countries.map((c) =>
      `<option value="${esc(c.country)}">${esc(c.country)} (${esc(c.config)})${c.has_raw ? "" : " — no raw data"}</option>`).join("");
    if (!cfgs.download_key_present) {
      $("#pipeSkipDownload").querySelector('option[value="0"]').disabled = true;
      $("#pipeSkipDownload").title = "EMBEDDING_GATEWAY_API_KEY not set in server env";
    }
    $("#pipeRun").addEventListener("click", startPipeline);
    el.dataset.ready = "1";
  }
  await refreshPipeline();
}

async function startPipeline() {
  const country = $("#pipeCountry").value;
  const skip = $("#pipeSkipDownload").value;
  $("#pipeRun").disabled = true;
  try {
    await api(`/api/pipeline/start?country=${encodeURIComponent(country)}&skip_download=${skip}`);
  } catch (err) {
    $("#pipeStatus").innerHTML = `<div class="empty">${esc(err.message)}</div>`;
    $("#pipeRun").disabled = false;
    return;
  }
  await refreshPipeline();
}

async function refreshPipeline() {
  let s;
  try { s = await api("/api/pipeline/status"); }
  catch (err) { $("#pipeStatus").innerHTML = `<div class="empty">${esc(err.message)}</div>`; return; }
  renderPipelineStatus(s);
  clearTimeout(state.pipeline.timer);
  if (s.running) {
    const badge = $("#pipelineBadge");
    badge.textContent = "▶"; badge.classList.add("show");
    state.pipeline.timer = setTimeout(refreshPipeline, 2500);
  } else {
    $("#pipelineBadge").classList.remove("show");
  }
  if (state.view === "pipeline") $("#pipeRun").disabled = !!s.running;
}

function renderPipelineStatus(s) {
  const box = $("#pipeStatus");
  if (!box) return;
  if (!s.country) {
    box.innerHTML = `<div class="empty">No pipeline run yet this session — pick a country and hit Run.</div>`;
    $("#pipeLog").textContent = "";
    return;
  }
  const finished = !s.running && s.exit_code !== null && s.exit_code !== undefined;
  const ok = finished && s.exit_code === 0;
  const stateTag = s.running ? tag("running", "review")
    : ok ? tag("done", "auto") : finished ? tag(`failed (exit ${s.exit_code})`, "risk") : tag("—");
  box.innerHTML = `
    <div class="meta-grid">
      <div class="meta-item"><div class="ml">Country</div><div class="mv">${esc(s.country)}</div></div>
      <div class="meta-item"><div class="ml">Status</div><div class="mv">${stateTag}</div></div>
      <div class="meta-item"><div class="ml">Stage</div><div class="mv">${esc(s.stage || "starting…")}</div></div>
      <div class="meta-item"><div class="ml">Download</div><div class="mv">${s.skip_download ? "skipped" : "fresh"}</div></div>
    </div>
    ${ok && s.run_id ? `<div style="margin-top:10px"><button class="btn primary" id="pipeSwitch">Open run ${esc(s.run_id)} →</button></div>` : ""}
  `;
  $("#pipeLog").textContent = (s.log_tail || []).join("\n");
  const logEl = $("#pipeLog");
  logEl.scrollTop = logEl.scrollHeight;
  const sw = $("#pipeSwitch");
  if (sw) sw.addEventListener("click", async () => {
    await api(`/api/switch-run?run=${encodeURIComponent(s.run_id)}`);
    location.reload();
  });
}

/* ---------------- theme ---------------- */

function setTheme(theme) {
  document.body.classList.toggle("light", theme === "light");
  $$(".tt-btn").forEach((b) => b.classList.toggle("active", b.dataset.theme === theme));
  try { localStorage.setItem("hm-theme", theme); } catch { /* private mode */ }
}

/* ---------------- boot ---------------- */

async function boot() {
  $("#overview").innerHTML = loading();
  state.overview = await api("/api/overview");
  try { state.reviewSummary = await api("/api/review-summary"); } catch { state.reviewSummary = null; }
  const ov = state.overview;
  const c = ov.clustering || {};
  $("#pageSub").textContent = `${ov.run_id} · ${ov.country} · ${ov.version} · ${fmt(ov.total_records)} records`;
  try {
    const { runs } = await api("/api/runs");
    const sel = $("#runSwitch");
    sel.innerHTML = runs.map((r) => `
      <option value="${esc(r.run_id)}" ${r.current ? "selected" : ""}>
        ${esc(r.country || "?")} · ${esc(r.run_id)} · ${esc(r.version)}
      </option>`).join("");
    sel.addEventListener("change", async () => {
      sel.disabled = true;
      await api(`/api/switch-run?run=${encodeURIComponent(sel.value)}`);
      location.reload();
    });
  } catch { /* single-run fallback */ }
  const gp = $("#guardPill");
  gp.textContent = ov.guardrails_passed ? "guardrails pass" : "guardrails fail";
  gp.classList.add(ov.guardrails_passed ? "ok" : "warn");
  if (c.review) { const b = $("#reviewBadge"); b.textContent = compact(c.review); b.classList.add("show"); }
  if (c.singleton_unmatched) { const b = $("#unmappedBadge"); b.textContent = compact(c.singleton_unmatched); b.classList.add("show"); }
  await renderOverview();
}

$$(".nav-item").forEach((b) => b.addEventListener("click", () => setView(b.dataset.view)));
$$(".tt-btn").forEach((b) => b.addEventListener("click", () => setTheme(b.dataset.theme)));
$("#drawerClose").addEventListener("click", closeDrawer);
$("#overlay").addEventListener("click", closeDrawer);
document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeDrawer(); });

try { setTheme(localStorage.getItem("hm-theme") || "dark"); } catch { setTheme("dark"); }

boot().catch((err) => {
  $("#overview").innerHTML = `<div class="empty">Dashboard error: ${esc(err.message)}</div>`;
});
