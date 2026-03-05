/* scanner.js — Market Scanner page */

"use strict";

// ── Tag data ──────────────────────────────────────────────────────
// Fallback preset list shown instantly before the API responds.
// Replaced by the full live list once /api/scanner/tags loads.
const _PRESET_TAGS_FALLBACK = [
  { slug: "politics",             label: "Politics" },
  { slug: "elections",            label: "Elections" },
  { slug: "us-politics",          label: "US Politics" },
  { slug: "economy",              label: "Economy" },
  { slug: "crypto",               label: "Crypto" },
  { slug: "geopolitics",          label: "Geopolitics" },
  { slug: "iran",                 label: "Iran" },
  { slug: "ukraine",              label: "Ukraine" },
  { slug: "china",                label: "China" },
  { slug: "russia",               label: "Russia" },
  { slug: "big-tech",             label: "Big Tech" },
  { slug: "ai",                   label: "AI" },
  { slug: "sports",               label: "Sports" },
  { slug: "nfl",                  label: "NFL" },
  { slug: "nba",                  label: "NBA" },
  { slug: "soccer",               label: "Soccer" },
  { slug: "finance",              label: "Finance" },
  { slug: "nfts",                 label: "NFTs" },
  { slug: "token-launch",         label: "Token Launch" },
  { slug: "pre-market",           label: "Pre-Market" },
  { slug: "us-current-affairs",   label: "US Current Affairs" },
  { slug: "science",              label: "Science" },
  { slug: "entertainment",        label: "Entertainment" },
  { slug: "pop-culture",          label: "Pop Culture" },
  { slug: "climate",              label: "Climate" },
  { slug: "middle-east",          label: "Middle East" },
  { slug: "tariffs",              label: "Tariffs" },
  { slug: "technology",           label: "Technology" },
];

// Live tag list — starts as a copy of the fallback, silently upgraded
// by loadTagsFromAPI() once the server responds.
let _allTags = [..._PRESET_TAGS_FALLBACK];

async function loadTagsFromAPI() {
  try {
    const resp = await fetch("/api/scanner/tags");
    if (!resp.ok) return;
    const tags = await resp.json();
    if (Array.isArray(tags) && tags.length > 0) {
      _allTags = tags; // drop-in replacement; dropdown reads _allTags fresh on each open
    }
  } catch {
    // Silently keep the fallback list
  }
}

// ── State ──────────────────────────────────────────────────────────
let _page      = 1;
let _pages     = 1;
let _abortCtrl = null;

const _includeTags = new Map(); // slug → label  (events MUST have one of these)
const _excludeTags = new Map(); // slug → label  (events MUST NOT have any of these)
let   _includeTagReset;
let   _excludeTagReset;

const _vol = { lo: 0, hi: 10_000_000 };
const _liq = { lo: 0, hi:  5_000_000 };

// ── Formatters ─────────────────────────────────────────────────────

function fmtUSDC(raw) {
  const v = parseFloat(raw) || 0;
  if (v >= 1_000_000) return "$" + (v / 1_000_000).toFixed(1) + "M";
  if (v >= 1_000)     return "$" + (v / 1_000).toFixed(0) + "K";
  return "$" + v.toFixed(0);
}

function fmtPrice(raw) {
  const v = parseFloat(raw);
  if (isNaN(v)) return "—";
  return Math.round(v * 100) + "¢";
}

function fmtDate(iso) {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return "—";
    if (d.getTime() < Date.now()) return "Expired";
    return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
  } catch {
    return "—";
  }
}

function fmtRangeLabel(lo, hi, max) {
  const loIsMin = lo <= 0;
  const hiIsMax = hi >= max;
  if (loIsMin && hiIsMax) return "Any";
  if (loIsMin) return "≤ " + fmtUSDC(hi);
  if (hiIsMax) return "≥ " + fmtUSDC(lo);
  return fmtUSDC(lo) + " – " + fmtUSDC(hi);
}

function statusBadge(m) {
  if (m.resolved) return '<span class="status-badge status-badge--resolved">Resolved</span>';
  if (m.closed)   return '<span class="status-badge status-badge--closed">Closed</span>';
  if (m.active)   return '<span class="status-badge status-badge--active">Active</span>';
  return '<span class="status-badge status-badge--closed">Inactive</span>';
}

function escHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function truncate(s, max) {
  return s && s.length > max ? s.slice(0, max) + "…" : (s || "");
}

// ── Spread helpers ─────────────────────────────────────────────────

function calcSpread(prices) {
  if (!prices || !prices.length) return null;
  const sum = prices.reduce((acc, p) => acc + (parseFloat(p) || 0), 0);
  return (1.0 - sum) * 100;
}

function spreadBadge(spread) {
  if (spread === null || isNaN(spread) || spread < 0) return "";
  const pct = spread.toFixed(1);
  let cls = "spread-badge--tight";
  if (spread > 5)      cls = "spread-badge--wide";
  else if (spread > 2) cls = "spread-badge--med";
  return `<span class="spread-badge ${cls}">Spread ${pct}%</span>`;
}

// ── Dual-range slider ──────────────────────────────────────────────

function initDualRange(lowId, highId, fillId, labelId, state) {
  const lowEl   = document.getElementById(lowId);
  const highEl  = document.getElementById(highId);
  const fillEl  = document.getElementById(fillId);
  const labelEl = document.getElementById(labelId);

  function update() {
    let lo = parseFloat(lowEl.value);
    let hi = parseFloat(highEl.value);
    if (lo > hi) {
      if (document.activeElement === lowEl) lo = hi;
      else hi = lo;
      lowEl.value  = lo;
      highEl.value = hi;
    }
    state.lo = lo;
    state.hi = hi;

    const min  = parseFloat(lowEl.min);
    const max  = parseFloat(lowEl.max);
    const pLo  = ((lo - min) / (max - min)) * 100;
    const pHi  = ((hi - min) / (max - min)) * 100;
    fillEl.style.left  = pLo + "%";
    fillEl.style.width = (pHi - pLo) + "%";
    labelEl.textContent = fmtRangeLabel(lo, hi, max);
  }

  lowEl.addEventListener("input", update);
  highEl.addEventListener("input", update);
  update();

  return function reset() {
    lowEl.value  = lowEl.min;
    highEl.value = highEl.max;
    update();
  };
}

// ── Tag combobox ───────────────────────────────────────────────────

/**
 * Initialise a tag combobox.
 *
 * @param {string} inputId     - id of the <input> element
 * @param {string} dropdownId  - id of the dropdown <div>
 * @param {string} pillsId     - id of the pills container <div>
 * @param {Map}    selectedMap - the Map<slug,label> to read/write
 * @param {string} pillClass   - extra CSS class for pills (e.g. "tag-pill--exclude")
 * @returns {function} reset() - clears the map and re-renders
 */
function initTagInput(inputId, dropdownId, pillsId, selectedMap, pillClass) {
  const input    = document.getElementById(inputId);
  const dropdown = document.getElementById(dropdownId);
  const pillsEl  = document.getElementById(pillsId);

  function renderDropdown(query) {
    const q = (query || "").toLowerCase().trim();
    const matches = _allTags.filter(t =>
      !selectedMap.has(t.slug) &&
      (!q || t.label.toLowerCase().includes(q) || t.slug.toLowerCase().includes(q))
    );

    let html = "";
    matches.slice(0, 12).forEach(t => {
      html += `<div class="tag-dropdown-item" data-slug="${escHtml(t.slug)}" data-label="${escHtml(t.label)}">${escHtml(t.label)}</div>`;
    });

    if (q) {
      const exactMatch = _allTags.some(
        t => t.label.toLowerCase() === q || t.slug.toLowerCase() === q
      );
      if (!exactMatch && !selectedMap.has(q)) {
        const displayQ = query.trim();
        html += `<div class="tag-dropdown-item tag-dropdown-item--custom" data-slug="${escHtml(q)}" data-label="${escHtml(displayQ)}">Add &ldquo;${escHtml(displayQ)}&rdquo;</div>`;
      }
    }

    dropdown.innerHTML = html;
    dropdown.style.display = html ? "" : "none";

    dropdown.querySelectorAll(".tag-dropdown-item").forEach(item => {
      item.addEventListener("mousedown", e => {
        e.preventDefault();
        addTag(item.dataset.slug, item.dataset.label);
        input.value = "";
        dropdown.style.display = "none";
      });
    });
  }

  function addTag(slug, label) {
    if (selectedMap.has(slug)) return;
    selectedMap.set(slug, label);
    renderPills();
  }

  function removeTag(slug) {
    selectedMap.delete(slug);
    renderPills();
  }

  function renderPills() {
    pillsEl.innerHTML = "";
    selectedMap.forEach((label, slug) => {
      const pill = document.createElement("span");
      pill.className = pillClass ? `tag-pill ${pillClass}` : "tag-pill";
      pill.innerHTML = `${escHtml(label)}<button class="tag-pill-remove" data-slug="${escHtml(slug)}" aria-label="Remove tag">&times;</button>`;
      pill.querySelector(".tag-pill-remove").addEventListener("click", e => {
        e.stopPropagation();
        removeTag(slug);
      });
      pillsEl.appendChild(pill);
    });
  }

  input.addEventListener("focus", () => renderDropdown(input.value));
  input.addEventListener("input", () => renderDropdown(input.value));
  input.addEventListener("blur",  () => {
    setTimeout(() => { dropdown.style.display = "none"; }, 150);
  });
  input.addEventListener("keydown", e => {
    if (e.key === "Enter" && input.value.trim()) {
      e.preventDefault();
      const raw   = input.value.trim();
      const match = _allTags.find(t => t.label.toLowerCase() === raw.toLowerCase());
      if (match) {
        addTag(match.slug, match.label);
      } else {
        addTag(raw.toLowerCase().replace(/\s+/g, "-"), raw);
      }
      input.value = "";
      dropdown.style.display = "none";
    } else if (e.key === "Escape") {
      dropdown.style.display = "none";
    }
  });

  return function reset() {
    selectedMap.clear();
    renderPills();
    input.value = "";
    dropdown.style.display = "none";
  };
}

// ── Query builder ──────────────────────────────────────────────────

function buildQueryString() {
  const params = new URLSearchParams();

  const q = document.getElementById("scanQ").value.trim();
  if (q) params.set("q", q);

  // Event-level filters
  const evStatus = document.querySelector('input[name="scanEventStatus"]:checked');
  if (evStatus && evStatus.value !== "all") params.set("event_status", evStatus.value);

  if (_includeTags.size) params.set("tags",         [..._includeTags.keys()].join(","));
  if (_excludeTags.size) params.set("exclude_tags",  [..._excludeTags.keys()].join(","));

  // Market-level filters
  const mktStatus = document.querySelector('input[name="scanMktStatus"]:checked');
  if (mktStatus && mktStatus.value !== "all") params.set("market_status", mktStatus.value);

  const volMax = parseFloat(document.getElementById("volHigh").max);
  if (_vol.lo > 0)      params.set("min_market_vol", _vol.lo);
  if (_vol.hi < volMax) params.set("max_market_vol", _vol.hi);

  const liqMax = parseFloat(document.getElementById("liqHigh").max);
  if (_liq.lo > 0)      params.set("min_market_liq", _liq.lo);
  if (_liq.hi < liqMax) params.set("max_market_liq", _liq.hi);

  const spread = (document.getElementById("scanMaxSpread").value || "").trim();
  if (spread) params.set("max_spread", spread);

  params.set("sort_by",   document.getElementById("scanSortBy").value);
  params.set("sort_dir",  document.getElementById("scanSortDir").value);
  params.set("page",      _page);
  params.set("page_size", 25);

  return params.toString();
}

// ── Skeleton ───────────────────────────────────────────────────────

function showSkeleton() {
  const container = document.getElementById("scannerEvents");
  let html = "";
  for (let i = 0; i < 3; i++) {
    html += `
      <div class="event-card event-card--skeleton">
        <div class="event-card-header">
          <div class="event-card-title-row">
            <div class="skeleton-line" style="width:55%;height:14px"></div>
            <div class="skeleton-line" style="width:20%;height:18px;border-radius:9px"></div>
          </div>
          <div class="skeleton-line" style="width:35%;height:10px;margin-top:8px"></div>
        </div>
        <div class="event-markets">
          <div class="market-card">
            <div class="skeleton-line" style="width:78%"></div>
            <div style="display:flex;gap:6px;margin-top:8px">
              <div class="skeleton-line" style="width:60px;height:20px;border-radius:10px"></div>
              <div class="skeleton-line" style="width:60px;height:20px;border-radius:10px"></div>
              <div class="skeleton-line" style="width:70px;height:20px;border-radius:10px"></div>
            </div>
            <div class="skeleton-line" style="width:40%;margin-top:8px;height:10px"></div>
          </div>
          <div class="market-card">
            <div class="skeleton-line" style="width:65%"></div>
            <div style="display:flex;gap:6px;margin-top:8px">
              <div class="skeleton-line" style="width:60px;height:20px;border-radius:10px"></div>
              <div class="skeleton-line" style="width:60px;height:20px;border-radius:10px"></div>
            </div>
            <div class="skeleton-line" style="width:35%;margin-top:8px;height:10px"></div>
          </div>
        </div>
      </div>`;
  }
  container.innerHTML = html;
  document.getElementById("scanResultCount").textContent = "Loading…";
  document.getElementById("scanCacheInfo").textContent   = "";
  document.getElementById("scannerPagination").innerHTML = "";
}

// ── Card rendering ─────────────────────────────────────────────────

function renderMarketCard(m) {
  let outcomes = [], prices = [];
  try { outcomes = JSON.parse(m.outcomes      || "[]"); } catch {}
  try { prices   = JSON.parse(m.outcomePrices || "[]"); } catch {}

  const cid    = escHtml(m.conditionId || "");
  const spread = calcSpread(prices);
  const vol    = fmtUSDC(m.volume);
  const liq    = fmtUSDC(m.liquidity);
  const sbadge = statusBadge(m);

  let pricesHtml;
  if (outcomes.length === 2) {
    pricesHtml =
      `<span class="market-price market-price--yes">${escHtml(outcomes[0])} ${fmtPrice(prices[0])}</span>` +
      `<span class="market-price market-price--no">${escHtml(outcomes[1])} ${fmtPrice(prices[1])}</span>` +
      spreadBadge(spread);
  } else {
    pricesHtml = outcomes.map((o, i) =>
      `<span class="market-price">${escHtml(o)} ${fmtPrice(prices[i])}</span>`
    ).join("") + spreadBadge(spread);
  }

  // Question — prefer market question; fall back to "(no question)"
  const question = escHtml(truncate(m.question || "", 120));

  return `
    <div class="market-card" data-cid="${cid}">
      <div class="market-card-question">${question}</div>
      <div class="market-card-prices">${pricesHtml}</div>
      <div class="market-card-meta">
        <span class="dim">Vol ${vol}</span>
        <span class="dim">Liq ${liq}</span>
        ${sbadge}
      </div>
    </div>`;
}

function renderEventCard(ev) {
  const markets = ev.markets || [];
  const title   = escHtml(truncate(ev.title || "(no title)", 140));

  const tagBadges = (ev.tags || []).slice(0, 4)
    .map(t => `<span class="category-badge">${escHtml(t.label || t.slug || "")}</span>`)
    .join("");

  const vol24    = fmtUSDC(ev.volume24hr);
  const volTotal = fmtUSDC(ev.volume);
  const endDate  = fmtDate(ev.endDate);
  const sbadge   = statusBadge(ev);

  const marketCardsHtml = markets.map(renderMarketCard).join("");

  return `
    <div class="event-card">
      <div class="event-card-header">
        <div class="event-card-title-row">
          <div class="event-card-title">${title}</div>
          <div class="event-card-badges">${tagBadges}</div>
        </div>
        <div class="event-card-meta">
          <span>24h <b>${vol24}</b></span>
          <span>Total <b>${volTotal}</b></span>
          <span>${endDate}</span>
          ${sbadge}
        </div>
      </div>
      <div class="event-markets">
        ${marketCardsHtml || '<div class="event-no-markets">No individual markets matched the filters.</div>'}
      </div>
    </div>`;
}

// ── Pagination ─────────────────────────────────────────────────────

function renderPagination(page, pages) {
  const wrap = document.getElementById("scannerPagination");
  if (pages <= 1) { wrap.innerHTML = ""; return; }

  let html = "";
  html += `<button class="page-btn" ${page <= 1 ? "disabled" : ""} data-p="${page - 1}">‹</button>`;

  const range  = new Set([1, pages, page - 1, page, page + 1].filter(p => p >= 1 && p <= pages));
  const sorted = [...range].sort((a, b) => a - b);
  let prev = 0;
  sorted.forEach(p => {
    if (p - prev > 1) html += `<button class="page-btn" disabled>…</button>`;
    html += `<button class="page-btn${p === page ? " page-btn--active" : ""}" data-p="${p}">${p}</button>`;
    prev = p;
  });

  html += `<button class="page-btn" ${page >= pages ? "disabled" : ""} data-p="${page + 1}">›</button>`;

  wrap.innerHTML = html;
  wrap.querySelectorAll(".page-btn[data-p]").forEach(btn => {
    btn.addEventListener("click", () => {
      _page = parseInt(btn.dataset.p, 10);
      doScan(false);
    });
  });
}

// ── Results rendering ──────────────────────────────────────────────

function renderResults(data) {
  const container = document.getElementById("scannerEvents");
  container.innerHTML = "";

  const events = data.markets || []; // API uses "markets" key for backward compat
  if (!events.length) {
    container.innerHTML = '<div class="scanner-no-results">No events match the current filters.</div>';
    document.getElementById("scanResultCount").textContent = "No results";
    document.getElementById("scanCacheInfo").textContent   = "";
    document.getElementById("scannerPagination").innerHTML = "";
    return;
  }

  events.forEach(ev => container.insertAdjacentHTML("beforeend", renderEventCard(ev)));

  // Wire market card clicks → load market in Live Trades
  container.querySelectorAll(".market-card[data-cid]").forEach(card => {
    card.addEventListener("click", () => {
      const cid = card.dataset.cid;
      if (cid) window.location.href = "/?market_id=" + encodeURIComponent(cid);
    });
  });

  const total = data.total || 0;
  const pg    = data.page  || 1;
  const pgs   = data.pages || 1;
  document.getElementById("scanResultCount").textContent =
    total + " event" + (total !== 1 ? "s" : "") + " found" +
    (pgs > 1 ? ` — page ${pg} of ${pgs}` : "");

  const cacheEl = document.getElementById("scanCacheInfo");
  cacheEl.textContent = data.cached && data.cache_age_s !== undefined
    ? "(cached " + data.cache_age_s + "s ago)"
    : "(fresh)";

  _pages = pgs;
  renderPagination(pg, pgs);
}

// ── Main scan ─────────────────────────────────────────────────────

async function doScan(resetPage = true) {
  if (resetPage) _page = 1;

  if (_abortCtrl) { _abortCtrl.abort(); _abortCtrl = null; }
  _abortCtrl = new AbortController();

  document.getElementById("scannerEmpty").style.display  = "none";
  document.getElementById("scannerPanel").style.display  = "";
  document.getElementById("scanCancelBtn").style.display = "";
  showSkeleton();

  try {
    const resp = await fetch("/api/scanner?" + buildQueryString(), {
      signal: _abortCtrl.signal,
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.error || "Server error " + resp.status);
    }

    renderResults(await resp.json());

  } catch (e) {
    const container = document.getElementById("scannerEvents");
    if (e.name === "AbortError") {
      container.innerHTML = '<div class="scanner-no-results">Scan cancelled.</div>';
      document.getElementById("scanResultCount").textContent = "";
    } else {
      container.innerHTML = `<div class="scanner-no-results">Error: ${escHtml(e.message)}</div>`;
      document.getElementById("scanResultCount").textContent = "";
      if (typeof toast === "function") toast(e.message, "error");
    }
    document.getElementById("scannerPagination").innerHTML = "";
  } finally {
    document.getElementById("scanCancelBtn").style.display = "none";
    _abortCtrl = null;
  }
}

// ── Reset ─────────────────────────────────────────────────────────

let _resetVol, _resetLiq;

function resetFilters() {
  document.getElementById("scanQ").value = "";

  const allEv = document.querySelector('input[name="scanEventStatus"][value="all"]');
  if (allEv) allEv.checked = true;

  const allMkt = document.querySelector('input[name="scanMktStatus"][value="all"]');
  if (allMkt) allMkt.checked = true;

  document.getElementById("scanMaxSpread").value = "";

  if (_includeTagReset) _includeTagReset();
  if (_excludeTagReset) _excludeTagReset();
  if (_resetVol) _resetVol();
  if (_resetLiq) _resetLiq();

  document.getElementById("scanSortBy").value  = "volume";
  document.getElementById("scanSortDir").value = "desc";
}

// ── Init ──────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  _resetVol        = initDualRange("volLow", "volHigh", "volFill", "volRangeLabel", _vol);
  _resetLiq        = initDualRange("liqLow", "liqHigh", "liqFill", "liqRangeLabel", _liq);
  _includeTagReset = initTagInput("includeTagInput", "includeTagDropdown", "includeTagPills", _includeTags, "");
  _excludeTagReset = initTagInput("excludeTagInput", "excludeTagDropdown", "excludeTagPills", _excludeTags, "tag-pill--exclude");

  // Silently load full tag list from server (replaces preset fallback).
  // The dropdown re-reads _allTags on every open so no re-init needed.
  loadTagsFromAPI();

  document.getElementById("scanBtn").addEventListener("click",       () => doScan(true));
  document.getElementById("scanResetBtn").addEventListener("click",  resetFilters);
  document.getElementById("scanCancelBtn").addEventListener("click", () => {
    if (_abortCtrl) _abortCtrl.abort();
  });

  document.getElementById("scanQ").addEventListener("keydown", e => {
    if (e.key === "Enter") doScan(true);
  });
});
