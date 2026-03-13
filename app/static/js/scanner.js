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
let _viewMode  = localStorage.getItem("scanner_view") || "row";

// Track events the user has bookmarked (updated after modal save).
const _watchedEvents = new Set();

const _includeTags = new Map(); // slug → label  (events MUST have one of these)
const _excludeTags = new Map(); // slug → label  (events MUST NOT have any of these)
let   _includeTagReset;
let   _excludeTagReset;

const _evVol = { lo: 0, hi: 10_000_000 };
const _vol   = { lo: 0, hi: 10_000_000 };
const _liq   = { lo: 0, hi:  5_000_000 };

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

// Parse a number string that may contain K/M suffixes or commas.
function parseNum(s) {
  s = String(s || "").trim().replace(/[$,\s]/g, "");
  if (!s) return NaN;
  const m = s.match(/^([\d.]+)\s*([kmb]?)$/i);
  if (!m) return parseFloat(s);
  const n      = parseFloat(m[1]);
  const suffix = m[2].toLowerCase();
  if (suffix === "k") return n * 1_000;
  if (suffix === "m") return n * 1_000_000;
  if (suffix === "b") return n * 1_000_000_000;
  return n;
}

// Format a raw number for the idle numbox display (no $ prefix).
function fmtNumInput(v, max) {
  if (v <= 0)    return "0";
  if (v >= max)  return "Any";
  if (v >= 1_000_000) return (v / 1_000_000).toFixed(v % 1_000_000 === 0 ? 0 : 1) + "M";
  if (v >= 1_000)     return (v / 1_000).toFixed(v % 1_000 === 0 ? 0 : 0) + "K";
  return String(v);
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

// ── Hybrid range: number-input | slider | number-input ─────────────
// Replaces initDualRange for volume. No label span needed.
function initHybridRange(lowId, highId, fillId, loInputId, hiInputId, state) {
  const lowEl   = document.getElementById(lowId);
  const highEl  = document.getElementById(highId);
  const fillEl  = document.getElementById(fillId);
  const loInput = document.getElementById(loInputId);
  const hiInput = document.getElementById(hiInputId);
  const MAX     = parseFloat(lowEl.max);
  const MIN     = parseFloat(lowEl.min);

  function updateFill() {
    const lo = parseFloat(lowEl.value);
    const hi = parseFloat(highEl.value);
    const pLo = ((lo - MIN) / (MAX - MIN)) * 100;
    const pHi = ((hi - MIN) / (MAX - MIN)) * 100;
    fillEl.style.left  = pLo + "%";
    fillEl.style.width = (pHi - pLo) + "%";
  }

  function syncFromSliders() {
    const lo = parseFloat(lowEl.value);
    const hi = parseFloat(highEl.value);
    state.lo = lo;
    state.hi = hi;
    if (document.activeElement !== loInput) loInput.value = fmtNumInput(lo, MAX);
    if (document.activeElement !== hiInput) hiInput.value = fmtNumInput(hi, MAX);
    updateFill();
  }

  // Slider events
  lowEl.addEventListener("input", () => {
    if (parseFloat(lowEl.value) > parseFloat(highEl.value))
      lowEl.value = highEl.value;
    syncFromSliders();
  });
  highEl.addEventListener("input", () => {
    if (parseFloat(highEl.value) < parseFloat(lowEl.value))
      highEl.value = lowEl.value;
    syncFromSliders();
  });

  // Lo numbox: show raw number on focus, formatted on blur
  loInput.addEventListener("focus", () => {
    loInput.value = Math.round(parseFloat(lowEl.value));
    loInput.select();
  });
  loInput.addEventListener("blur", () => {
    const val     = parseNum(loInput.value);
    const clamped = isNaN(val) ? MIN : Math.max(MIN, Math.min(MAX, val));
    lowEl.value   = Math.min(clamped, parseFloat(highEl.value));
    syncFromSliders();
  });
  loInput.addEventListener("keydown", e => { if (e.key === "Enter") loInput.blur(); });

  // Hi numbox: empty = "Any" = MAX
  hiInput.addEventListener("focus", () => {
    const cur = parseFloat(highEl.value);
    hiInput.value = cur >= MAX ? "" : Math.round(cur);
    hiInput.select();
  });
  hiInput.addEventListener("blur", () => {
    const val = parseNum(hiInput.value);
    if (hiInput.value === "" || isNaN(val) || val >= MAX) {
      highEl.value = MAX;
    } else {
      const clamped = Math.max(parseFloat(lowEl.value), Math.min(MAX, val));
      highEl.value  = clamped;
    }
    syncFromSliders();
  });
  hiInput.addEventListener("keydown", e => { if (e.key === "Enter") hiInput.blur(); });

  // Init display
  syncFromSliders();

  return function reset() {
    lowEl.value  = MIN;
    highEl.value = MAX;
    syncFromSliders();
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

  // End date filter
  const endHas = document.querySelector('input[name="scanEndDateHas"]:checked');
  if (endHas && endHas.value !== "all") params.set("end_date_filter", endHas.value);
  const endAfter  = (document.getElementById("scanEndAfter")?.value  || "").trim();
  const endBefore = (document.getElementById("scanEndBefore")?.value || "").trim();
  if (endAfter)  params.set("end_date_after",  endAfter);
  if (endBefore) params.set("end_date_before", endBefore);

  // Event-level volume filter
  const evVolMax = parseFloat(document.getElementById("evVolHigh").max);
  if (_evVol.lo > 0)         params.set("min_event_vol", _evVol.lo);
  if (_evVol.hi < evVolMax)  params.set("max_event_vol", _evVol.hi);

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

  // Market count filter — mutually exclusive checkboxes
  if (document.getElementById("minMarkets4")?.checked)      params.set("min_markets", 4);
  else if (document.getElementById("minMarkets3")?.checked) params.set("min_markets", 3);

  params.set("sort_by",   document.getElementById("scanSortBy").value);
  params.set("sort_dir",  document.getElementById("scanSortDir").value);
  params.set("page",      _page);
  params.set("page_size", 25);

  return params.toString();
}

// ── View toggle ────────────────────────────────────────────────────

function setViewMode(mode) {
  _viewMode = mode;
  try { localStorage.setItem("scanner_view", mode); } catch {}
  document.getElementById("scannerEvents")
    ?.classList.toggle("scanner-events--grid", mode === "grid");
  document.getElementById("viewRow") ?.classList.toggle("view-toggle-btn--active", mode === "row");
  document.getElementById("viewGrid")?.classList.toggle("view-toggle-btn--active", mode === "grid");
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

const _BOOKMARK_EMPTY = `<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 2h10a1 1 0 0 1 1 1v11l-6-3-6 3V3a1 1 0 0 1 1-1z"/></svg>`;
const _BOOKMARK_FILLED = `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 2h10a1 1 0 0 1 1 1v11l-6-3-6 3V3a1 1 0 0 1 1-1z"/></svg>`;

function renderEventCard(ev) {
  // Sort markets by YES ask price descending so most-probable outcome is first.
  const markets = (ev.markets || []).slice().sort((a, b) => {
    let pa = 0, pb = 0;
    try { pa = parseFloat(JSON.parse(a.outcomePrices || "[]")[0]) || 0; } catch {}
    try { pb = parseFloat(JSON.parse(b.outcomePrices || "[]")[0]) || 0; } catch {}
    return pb - pa;
  });
  const evId    = ev.id || "";
  const title   = escHtml(truncate(ev.title || "(no title)", 140));
  const rawTitle = ev.title || "(no title)";
  const isSaved = _watchedEvents.has(String(evId));

  const tagBadges = (ev.tags || []).slice(0, 4)
    .map(t => `<span class="category-badge">${escHtml(t.label || t.slug || "")}</span>`)
    .join("");

  const vol24    = fmtUSDC(ev.volume24hr);
  const volTotal = fmtUSDC(ev.volume);
  const endDate  = fmtDate(ev.endDate);
  const sbadge   = statusBadge(ev);

  const marketCardsHtml = markets.map(renderMarketCard).join("");

  return `
    <div class="event-card" data-ev-id="${escHtml(String(evId))}" data-ev-title="${escHtml(rawTitle)}">
      <div class="event-card-header">
        <div class="event-card-title-wrap">
          <div class="event-card-title">${title}</div>
          <button class="bookmark-btn${isSaved ? " bookmark-btn--saved" : ""}" title="Add to watchlist" aria-label="Bookmark event">
            ${isSaved ? _BOOKMARK_FILLED : _BOOKMARK_EMPTY}
          </button>
        </div>
        ${tagBadges ? `<div class="event-card-badges">${tagBadges}</div>` : ""}
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

  // Wire bookmark buttons
  container.querySelectorAll(".bookmark-btn").forEach(btn => {
    btn.addEventListener("click", e => {
      e.stopPropagation();
      const card   = btn.closest(".event-card");
      const evId   = card?.dataset.evId   || "";
      const evTitle = card?.dataset.evTitle || "";
      if (!evId || !window.WL) return;
      window.WL.showAddModal("event", evId, evTitle, () => {
        _watchedEvents.add(String(evId));
        btn.classList.add("bookmark-btn--saved");
        btn.innerHTML = _BOOKMARK_FILLED;
      });
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

let _resetEvVol, _resetVol, _resetLiq;

function resetFilters() {
  document.getElementById("scanQ").value = "";

  const allEv = document.querySelector('input[name="scanEventStatus"][value="active"]');
  if (allEv) allEv.checked = true;

  const allMkt = document.querySelector('input[name="scanMktStatus"][value="all"]');
  if (allMkt) allMkt.checked = true;

  document.getElementById("scanMaxSpread").value = "";

  const cb3 = document.getElementById("minMarkets3");
  const cb4 = document.getElementById("minMarkets4");
  if (cb3) cb3.checked = false;
  if (cb4) cb4.checked = false;

  const allEnd = document.querySelector('input[name="scanEndDateHas"][value="all"]');
  if (allEnd) allEnd.checked = true;
  const endRangeWrap = document.getElementById("endDateRangeWrap");
  if (endRangeWrap) endRangeWrap.style.display = "";
  const scanEndAfter  = document.getElementById("scanEndAfter");
  const scanEndBefore = document.getElementById("scanEndBefore");
  if (scanEndAfter)  scanEndAfter.value  = "";
  if (scanEndBefore) scanEndBefore.value = "";

  if (_includeTagReset) _includeTagReset();
  if (_excludeTagReset) _excludeTagReset();
  if (_resetEvVol) _resetEvVol();
  if (_resetVol)   _resetVol();
  if (_resetLiq)   _resetLiq();

  document.getElementById("scanSortBy").value  = "volume";
  document.getElementById("scanSortDir").value = "desc";
}

// ── Init ──────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  _resetEvVol      = initHybridRange("evVolLow", "evVolHigh", "evVolFill", "evVolLoInput", "evVolHiInput", _evVol);
  _resetVol        = initHybridRange("volLow", "volHigh", "volFill", "volLoInput", "volHiInput", _vol);
  _resetLiq        = initDualRange("liqLow", "liqHigh", "liqFill", "liqRangeLabel", _liq);

  // Market-count checkboxes are mutually exclusive (checking one unchecks the other)
  const cb3 = document.getElementById("minMarkets3");
  const cb4 = document.getElementById("minMarkets4");
  cb3?.addEventListener("change", () => { if (cb3.checked && cb4) cb4.checked = false; });
  cb4?.addEventListener("change", () => { if (cb4.checked && cb3) cb3.checked = false; });
  _includeTagReset = initTagInput("includeTagInput", "includeTagDropdown", "includeTagPills", _includeTags, "");
  _excludeTagReset = initTagInput("excludeTagInput", "excludeTagDropdown", "excludeTagPills", _excludeTags, "tag-pill--exclude");

  // Silently load full tag list from server (replaces preset fallback).
  // The dropdown re-reads _allTags on every open so no re-init needed.
  loadTagsFromAPI();

  // View toggle
  document.getElementById("viewRow") ?.addEventListener("click", () => setViewMode("row"));
  document.getElementById("viewGrid")?.addEventListener("click", () => setViewMode("grid"));
  setViewMode(_viewMode); // apply persisted preference immediately

  // End date radio — hide date range pickers when "No end date" is selected
  document.querySelectorAll('input[name="scanEndDateHas"]').forEach(radio => {
    radio.addEventListener("change", () => {
      const wrap = document.getElementById("endDateRangeWrap");
      if (wrap) wrap.style.display = radio.value === "none" ? "none" : "";
    });
  });

  document.getElementById("scanBtn").addEventListener("click",       () => doScan(true));
  document.getElementById("scanResetBtn").addEventListener("click",  resetFilters);
  document.getElementById("scanCancelBtn").addEventListener("click", () => {
    if (_abortCtrl) _abortCtrl.abort();
  });

  document.getElementById("scanQ").addEventListener("keydown", e => {
    if (e.key === "Enter") doScan(true);
  });
});
