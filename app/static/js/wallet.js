/* ================================================================
   wallet.js — Wallet Analytics Dashboard
   Fetches /api/wallet/<address> and renders profile, positions,
   and recent trades.  No framework — vanilla JS only.
================================================================ */

"use strict";

// ----------------------------------------------------------------
// Config (injected by Flask)
// ----------------------------------------------------------------
const CFG = window.APP_CONFIG || { whaleThreshold: 1000 };
const INITIAL_ADDRESS = window.INITIAL_ADDRESS || "";
const REFRESH_MS = 60_000;

// ----------------------------------------------------------------
// State
// ----------------------------------------------------------------
let _currentAddress = "";
let _marketsMap     = {};   // condition_id → title
let _refreshTimer   = null;
let _autoRefresh    = false;

// Chart state
let _pnlHistory   = [];   // full sorted dataset [{ts, pnl, cum_pnl}]
let _pnlChart     = null; // Chart.js instance
let _activePeriod = "all";
let _walletVolume = 0;    // total_volume from wallet data — used for tooltip %

// Positions / Activity tab state
let _posStatus            = "active";   // "active" | "closed"
let _posMarketFilter      = new Set();  // empty = all markets; else condition IDs to show
let _posAllMarkets        = [];         // [{id, title}] built from loaded position data
let _activityLoaded       = false;
let _initialMarketApplied = false;
const _posDataCache       = { active: [], closed: [] };

// Pagination state (page is 1-based)
const _pag = {
  active:   { page: 1, pageSize: 25 },
  closed:   { page: 1, pageSize: 25 },
  activity: { page: 1, pageSize: 25 },
};

// Activity filter state (mirrors position market filter for Activity tab)
let _activityMarketFilter = new Set();
let _activityAllMarkets   = [];
let _activityDataCache    = [];

// ----------------------------------------------------------------
// DOM helpers
// ----------------------------------------------------------------
const $ = (sel) => document.querySelector(sel);

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function escHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// ----------------------------------------------------------------
// Formatters
// ----------------------------------------------------------------
const fmtUSD = (v) =>
  v == null ? "—" :
  v >= 1_000_000 ? `$${(v / 1_000_000).toFixed(2)}M` :
  v >= 1_000     ? `$${(v / 1_000).toFixed(1)}k` :
                   `$${Number(v).toFixed(2)}`;

const fmtNum = (v) =>
  v == null ? "—" : Number(v).toLocaleString("en-US", { maximumFractionDigits: 2 });

const fmtPrice = (v) =>
  v == null ? "—" : `${(Number(v) * 100).toFixed(1)}¢`;

function fmtTime(ts) {
  if (!ts) return "—";
  const d   = new Date(ts * 1000);
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} `
       + `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

function fmtAge(firstSeenTs) {
  if (!firstSeenTs) return "—";
  const days = Math.floor((Date.now() / 1000 - firstSeenTs) / 86400);
  if (days === 0) return "Today";
  if (days === 1) return "1 day";
  return `${days.toLocaleString()} days`;
}

function shortAddr(addr) {
  if (!addr) return "—";
  return addr.slice(0, 6) + "…" + addr.slice(-4);
}

function fmtDuration(seconds) {
  if (seconds == null || isNaN(seconds)) return "—";
  if (seconds < 60)    return `${Math.round(seconds)}s`;
  if (seconds < 3600)  return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}

// Calculate avg and median gap between consecutive trades (seconds).
// trades: array of activity items, each with timestamp or createdAt field.
function calcTradeTiming(trades) {
  const timestamps = trades
    .map(t => {
      const raw = t.timestamp ?? t.createdAt ?? null;
      if (raw == null) return null;
      return typeof raw === "string" ? Math.floor(new Date(raw).getTime() / 1000) : raw;
    })
    .filter(t => t != null && !isNaN(t))
    .sort((a, b) => a - b);

  if (timestamps.length < 2) return { avg: null, median: null };

  const diffs = [];
  for (let i = 1; i < timestamps.length; i++) {
    const d = timestamps[i] - timestamps[i - 1];
    if (d >= 0) diffs.push(d);
  }
  if (!diffs.length) return { avg: null, median: null };

  const avg    = diffs.reduce((a, b) => a + b, 0) / diffs.length;
  const sorted = [...diffs].sort((a, b) => a - b);
  const mid    = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];

  return { avg, median };
}

// Update the two timing stat cards from a set of activity items.
function updateTimingStats(trades) {
  const { avg, median } = calcTradeTiming(trades);
  setText("wStatAvgTime",  fmtDuration(avg));
  setText("wStatMedTime",  fmtDuration(median));
}

// Generic paginator — injects buttons into containerId, calls onPageChange(newPage).
function renderPagination(containerId, total, pageSize, currentPage, onPageChange) {
  const el = document.getElementById(containerId);
  if (!el) return;
  const pages = Math.max(1, Math.ceil(total / pageSize));
  if (pages <= 1) { el.innerHTML = ""; return; }

  const mkBtn = (label, page, disabled, active) =>
    `<button class="pag-btn${active ? " pag-btn--active" : ""}"` +
    ` ${disabled ? "disabled" : ""} data-page="${page}">${label}</button>`;

  let html = mkBtn("«", 1,              currentPage === 1,     false)
           + mkBtn("‹", currentPage - 1, currentPage === 1,     false);

  const startP = Math.max(1, currentPage - 2);
  const endP   = Math.min(pages, startP + 4);
  if (startP > 1) html += `<span class="pag-ellipsis">…</span>`;
  for (let p = startP; p <= endP; p++) html += mkBtn(p, p, false, p === currentPage);
  if (endP < pages) html += `<span class="pag-ellipsis">…</span>`;

  html += mkBtn("›", currentPage + 1, currentPage === pages, false)
        + mkBtn("»", pages,           currentPage === pages, false);

  el.innerHTML = html;
  el.querySelectorAll(".pag-btn:not([disabled])").forEach(b =>
    b.addEventListener("click", () => onPageChange(parseInt(b.dataset.page)))
  );
}

// Returns a human-readable market label: title (truncated) or short ID.
function marketLabel(conditionId) {
  const title = _marketsMap[conditionId];
  if (title) {
    return title.length > 48 ? title.slice(0, 45) + "…" : title;
  }
  // Fall back to truncated condition_id
  return conditionId ? conditionId.slice(0, 14) + "…" : "—";
}

// ----------------------------------------------------------------
// Toast
// ----------------------------------------------------------------
function toast(msg, type = "info", ms = 3500) {
  const c = document.getElementById("toastContainer");
  if (!c) return;
  const el = document.createElement("div");
  el.className = `toast toast--${type}`;
  el.textContent = msg;
  c.appendChild(el);
  setTimeout(() => el.remove(), ms);
}

// ----------------------------------------------------------------
// API helpers
// ----------------------------------------------------------------
async function apiFetch(path) {
  const resp = await fetch(path);
  if (!resp.ok) {
    const err = new Error(`HTTP ${resp.status}`);
    err.status = resp.status;
    throw err;
  }
  return resp.json();
}

async function loadMarkets() {
  try {
    const markets = await apiFetch("/api/markets?limit=200");
    _marketsMap = {};
    for (const m of markets) {
      if (m.condition_id && m.title) {
        _marketsMap[m.condition_id] = m.title;
      }
    }
  } catch (_) {
    // non-critical — positions will fall back to truncated IDs
  }
}

// ----------------------------------------------------------------
// Show / hide states
// ----------------------------------------------------------------
function showState(state) {
  document.getElementById("walletEmpty").style.display   = state === "empty"   ? "" : "none";
  document.getElementById("walletError").style.display   = state === "error"   ? "" : "none";
  document.getElementById("walletContent").style.display = state === "content" ? "" : "none";
}

// ----------------------------------------------------------------
// Render wallet content
// ----------------------------------------------------------------
function renderWallet(data) {
  const w = data.wallet || {};

  // ── Profile ──────────────────────────────────────────────────
  const displayName = w.pseudonym || w.name || shortAddr(w.address);
  setText("walletDisplayName", displayName);
  setText("walletAddressMono", w.address || "");

  // ── Wallet eye button (watchlist) ─────────────────────────────
  const eyeBtn = document.getElementById("walletEyeBtn");
  if (eyeBtn && w.address && window.WL) {
    eyeBtn.style.display = "";
    const _applyWalletEye = (isWatched) => {
      eyeBtn.classList.toggle("eye-btn--watched",   isWatched);
      eyeBtn.classList.toggle("eye-btn--unwatched", !isWatched);
      eyeBtn.innerHTML = isWatched ? "Watchlist 👁" : "Add Wallet to Watchlist 👁";
    };
    window.WL.checkWatchStatus("wallet", w.address).then(s => _applyWalletEye(s.is_watched));
    eyeBtn.onclick = () => {
      window.WL.showAddModal("wallet", w.address, displayName, () => {
        window.WL.checkWatchStatus("wallet", w.address).then(s => _applyWalletEye(s.is_watched));
      });
    };
  }

  const imgEl  = document.getElementById("walletAvatarImg");
  const phEl   = document.getElementById("walletAvatarPlaceholder");
  if (w.profile_image) {
    imgEl.src = w.profile_image;
    imgEl.style.display = "";
    phEl.style.display  = "none";
  } else {
    imgEl.style.display = "none";
    phEl.style.display  = "";
  }

  const bioEl = document.getElementById("walletBio");
  if (w.bio) {
    bioEl.textContent  = w.bio;
    bioEl.style.display = "";
  } else {
    bioEl.style.display = "none";
  }

  const updated = w.last_updated ? `Updated ${w.last_updated.slice(0, 16)} UTC` : "";
  setText("walletUpdated", updated);

  // ── Overview cards ───────────────────────────────────────────
  setText("wStatAge",     fmtAge(w.first_seen));
  setText("wStatTrades",  fmtNum(w.total_trades));
  setText("wStatVolume",  fmtUSD(w.total_volume));
  setText("wStatAvg",     fmtUSD(w.avg_trade_size));
  setText("wStatLargest", fmtUSD(w.largest_trade));
  _walletVolume = w.total_volume || 0;

  // PnL — color-coded
  const pnlEl = document.getElementById("wStatPnl");
  if (pnlEl) {
    const pnl = w.realized_pnl;
    if (pnl == null) {
      pnlEl.textContent = "—";
      pnlEl.style.color = "";
    } else {
      pnlEl.textContent = (pnl >= 0 ? "+" : "") + fmtUSD(pnl);
      pnlEl.style.color = pnl >= 0 ? "var(--green)" : "var(--red)";
    }
  }

}

// ----------------------------------------------------------------
// PnL Chart — Chart.js helpers
// ----------------------------------------------------------------

// Crosshair plugin: draws a vertical line at the hovered x position
const _crosshairPlugin = {
  id: "crosshair",
  afterDraw(chart) {
    if (!chart._crosshairX) return;
    const { ctx, chartArea: { top, bottom } } = chart;
    ctx.save();
    ctx.beginPath();
    ctx.moveTo(chart._crosshairX, top);
    ctx.lineTo(chart._crosshairX, bottom);
    ctx.strokeStyle = "rgba(255,255,255,0.18)";
    ctx.lineWidth   = 1;
    ctx.setLineDash([4, 4]);
    ctx.stroke();
    ctx.restore();
  },
};

function _buildGradient(ctx, chartArea, isPositive) {
  const grad = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
  if (isPositive) {
    grad.addColorStop(0, "rgba(63,185,80,0.35)");
    grad.addColorStop(1, "rgba(63,185,80,0.02)");
  } else {
    grad.addColorStop(0, "rgba(248,81,73,0.02)");
    grad.addColorStop(1, "rgba(248,81,73,0.35)");
  }
  return grad;
}

function initPnlChart() {
  const canvas = document.getElementById("pnlChart");
  if (!canvas || !window.Chart) return;

  if (_pnlChart) {
    _pnlChart.destroy();
    _pnlChart = null;
  }

  const ctx = canvas.getContext("2d");

  _pnlChart = new Chart(ctx, {
    type: "line",
    plugins: [_crosshairPlugin],
    data: {
      labels: [],
      datasets: [{
        data: [],
        borderColor: "var(--green)",
        borderWidth: 1.5,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: "var(--green)",
        fill: true,
        backgroundColor: (context) => {
          const chart     = context.chart;
          const { ctx: c, chartArea } = chart;
          if (!chartArea) return "transparent";
          const data = chart.data.datasets[0].data;
          const last = data.length ? data[data.length - 1] : 0;
          return _buildGradient(c, chartArea, last >= 0);
        },
        tension: 0.3,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 250 },
      interaction: { mode: "index", intersect: false },
      onHover(_, elements, chart) {
        if (elements.length) {
          const el = elements[0];
          chart._crosshairX = el.element.x;
        } else {
          chart._crosshairX = null;
        }
        chart.draw();
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          displayColors: false,
          backgroundColor: "#1f2937",
          borderColor: "#30363d",
          borderWidth: 1,
          titleColor: "#8b949e",
          bodyColor: "#e6edf3",
          padding: 10,
          callbacks: {
            title(items) {
              return items[0]?.label || "";
            },
            label(item) {
              const val  = item.raw;
              const sign = val >= 0 ? "+" : "";
              const pnlStr = `PnL  ${sign}${fmtUSD(val)}`;
              if (_walletVolume > 0) {
                const pct    = (val / _walletVolume) * 100;
                const pctStr = `${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%`;
                return `${pnlStr}  (${pctStr})`;
              }
              return pnlStr;
            },
          },
        },
      },
      scales: {
        x: {
          ticks: {
            color: "#8b949e",
            font: { size: 10 },
            maxTicksLimit: 6,
            maxRotation: 0,
          },
          grid: { color: "rgba(48,54,61,0.6)" },
        },
        y: {
          ticks: {
            color: "#8b949e",
            font: { size: 10 },
            callback: (v) => fmtUSD(v),
          },
          grid: { color: "rgba(48,54,61,0.6)" },
        },
      },
    },
  });
}

function _cutoffTs(period) {
  const now = Date.now() / 1000;
  switch (period) {
    case "24h": return now - 86_400;
    case "1w":  return now - 7 * 86_400;
    case "1m":  return now - 30 * 86_400;
    case "3m":  return now - 91 * 86_400;
    case "1y":  return now - 365 * 86_400;
    default:    return 0;
  }
}

function _updatePnlChart(data) {
  if (!_pnlChart) return;

  if (!data || data.length === 0) {
    _pnlChart.data.labels   = [];
    _pnlChart.data.datasets[0].data = [];
    _pnlChart.update();
    return;
  }

  // Re-base to the start of the visible window so each period starts at 0.
  // Without this, a 3M view would show all-time cumulative PnL values instead
  // of just the PnL earned within the selected period.
  const baseline = data[0].cum_pnl;
  const rebased  = data.map(pt => pt.cum_pnl - baseline);

  // Determine chart line color based on net direction of rebased data
  const lastVal   = rebased[rebased.length - 1];
  const lineColor = lastVal >= 0 ? "var(--green)" : "var(--red)";
  _pnlChart.data.datasets[0].borderColor              = lineColor;
  _pnlChart.data.datasets[0].pointHoverBackgroundColor = lineColor;

  _pnlChart.data.labels = data.map(pt => {
    const d = new Date(pt.ts * 1000);
    const mo  = String(d.getUTCMonth() + 1).padStart(2, "0");
    const day = String(d.getUTCDate()).padStart(2, "0");
    const hr  = String(d.getUTCHours()).padStart(2, "0");
    const mn  = String(d.getUTCMinutes()).padStart(2, "0");
    return `${mo}-${day} ${hr}:${mn}`;
  });
  _pnlChart.data.datasets[0].data = rebased;
  _pnlChart.update();
}

function setPeriod(period) {
  _activePeriod = period;
  // Update button styles
  document.querySelectorAll(".period-btn").forEach(btn => {
    btn.classList.toggle("period-btn--active", btn.dataset.period === period);
  });
  // Filter and redraw
  const cutoff  = _cutoffTs(period);
  const filtered = cutoff ? _pnlHistory.filter(pt => pt.ts >= cutoff) : _pnlHistory;
  _updatePnlChart(filtered);
}

async function loadPnlHistory(address) {
  if (!address) return;
  try {
    const data = await apiFetch(`/api/wallet-pnl-history/${encodeURIComponent(address)}`);
    _pnlHistory = Array.isArray(data) ? data : [];
    setPeriod(_activePeriod);
  } catch (_) {
    // Non-critical — chart stays empty
    _pnlHistory = [];
    _updatePnlChart([]);
  }
}

// ----------------------------------------------------------------
// Load wallet from API
// ----------------------------------------------------------------
async function loadWallet(address) {
  if (!address) return;

  try {
    const data = await apiFetch(`/api/wallet/${encodeURIComponent(address)}`);
    renderWallet(data);
    showState("content");
    // Fire background fetches — update cards and chart when ready
    loadWalletLifetime(address);
    loadPnlHistory(address);
    loadPositions(address, "active");
    loadActivity(address);  // eagerly fetch for timing stats; tab renders from cache
  } catch (e) {
    if (e.status === 404) {
      setText("walletErrorTitle", "Wallet not found");
      setText("walletErrorSub",
        "No analytics data found for this address. " +
        "The wallet analyzer may not have processed it yet, " +
        "or this address has no recorded trades.");
    } else {
      setText("walletErrorTitle", "Error loading wallet");
      setText("walletErrorSub", e.message || "Unknown error");
    }
    showState("error");
  }
}

// ----------------------------------------------------------------
// Load lifetime stats from Polymarket APIs (non-blocking overlay)
// Overwrites the three cards that are wrong when derived from local DB:
//   wStatAge    ← account createdAt (Gamma /public-profile)
//   wStatTrades ← lifetime trade count (data-api /traded)
//   wStatPnl    ← sum of closed-position realizedPnl (data-api /closed-positions)
// ----------------------------------------------------------------
async function loadWalletLifetime(address) {
  try {
    const s = await apiFetch(`/api/wallet-lifetime/${encodeURIComponent(address)}`);

    // Wallet age — replace "Today" with real account creation date
    if (s.created_at_ts != null) {
      setText("wStatAge", fmtAge(s.created_at_ts));
    }

    // Lifetime trade count
    if (s.total_traded != null) {
      setText("wStatTrades", fmtNum(s.total_traded));
    }

    // Lifetime realized PnL from closed positions
    if (s.realized_pnl != null) {
      const pnlEl = document.getElementById("wStatPnl");
      if (pnlEl) {
        pnlEl.textContent  = (s.realized_pnl >= 0 ? "+" : "") + fmtUSD(s.realized_pnl);
        pnlEl.style.color  = s.realized_pnl >= 0 ? "var(--green)" : "var(--red)";
      }
    }

    // Profile fields — essential for external wallets where the stub has nulls,
    // and also keeps local-wallet profiles up-to-date with Polymarket's latest data.
    if (s.pseudonym) {
      setText("walletDisplayName", s.pseudonym);
    }
    const imgEl = document.getElementById("walletAvatarImg");
    const phEl  = document.getElementById("walletAvatarPlaceholder");
    if (s.profile_image && imgEl && phEl) {
      imgEl.src           = s.profile_image;
      imgEl.style.display = "";
      phEl.style.display  = "none";
    }
    const bioEl = document.getElementById("walletBio");
    if (s.bio && bioEl) {
      bioEl.textContent   = s.bio;
      bioEl.style.display = "";
    }
  } catch (_) {
    // Non-critical — cards retain the local-DB fallback values already displayed
  }
}

// ----------------------------------------------------------------
// Section tabs — Positions | Activity
// ----------------------------------------------------------------
function initSectionTabs() {
  document.querySelectorAll(".wallet-section-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      const target = tab.dataset.tab;
      document.querySelectorAll(".wallet-section-tab").forEach(t =>
        t.classList.toggle("wallet-section-tab--active", t.dataset.tab === target));
      document.getElementById("tabPositions").style.display = target === "positions" ? "" : "none";
      document.getElementById("tabActivity").style.display  = target === "activity"  ? "" : "none";
      if (target === "activity" && _currentAddress) {
        if (!_activityLoaded) {
          loadActivity(_currentAddress);
        }
        // If already loaded, data is already rendered (eager load in loadWallet).
      }
    });
  });
}

// ----------------------------------------------------------------
// Positions — status toggle + market filter setup
// ----------------------------------------------------------------
function initPositionsControls() {
  // Active / Closed toggle
  document.querySelectorAll(".pos-status-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const status = btn.dataset.status;
      if (status === _posStatus) return;
      _posStatus = status;
      document.querySelectorAll(".pos-status-btn").forEach(b =>
        b.classList.toggle("pos-status-btn--active", b.dataset.status === status));

      const isActive = status === "active";
      document.getElementById("activePositionsWrap").style.display       = isActive ? "" : "none";
      document.getElementById("closedPositionsWrap").style.display       = isActive ? "none" : "";
      document.getElementById("activePosResultCount").style.display      = isActive ? "" : "none";
      document.getElementById("closedPosResultCount").style.display      = isActive ? "none" : "";
      document.getElementById("activePosPagination").style.display       = isActive ? "" : "none";
      document.getElementById("closedPosPagination").style.display       = isActive ? "none" : "";

      if (_currentAddress) {
        if (_posDataCache[status].length) {
          isActive
            ? renderActivePositions(_posDataCache.active)
            : renderClosedPositions(_posDataCache.closed);
        } else {
          loadPositions(_currentAddress, status);
        }
      }
    });
  });

  // Positions page-size selector (shared between active / closed)
  const posPageSizeEl = document.getElementById("posPageSize");
  if (posPageSizeEl) {
    posPageSizeEl.addEventListener("change", () => {
      const ps = parseInt(posPageSizeEl.value);
      _pag.active.pageSize = ps;
      _pag.closed.pageSize = ps;
      _pag.active.page     = 1;
      _pag.closed.page     = 1;
      if (_posStatus === "active") renderActivePositions(_posDataCache.active);
      else                         renderClosedPositions(_posDataCache.closed);
    });
  }

  // Positions market filter dropdown
  const btn      = document.getElementById("marketFilterBtn");
  const dropdown = document.getElementById("marketFilterDropdown");
  if (btn && dropdown) {
    btn.addEventListener("click", e => {
      e.stopPropagation();
      dropdown.style.display = dropdown.style.display === "none" ? "" : "none";
    });
    document.addEventListener("click", () => { dropdown.style.display = "none"; });
    dropdown.addEventListener("click", e => e.stopPropagation());
  }

  // Activity filter dropdown
  const actBtn      = document.getElementById("activityFilterBtn");
  const actDropdown = document.getElementById("activityFilterDropdown");
  if (actBtn && actDropdown) {
    actBtn.addEventListener("click", e => {
      e.stopPropagation();
      actDropdown.style.display = actDropdown.style.display === "none" ? "" : "none";
    });
    actDropdown.addEventListener("click", e => e.stopPropagation());
  }

  // Activity page-size selector
  const actPageSizeEl = document.getElementById("activityPageSize");
  if (actPageSizeEl) {
    actPageSizeEl.addEventListener("change", () => {
      _pag.activity.pageSize = parseInt(actPageSizeEl.value);
      _pag.activity.page     = 1;
      applyActivityFilter();
    });
  }
}

// ----------------------------------------------------------------
// Positions — market filter dropdown build + apply
// ----------------------------------------------------------------
function buildMarketFilter(markets, preSelectedId) {
  _posAllMarkets = markets;
  if (preSelectedId && markets.some(m => m.id === preSelectedId)) {
    _posMarketFilter.clear();
    _posMarketFilter.add(preSelectedId);
  }

  const dropdown = document.getElementById("marketFilterDropdown");
  if (!dropdown) return;

  if (markets.length === 0) {
    dropdown.innerHTML = '<div class="market-filter-empty">No markets found</div>';
    return;
  }

  const allSelected = _posMarketFilter.size === 0;
  let html = `<label class="market-filter-option market-filter-all">
    <input type="checkbox" class="mf-all-cb" ${allSelected ? "checked" : ""}> All Markets
  </label><div class="market-filter-divider"></div>`;
  for (const m of markets) {
    const checked = allSelected || _posMarketFilter.has(m.id);
    const short   = m.title.length > 46 ? m.title.slice(0, 43) + "…" : m.title;
    html += `<label class="market-filter-option" title="${escHtml(m.title)}">
      <input type="checkbox" class="mf-market-cb" data-id="${escHtml(m.id)}" ${checked ? "checked" : ""}> ${escHtml(short)}
    </label>`;
  }
  dropdown.innerHTML = html;

  const allCb     = dropdown.querySelector(".mf-all-cb");
  const marketCbs = dropdown.querySelectorAll(".mf-market-cb");

  allCb?.addEventListener("change", () => {
    _posMarketFilter.clear();
    marketCbs.forEach(cb => { cb.checked = allCb.checked; });
    updateMarketFilterLabel();
    applyMarketFilter();
  });

  marketCbs.forEach(cb => {
    cb.addEventListener("change", () => {
      if (cb.checked) { _posMarketFilter.add(cb.dataset.id); }
      else            { _posMarketFilter.delete(cb.dataset.id); }
      // If every market is now checked manually, treat as "all"
      if (_posMarketFilter.size === markets.length) {
        _posMarketFilter.clear();
        marketCbs.forEach(c => { c.checked = true; });
        if (allCb) allCb.checked = true;
      } else {
        if (allCb) allCb.checked = false;
      }
      updateMarketFilterLabel();
      applyMarketFilter();
    });
  });
}

function updateMarketFilterLabel() {
  const label = document.getElementById("marketFilterLabel");
  if (!label) return;
  label.textContent = _posMarketFilter.size === 0
    ? "All Markets"
    : `${_posMarketFilter.size} market${_posMarketFilter.size !== 1 ? "s" : ""} selected`;
}

function applyMarketFilter() {
  if (_posStatus === "active") renderActivePositions(_posDataCache.active);
  else                         renderClosedPositions(_posDataCache.closed);
}

// ----------------------------------------------------------------
// Activity — market filter dropdown (mirrors positions filter)
// ----------------------------------------------------------------
function buildActivityFilter(markets) {
  _activityAllMarkets = markets;
  const dropdown = document.getElementById("activityFilterDropdown");
  if (!dropdown) return;

  if (markets.length === 0) {
    dropdown.innerHTML = '<div class="market-filter-empty">No markets found</div>';
    return;
  }

  const allSelected = _activityMarketFilter.size === 0;
  let html = `<label class="market-filter-option market-filter-all">
    <input type="checkbox" class="af-all-cb" ${allSelected ? "checked" : ""}> All Markets
  </label><div class="market-filter-divider"></div>`;
  for (const m of markets) {
    const checked = allSelected || _activityMarketFilter.has(m.id);
    const short   = m.title.length > 46 ? m.title.slice(0, 43) + "…" : m.title;
    html += `<label class="market-filter-option" title="${escHtml(m.title)}">
      <input type="checkbox" class="af-market-cb" data-id="${escHtml(m.id)}" ${checked ? "checked" : ""}> ${escHtml(short)}
    </label>`;
  }
  dropdown.innerHTML = html;

  const allCb     = dropdown.querySelector(".af-all-cb");
  const marketCbs = dropdown.querySelectorAll(".af-market-cb");

  allCb?.addEventListener("change", () => {
    _activityMarketFilter.clear();
    marketCbs.forEach(cb => { cb.checked = allCb.checked; });
    updateActivityFilterLabel();
    applyActivityFilter();
  });

  marketCbs.forEach(cb => {
    cb.addEventListener("change", () => {
      if (cb.checked) _activityMarketFilter.add(cb.dataset.id);
      else            _activityMarketFilter.delete(cb.dataset.id);
      if (_activityMarketFilter.size === markets.length) {
        _activityMarketFilter.clear();
        marketCbs.forEach(c => { c.checked = true; });
        if (allCb) allCb.checked = true;
      } else {
        if (allCb) allCb.checked = false;
      }
      updateActivityFilterLabel();
      applyActivityFilter();
    });
  });
}

function updateActivityFilterLabel() {
  const label = document.getElementById("activityFilterLabel");
  if (!label) return;
  label.textContent = _activityMarketFilter.size === 0
    ? "All Markets"
    : `${_activityMarketFilter.size} market${_activityMarketFilter.size !== 1 ? "s" : ""} selected`;
}

// Re-render activity with current filter and update timing stats.
function applyActivityFilter() {
  const filtered = _activityMarketFilter.size === 0
    ? _activityDataCache
    : _activityDataCache.filter(item => {
        const id = item.market || item.conditionId || item.condition_id || "";
        return _activityMarketFilter.has(id);
      });
  _pag.activity.page = 1;
  renderActivity(filtered);
  updateTimingStats(filtered);
}

// ----------------------------------------------------------------
// Positions — load + render
// ----------------------------------------------------------------
async function loadPositions(address, status) {
  const tbody = document.getElementById(status === "active" ? "activePositionsBody" : "closedPositionsBody");
  const cols  = status === "active" ? 6 : 5;
  if (tbody) tbody.innerHTML = `<tr><td colspan="${cols}" class="empty">Loading…</td></tr>`;

  try {
    const data = await apiFetch(
      `/api/wallet-positions/${encodeURIComponent(address)}?status=${status}`
    );
    _posDataCache[status] = Array.isArray(data) ? data : [];

    if (status === "active") {
      // Build market list from this data
      const seen = new Map();
      for (const p of _posDataCache.active) {
        const id    = p.market || p.conditionId || p.condition_id || "";
        const title = p.title  || p.question    || (id ? id.slice(0, 16) + "…" : "Unknown");
        if (id && !seen.has(id)) seen.set(id, title);
      }
      const markets    = [...seen.entries()].map(([id, title]) => ({ id, title }));
      const preSelMkt  = (!_initialMarketApplied && window.INITIAL_MARKET_ID) ? window.INITIAL_MARKET_ID : null;
      if (preSelMkt) _initialMarketApplied = true;
      buildMarketFilter(markets, preSelMkt);
      updateMarketFilterLabel();
      renderActivePositions(_posDataCache.active);
    } else {
      renderClosedPositions(_posDataCache.closed);
    }
  } catch (_) {
    const cols = status === "active" ? 6 : 5;
    if (tbody) tbody.innerHTML = `<tr><td colspan="${cols}" class="empty">Could not load positions.</td></tr>`;
  }
}

function _filterByMarket(data) {
  if (_posMarketFilter.size === 0) return data;
  return data.filter(p => {
    const id = p.market || p.conditionId || p.condition_id || "";
    return _posMarketFilter.has(id);
  });
}

function renderActivePositions(data) {
  const tbody = document.getElementById("activePositionsBody");
  if (!tbody) return;

  const rows  = _filterByMarket(data);
  const total = rows.length;
  const ps    = _pag.active.pageSize;
  const pg    = _pag.active.page;
  const slice = rows.slice((pg - 1) * ps, pg * ps);

  const rcEl = document.getElementById("activePosResultCount");
  if (rcEl) rcEl.textContent = total ? `${total} position${total !== 1 ? "s" : ""}` : "";

  if (!slice.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty">No active positions.</td></tr>';
    renderPagination("activePosPagination", 0, ps, 1, () => {});
    return;
  }

  tbody.innerHTML = slice.map(p => {
    const condId   = p.market || p.conditionId || p.condition_id || "";
    const title    = p.title || p.question || (condId ? condId.slice(0, 16) + "…" : "—");
    const outcome  = p.outcome  || "—";
    const outLow   = outcome.toLowerCase();
    const avgPrice = p.avgPrice     ?? p.averagePrice ?? p.avg_price ?? null;
    const shares   = p.size         ?? p.shares       ?? null;
    const value    = p.currentValue ?? p.value        ?? null;
    const curPrice = p.currentPrice ?? p.price
      ?? (value != null && shares != null && shares > 0 ? value / shares : null);
    const titleHtml = condId
      ? `<a href="/?market_id=${encodeURIComponent(condId)}" class="market-link">${escHtml(title)}</a>`
      : escHtml(title);
    return `<tr>
      <td title="${escHtml(condId)}">${titleHtml}</td>
      <td><span class="badge badge--${outLow}">${escHtml(outcome)}</span></td>
      <td class="num">${avgPrice != null ? fmtPrice(avgPrice) : "—"}</td>
      <td class="num">${curPrice != null ? fmtPrice(curPrice) : "—"}</td>
      <td class="num">${shares  != null ? fmtNum(shares)      : "—"}</td>
      <td class="num">${value   != null ? fmtUSD(value)       : "—"}</td>
    </tr>`;
  }).join("");

  renderPagination("activePosPagination", total, ps, pg, (newPg) => {
    _pag.active.page = newPg;
    renderActivePositions(data);
  });
}

function renderClosedPositions(data) {
  const tbody = document.getElementById("closedPositionsBody");
  if (!tbody) return;

  const rows  = _filterByMarket(data);
  const total = rows.length;
  const ps    = _pag.closed.pageSize;
  const pg    = _pag.closed.page;
  const slice = rows.slice((pg - 1) * ps, pg * ps);

  const rcEl = document.getElementById("closedPosResultCount");
  if (rcEl) rcEl.textContent = total ? `${total} position${total !== 1 ? "s" : ""}` : "";

  if (!slice.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty">No closed positions.</td></tr>';
    renderPagination("closedPosPagination", 0, ps, 1, () => {});
    return;
  }

  tbody.innerHTML = slice.map(p => {
    const condId   = p.market || p.conditionId || p.condition_id || "";
    const title    = p.title || p.question || (condId ? condId.slice(0, 16) + "…" : "—");
    const outcome  = p.outcome || "—";
    const outLow   = outcome.toLowerCase();
    const pnl      = p.realizedPnl ?? p.realized_pnl ?? p.pnl ?? null;
    const avgPrice = p.avgPrice    ?? p.averagePrice  ?? p.avg_price ?? null;
    let resultHtml;
    if (pnl == null)        resultHtml = "—";
    else if (pnl > 0.005)   resultHtml = '<span class="badge badge--won">WON</span>';
    else if (pnl < -0.005)  resultHtml = '<span class="badge badge--lost">LOST</span>';
    else                    resultHtml = '<span class="badge badge--neutral">BREAK-EVEN</span>';
    const pnlStr   = pnl != null ? (pnl >= 0 ? "+" : "") + fmtUSD(pnl) : "—";
    const pnlColor = pnl != null ? (pnl >= 0 ? "var(--green)" : "var(--red)") : "";
    const titleHtml = condId
      ? `<a href="/?market_id=${encodeURIComponent(condId)}" class="market-link">${escHtml(title)}</a>`
      : escHtml(title);
    return `<tr>
      <td>${resultHtml}</td>
      <td title="${escHtml(condId)}">${titleHtml}</td>
      <td><span class="badge badge--${outLow}">${escHtml(outcome)}</span></td>
      <td class="num">${avgPrice != null ? fmtPrice(avgPrice) : "—"}</td>
      <td class="num" style="color:${pnlColor}">${pnlStr}</td>
    </tr>`;
  }).join("");

  renderPagination("closedPosPagination", total, ps, pg, (newPg) => {
    _pag.closed.page = newPg;
    renderClosedPositions(data);
  });
}

// ----------------------------------------------------------------
// Activity — load + render
// ----------------------------------------------------------------
async function loadActivity(address) {
  _activityLoaded = true;
  const tbody = document.getElementById("activityBody");
  if (tbody) tbody.innerHTML = '<tr><td colspan="7" class="empty">Loading…</td></tr>';
  try {
    const data = await apiFetch(`/api/wallet-activity/${encodeURIComponent(address)}`);
    // Guard against stale responses if the user searched a new wallet mid-flight.
    if (address !== _currentAddress) return;
    _activityDataCache = Array.isArray(data) ? data : [];

    // Build activity market filter from loaded trades.
    const seen = new Map();
    for (const item of _activityDataCache) {
      const id    = item.market || item.conditionId || item.condition_id || "";
      const title = item.title  || item.question    || (id ? id.slice(0, 16) + "…" : "Unknown");
      if (id && !seen.has(id)) seen.set(id, title);
    }
    buildActivityFilter([...seen.entries()].map(([id, title]) => ({ id, title })));
    updateActivityFilterLabel();

    // Render + update timing stats (respects any active market filter).
    applyActivityFilter();
  } catch (_) {
    if (address !== _currentAddress) return;
    if (tbody) tbody.innerHTML = '<tr><td colspan="7" class="empty">Could not load activity.</td></tr>';
  }
}

function renderActivity(data) {
  const tbody = document.getElementById("activityBody");
  if (!tbody) return;

  const total = data.length;
  const ps    = _pag.activity.pageSize;
  const pg    = _pag.activity.page;
  const slice = data.slice((pg - 1) * ps, pg * ps);

  const rcEl = document.getElementById("activityResultCount");
  if (rcEl) rcEl.textContent = total ? `${total} trade${total !== 1 ? "s" : ""}` : "";

  if (!slice.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty">No activity found.</td></tr>';
    renderPagination("activityPagination", 0, ps, 1, () => {});
    return;
  }

  tbody.innerHTML = slice.map(item => {
    const ts      = item.timestamp ?? item.createdAt ?? null;
    const type    = (item.type ?? item.side ?? "—").toUpperCase();
    const condId  = item.market || item.conditionId || item.condition_id || "";
    const title   = item.title ?? item.question ?? (condId ? condId.slice(0, 16) + "…" : "—");
    const outcome = item.outcome ?? "—";
    const outLow  = outcome.toLowerCase();
    const price   = item.price ?? null;
    const size    = item.size  ?? null;
    const usdc    = item.usdcSize ?? item.cashPnl ?? item.amount ?? null;
    const typeClass  = type === "BUY" ? "buy" : type === "SELL" ? "sell" : "";
    const shortTitle = typeof title === "string" && title.length > 40 ? title.slice(0, 37) + "…" : title;
    const titleHtml  = condId
      ? `<a href="/?market_id=${encodeURIComponent(condId)}" class="market-link">${escHtml(shortTitle)}</a>`
      : escHtml(shortTitle);
    return `<tr>
      <td>${ts ? fmtTime(typeof ts === "string" ? Math.floor(new Date(ts).getTime() / 1000) : ts) : "—"}</td>
      <td><span class="badge badge--${typeClass}">${escHtml(type)}</span></td>
      <td title="${escHtml(condId)}">${titleHtml}</td>
      <td>${outcome !== "—" ? `<span class="badge badge--${outLow}">${escHtml(outcome)}</span>` : "—"}</td>
      <td class="num">${price != null ? fmtPrice(price) : "—"}</td>
      <td class="num">${size  != null ? fmtNum(size)    : "—"}</td>
      <td class="num">${usdc  != null ? fmtUSD(usdc)    : "—"}</td>
    </tr>`;
  }).join("");

  renderPagination("activityPagination", total, ps, pg, (newPg) => {
    _pag.activity.page = newPg;
    renderActivity(data);
  });
}

// ----------------------------------------------------------------
// Auto-refresh
// ----------------------------------------------------------------
function startRefresh() {
  stopRefresh();
  _refreshTimer = setInterval(() => {
    if (_currentAddress) loadWallet(_currentAddress);
  }, REFRESH_MS);
}

function stopRefresh() {
  if (_refreshTimer) {
    clearInterval(_refreshTimer);
    _refreshTimer = null;
  }
}

// ----------------------------------------------------------------
// Search
// ----------------------------------------------------------------
function doSearch() {
  const input = document.getElementById("walletSearchInput");
  const addr  = (input?.value || "").trim();
  if (!addr) {
    toast("Enter a wallet address", "info");
    return;
  }
  // Reset tab/filter state for new search
  _posStatus      = "active";
  _activityLoaded = false;
  _posMarketFilter.clear();
  _activityMarketFilter.clear();
  _activityDataCache    = [];
  _activityAllMarkets   = [];
  _posDataCache.active  = [];
  _posDataCache.closed  = [];

  // Reset pagination
  _pag.active.page   = 1;
  _pag.closed.page   = 1;
  _pag.activity.page = 1;

  // Reset stat card timing values
  setText("wStatAvgTime", "—");
  setText("wStatMedTime", "—");

  document.querySelectorAll(".pos-status-btn").forEach(b =>
    b.classList.toggle("pos-status-btn--active", b.dataset.status === "active"));

  document.getElementById("activePositionsWrap").style.display  = "";
  document.getElementById("closedPositionsWrap").style.display  = "none";
  document.getElementById("activePosResultCount").style.display = "";
  document.getElementById("closedPosResultCount").style.display = "none";
  document.getElementById("activePosPagination").style.display  = "";
  document.getElementById("closedPosPagination").style.display  = "none";

  updateActivityFilterLabel();

  // Switch to Positions tab
  document.querySelectorAll(".wallet-section-tab").forEach(t =>
    t.classList.toggle("wallet-section-tab--active", t.dataset.tab === "positions"));
  document.getElementById("tabPositions").style.display = "";
  document.getElementById("tabActivity").style.display  = "none";
  _currentAddress = addr;

  // Update URL for bookmarkability
  const url = new URL(window.location.href);
  url.searchParams.set("address", addr);
  window.history.pushState({}, "", url.toString());

  loadWallet(addr);
}

// ----------------------------------------------------------------
// Sidebar toggle
// ----------------------------------------------------------------
function initSidebar() {
  const btn     = document.getElementById("sidebarToggle");
  const sidebar = document.getElementById("sidebar");
  if (!btn || !sidebar) return;

  btn.addEventListener("click", (e) => {
    e.stopPropagation();
    sidebar.classList.toggle("sidebar--open");
  });

  document.addEventListener("click", () => {
    if (window.innerWidth < 768) {
      sidebar.classList.remove("sidebar--open");
    }
  });
}

// ----------------------------------------------------------------
// Init
// ----------------------------------------------------------------
document.addEventListener("DOMContentLoaded", async () => {
  initSidebar();
  initPnlChart();
  initSectionTabs();
  initPositionsControls();

  // Load markets for label resolution (non-blocking)
  loadMarkets();

  const searchInput = document.getElementById("walletSearchInput");

  if (INITIAL_ADDRESS) {
    _currentAddress = INITIAL_ADDRESS;
    if (searchInput) searchInput.value = INITIAL_ADDRESS;
    await loadWallet(INITIAL_ADDRESS);
  } else {
    showState("empty");
  }

  // Search button
  document.getElementById("walletSearchBtn")?.addEventListener("click", doSearch);

  // Enter key in search box
  searchInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") doSearch();
  });

  // Period buttons
  document.querySelectorAll(".period-btn").forEach(btn => {
    btn.addEventListener("click", () => setPeriod(btn.dataset.period));
  });

  // Auto-refresh toggle
  document.getElementById("autoRefreshToggle")?.addEventListener("change", (e) => {
    _autoRefresh = e.target.checked;
    const info   = document.getElementById("refreshInfo");
    if (_autoRefresh) {
      startRefresh();
      if (info) info.style.display = "";
    } else {
      stopRefresh();
      if (info) info.style.display = "none";
    }
  });
});
