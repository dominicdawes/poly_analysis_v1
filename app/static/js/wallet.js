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
  const w         = data.wallet   || {};
  const positions = (data.positions || []).filter(p => (p.net_shares || 0) > 1e-6);
  const trades    = data.trades   || [];

  // ── Profile ──────────────────────────────────────────────────
  const displayName = w.pseudonym || w.name || shortAddr(w.address);
  setText("walletDisplayName", displayName);
  setText("walletAddressMono", w.address || "");

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

  // ── Positions ────────────────────────────────────────────────
  setText("positionCount", `${positions.length} active`);
  const posBody = document.getElementById("positionsBody");

  if (positions.length === 0) {
    posBody.innerHTML = '<tr><td colspan="6" class="empty">No active positions.</td></tr>';
  } else {
    posBody.innerHTML = positions.map(p => {
      const pnl       = p.realized_pnl || 0;
      const pnlColor  = pnl >= 0 ? "var(--green)" : "var(--red)";
      const pnlStr    = (pnl >= 0 ? "+" : "") + fmtUSD(pnl);
      const costBasis = (p.net_shares || 0) * (p.avg_entry_price || 0);
      const outLow    = (p.outcome || "").toLowerCase();
      const outClass  = outLow === "yes" ? "badge--yes" : "badge--no";
      const mktLabel  = escHtml(marketLabel(p.condition_id));
      return `<tr>
        <td title="${escHtml(p.condition_id)}">${mktLabel}</td>
        <td><span class="badge ${outClass}">${escHtml(p.outcome || "?")}</span></td>
        <td class="num">${fmtNum(p.net_shares)}</td>
        <td class="num">${fmtPrice(p.avg_entry_price)}</td>
        <td class="num">${fmtUSD(costBasis)}</td>
        <td class="num" style="color:${pnlColor}">${pnlStr}</td>
      </tr>`;
    }).join("");
  }

  // ── Recent trades ─────────────────────────────────────────────
  setText("walletTradeCount", `last ${trades.length}`);
  const tBody = document.getElementById("walletTradesBody");

  if (trades.length === 0) {
    tBody.innerHTML = '<tr><td colspan="7" class="empty">No trades found.</td></tr>';
  } else {
    tBody.innerHTML = trades.map(t => {
      const isWhale  = (t.amount || 0) >= CFG.whaleThreshold;
      const amtClass = isWhale
        ? "amount-large"
        : (t.amount || 0) >= CFG.whaleThreshold * 0.1 ? "amount-med" : "amount-small";

      // Use market_title from the trade row (populated by ingestion), or look up / truncate
      const mktTitle = t.market_title
        ? (t.market_title.length > 40 ? t.market_title.slice(0, 37) + "…" : t.market_title)
        : marketLabel(t.market_id);

      const outLow      = (t.outcome || "").toLowerCase();
      const outcomeHtml = t.outcome
        ? `<span class="badge badge--${outLow}">${escHtml(t.outcome)}</span>`
        : "—";

      return `<tr class="${isWhale ? "is-whale" : ""}">
        <td>${fmtTime(t.match_time)}</td>
        <td title="${escHtml(t.market_id || "")}">${escHtml(mktTitle)}</td>
        <td><span class="badge badge--${(t.side || "").toLowerCase()}">${escHtml(t.side || "?")}</span></td>
        <td>${outcomeHtml}</td>
        <td class="num">${fmtPrice(t.price)}</td>
        <td class="num">${fmtNum(t.size)}</td>
        <td class="num"><span class="${amtClass}">${fmtUSD(t.amount)}</span></td>
      </tr>`;
    }).join("");
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
