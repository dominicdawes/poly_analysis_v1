/* ================================================================
   main.js — Poly Analysis dashboard
   Polls the Flask API and updates the DOM without any framework.
================================================================ */

"use strict";

// ----------------------------------------------------------------
// Config (injected by Flask into index.html)
// ----------------------------------------------------------------
const CFG = window.APP_CONFIG || {
  marketId: "",
  whaleThreshold: 1000,
  fetchInterval: 60,
};

// UI refresh every 30s (independent of the backend poll interval)
const UI_REFRESH_MS = 30_000;

// ----------------------------------------------------------------
// State
// ----------------------------------------------------------------
let _trades      = [];
let _traders     = [];
let _stats       = {};
let _filterState = { minAmount: null, side: "", outcome: "", whalesOnly: false };
let _refreshTimer = null;
let _lastRefresh  = null;

// ----------------------------------------------------------------
// DOM helpers
// ----------------------------------------------------------------
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function setHtml(id, html) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = html;
}

// ----------------------------------------------------------------
// Number formatters
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
  const d = new Date(ts * 1000);
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

function shortWallet(addr) {
  if (!addr) return "—";
  return addr.slice(0, 6) + "…" + addr.slice(-4);
}

// ----------------------------------------------------------------
// Toast notifications
// ----------------------------------------------------------------
function toast(msg, type = "info", durationMs = 3500) {
  const container = document.getElementById("toastContainer");
  if (!container) return;
  const el = document.createElement("div");
  el.className = `toast toast--${type}`;
  el.textContent = msg;
  container.appendChild(el);
  setTimeout(() => el.remove(), durationMs);
}

// ----------------------------------------------------------------
// API fetch wrapper
// ----------------------------------------------------------------
async function apiFetch(path, params = {}) {
  const url = new URL(path, window.location.origin);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined && v !== "") url.searchParams.set(k, v);
  });

  const resp = await fetch(url.toString());
  if (!resp.ok) throw new Error(`API ${path} → HTTP ${resp.status}`);
  return resp.json();
}

// ----------------------------------------------------------------
// Load & render stats
// ----------------------------------------------------------------
async function loadStats() {
  try {
    _stats = await apiFetch("/api/stats", { market_id: CFG.marketId });
    renderStats();
    renderOutcomeRow();
  } catch (e) {
    console.error("loadStats:", e);
  }
}

function renderStats() {
  setText("statTrades",  fmtNum(_stats.total_trades));
  setText("statVolume",  fmtUSD(_stats.total_volume));
  setText("statAvg",     fmtUSD(_stats.avg_trade_size));
  setText("statLargest", fmtUSD(_stats.largest_trade));
  setText("statTraders", fmtNum(_stats.unique_traders));
}

function renderOutcomeRow() {
  const outcomes = _stats.volume_by_outcome || [];
  if (!outcomes.length) return;

  const html = outcomes.map(o => {
    const isYes = (o.outcome || "").toLowerCase() === "yes";
    const cls   = isYes ? "yes" : "no";
    const net   = (o.buy_volume || 0) - (o.sell_volume || 0);
    return `
      <div class="outcome-card outcome-card--${cls}">
        <div class="outcome-name outcome-name--${cls}">${escHtml(o.outcome || "?")} </div>
        <div class="outcome-stats">
          <div class="outcome-stat"><span class="lbl">Buy vol </span><span class="val">${fmtUSD(o.buy_volume)}</span></div>
          <div class="outcome-stat"><span class="lbl">Sell vol </span><span class="val">${fmtUSD(o.sell_volume)}</span></div>
          <div class="outcome-stat"><span class="lbl">Net </span><span class="val" style="color:${net>=0?'var(--green)':'var(--red)'}">${fmtUSD(Math.abs(net))}</span></div>
          <div class="outcome-stat"><span class="lbl">Trades </span><span class="val">${(o.buy_count||0)+(o.sell_count||0)}</span></div>
        </div>
      </div>`;
  }).join("");

  const el = document.getElementById("outcomeRow");
  if (el) el.innerHTML = html;
}

// ----------------------------------------------------------------
// Load & render trades
// ----------------------------------------------------------------
async function loadTrades() {
  const params = {
    market_id: CFG.marketId,
    limit: 200,
  };

  if (_filterState.minAmount)  params.min_amount  = _filterState.minAmount;
  if (_filterState.whalesOnly) params.min_amount  = CFG.whaleThreshold;

  try {
    const all = await apiFetch("/api/trades", params);
    // Client-side filtering for side / outcome (lightweight)
    _trades = all.filter(t => {
      if (_filterState.side    && t.side    !== _filterState.side)    return false;
      if (_filterState.outcome && t.outcome !== _filterState.outcome) return false;
      return true;
    });
    renderTradeTable();
  } catch (e) {
    console.error("loadTrades:", e);
    toast("Failed to load trades", "error");
  }
}

function renderTradeTable() {
  const tbody = document.getElementById("tradeTableBody");
  const count = document.getElementById("tradeCount");

  if (!_trades.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty">No trades match the current filter.</td></tr>';
    if (count) count.textContent = "0 trades";
    return;
  }

  if (count) count.textContent = `${_trades.length.toLocaleString()} trade${_trades.length !== 1 ? "s" : ""}`;

  tbody.innerHTML = _trades.map(t => {
    const isWhale = t.is_whale || (t.amount >= CFG.whaleThreshold);
    const whaleMark = isWhale ? '<span class="badge badge--whale">🐋</span>' : "";

    // Trader name / avatar
    const name   = t.trader_pseudonym || t.trader_name || shortWallet(t.proxy_wallet);
    const imgSrc = t.trader_profile_image;
    const avatarHtml = imgSrc
      ? `<img class="avatar" src="${escHtml(imgSrc)}" alt="" loading="lazy" onerror="this.style.display='none'">`
      : `<div class="avatar-placeholder">◆</div>`;

    // Tx hash link
    const txHash = t.transaction_hash;
    const txHtml = txHash
      ? `<a class="tx-link" href="https://polygonscan.com/tx/${escHtml(txHash)}" target="_blank" rel="noopener">${txHash.slice(0,8)}…</a>`
      : "—";

    const amtClass = isWhale ? "amount-large" : t.amount >= CFG.whaleThreshold * 0.1 ? "amount-med" : "amount-small";

    const outcomeLabel = t.outcome
      ? `<span class="badge badge--${(t.outcome||"").toLowerCase()}">${escHtml(t.outcome)}</span>`
      : "—";

    const walletHref = `/wallet-dashboard?address=${encodeURIComponent(t.proxy_wallet || "")}`;

    return `<tr class="${isWhale ? "is-whale" : ""}">
      <td>${fmtTime(t.match_time)}</td>
      <td>
        <div class="trader-cell">
          ${avatarHtml}
          <a class="trader-name trader-link" href="${walletHref}" title="${escHtml(t.proxy_wallet || "")}">${escHtml(name)}</a>
          ${whaleMark}
        </div>
      </td>
      <td><span class="badge badge--${(t.side||"").toLowerCase()}">${escHtml(t.side || "?")}</span></td>
      <td>${outcomeLabel}</td>
      <td class="num">${fmtPrice(t.price)}</td>
      <td class="num">${fmtNum(t.size)}</td>
      <td class="num"><span class="${amtClass}">${fmtUSD(t.amount)}</span></td>
      <td>${txHtml}</td>
    </tr>`;
  }).join("");
}

// ----------------------------------------------------------------
// Load & render traders
// ----------------------------------------------------------------
async function loadTraders() {
  try {
    _traders = await apiFetch("/api/traders", { market_id: CFG.marketId, limit: 20 });
    renderTraders();
  } catch (e) {
    console.error("loadTraders:", e);
  }
}

function renderTraders() {
  const panel = document.getElementById("tradersPanel");
  if (!panel) return;

  if (!_traders.length) {
    panel.innerHTML = '<div class="empty" style="padding:24px;text-align:center;color:var(--text-muted)">No traders yet.</div>';
    return;
  }

  panel.innerHTML = _traders.map((t, i) => {
    const name   = t.pseudonym || t.name || shortWallet(t.proxy_wallet);
    const imgSrc = t.profile_image;
    const isWhale = (t.total_volume || 0) >= CFG.whaleThreshold * 5;

    const avatarHtml = imgSrc
      ? `<img class="trader-avatar-lg" src="${escHtml(imgSrc)}" alt="" loading="lazy" onerror="this.style.display='none'">`
      : `<div class="trader-avatar-placeholder">◆</div>`;

    const walletHref = `/wallet-dashboard?address=${encodeURIComponent(t.proxy_wallet || "")}`;

    return `
      <div class="trader-card ${isWhale ? "trader-card--whale" : ""}">
        <div class="trader-rank">${i + 1}</div>
        ${avatarHtml}
        <div class="trader-info">
          <div class="trader-display-name">
            <a class="trader-link" href="${walletHref}" title="${escHtml(t.proxy_wallet || "")}">${escHtml(name)}</a>
          </div>
          <div class="trader-wallet">${shortWallet(t.proxy_wallet)}</div>
        </div>
        <div class="trader-stats">
          <div class="trader-volume">${fmtUSD(t.total_volume)}</div>
          <div class="trader-trades">${(t.trade_count || 0).toLocaleString()} trades</div>
        </div>
      </div>`;
  }).join("");
}

// ----------------------------------------------------------------
// WebSocket / status
// ----------------------------------------------------------------
async function loadStatus() {
  try {
    const s = await apiFetch("/api/status");
    const ws = s.ingestion?.ws_connected;
    const dotEl = document.querySelector("#wsIndicator .dot");
    if (dotEl) {
      dotEl.className = `dot dot--${ws === true ? "green" : ws === false ? "red" : "grey"}`;
    }
  } catch (_) {
    // non-critical
  }
}

// ----------------------------------------------------------------
// CSV export
// ----------------------------------------------------------------
function exportCsv() {
  const params = new URLSearchParams({ market_id: CFG.marketId, limit: 50000 });
  if (_filterState.minAmount)  params.set("min_amount", _filterState.minAmount);
  if (_filterState.whalesOnly) params.set("min_amount", CFG.whaleThreshold);
  window.location.href = `/api/export/csv?${params.toString()}`;
}

// ----------------------------------------------------------------
// Filters
// ----------------------------------------------------------------
function readFilters() {
  const minVal = parseFloat($("#filterMinAmount")?.value || "");
  _filterState.minAmount  = isNaN(minVal) ? null : minVal;
  _filterState.side       = $("#filterSide")?.value    || "";
  _filterState.outcome    = $("#filterOutcome")?.value  || "";
  _filterState.whalesOnly = $("#filterWhalesOnly")?.checked || false;

  if (_filterState.whalesOnly) {
    _filterState.minAmount = CFG.whaleThreshold;
    if ($("#filterMinAmount")) $("#filterMinAmount").value = CFG.whaleThreshold;
  }
}

function clearFilters() {
  _filterState = { minAmount: null, side: "", outcome: "", whalesOnly: false };
  if ($("#filterMinAmount"))  $("#filterMinAmount").value  = "";
  if ($("#filterSide"))       $("#filterSide").value       = "";
  if ($("#filterOutcome"))    $("#filterOutcome").value    = "";
  if ($("#filterWhalesOnly")) $("#filterWhalesOnly").checked = false;
}

// ----------------------------------------------------------------
// XSS protection
// ----------------------------------------------------------------
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
// Full refresh cycle
// ----------------------------------------------------------------
async function refresh() {
  _lastRefresh = Date.now();
  await Promise.allSettled([loadStats(), loadTrades(), loadTraders(), loadStatus()]);
}

function scheduleRefresh() {
  clearTimeout(_refreshTimer);
  _refreshTimer = setTimeout(async () => {
    await refresh();
    scheduleRefresh();
  }, UI_REFRESH_MS);
}

// ----------------------------------------------------------------
// Wire up events
// ----------------------------------------------------------------
document.addEventListener("DOMContentLoaded", () => {
  // Initial load
  refresh();
  scheduleRefresh();

  // Manual refresh
  document.getElementById("refreshBtn")?.addEventListener("click", async () => {
    clearTimeout(_refreshTimer);
    await refresh();
    scheduleRefresh();
    toast("Refreshed", "success", 1500);
  });

  // Filter apply
  document.getElementById("applyFilter")?.addEventListener("click", () => {
    readFilters();
    loadTrades();
  });

  // Filter clear
  document.getElementById("clearFilter")?.addEventListener("click", () => {
    clearFilters();
    loadTrades();
  });

  // Whale toggle auto-apply
  document.getElementById("filterWhalesOnly")?.addEventListener("change", () => {
    readFilters();
    loadTrades();
  });

  // CSV export
  document.getElementById("exportCsvBtn")?.addEventListener("click", exportCsv);

  // Sidebar toggle (mobile)
  const sidebarBtn = document.getElementById("sidebarToggle");
  const sidebar     = document.getElementById("sidebar");
  if (sidebarBtn && sidebar) {
    sidebarBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      sidebar.classList.toggle("sidebar--open");
    });
    document.addEventListener("click", () => {
      if (window.innerWidth < 768) {
        sidebar.classList.remove("sidebar--open");
      }
    });
  }
});
