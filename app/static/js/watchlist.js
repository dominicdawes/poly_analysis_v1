/* ================================================================
   watchlist.js — Watchlist feature
   Shared across all pages:
     - window.WL.showAddModal(type, identifier, label, onAdded?)
     - window.WL.checkWatchStatus(type, identifier) → Promise<{is_watched, watchlist_ids}>
   Full page init runs only when window.WL_PAGE === true
   (i.e. the /watchlist page).
================================================================ */

"use strict";

// ----------------------------------------------------------------
// Shared formatters (may coexist alongside wallet.js / main.js)
// ----------------------------------------------------------------
const _fmtUSD_WL = (v) =>
  v == null ? "—" :
  v >= 1_000_000 ? `$${(v / 1_000_000).toFixed(2)}M` :
  v >= 1_000     ? `$${(v / 1_000).toFixed(1)}k` :
                   `$${Number(v).toFixed(2)}`;

function _fmtRelative(ts) {
  if (!ts) return "—";
  const diff = Math.floor(Date.now() / 1000 - ts);
  if (diff < 60)          return "just now";
  if (diff < 3_600)       return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86_400)      return `${Math.floor(diff / 3_600)}h ago`;
  if (diff < 86_400 * 30) return `${Math.floor(diff / 86_400)}d ago`;
  return `${Math.floor(diff / (86_400 * 30))}mo ago`;
}

function _fmtAgeFromTs(ts) {
  if (!ts) return "—";
  const days = Math.floor((Date.now() / 1000 - ts) / 86_400);
  if (days === 0) return "Today";
  if (days === 1) return "1 day";
  return `${days.toLocaleString()} days`;
}

function _escHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function _shortAddr(addr) {
  if (!addr || addr.length < 10) return addr || "—";
  return addr.slice(0, 6) + "…" + addr.slice(-4);
}

// ----------------------------------------------------------------
// Toast (shared; guards against duplicate toast container)
// ----------------------------------------------------------------
function _toast(msg, type = "info", ms = 3500) {
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
async function _apiFetch(path, opts = {}) {
  const resp = await fetch(path, opts);
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}));
    const err  = new Error(body.error || `HTTP ${resp.status}`);
    err.status = resp.status;
    throw err;
  }
  return resp.json();
}

// ----------------------------------------------------------------
// Watch-status helper (exported on window.WL)
// ----------------------------------------------------------------
async function checkWatchStatus(type, identifier) {
  try {
    return await _apiFetch(
      `/api/watchlists/status?type=${encodeURIComponent(type)}&identifier=${encodeURIComponent(identifier)}`
    );
  } catch (_) {
    return { is_watched: false, watchlist_ids: [] };
  }
}

// ----------------------------------------------------------------
// Add-to-Watchlist modal (exported on window.WL)
// Injects a single .add-modal-overlay into <body> (created once).
// ----------------------------------------------------------------
let _modalEl = null;
let _modalOnAdded = null;

function showAddModal(type, identifier, displayLabel, onAdded) {
  _modalOnAdded = onAdded || null;

  // Fetch watchlists of the matching type, then build and show modal
  _apiFetch(`/api/watchlists?type=${encodeURIComponent(type)}`)
    .then(watchlists => _buildModal(type, identifier, displayLabel, watchlists))
    .catch(() => _toast("Could not load watchlists", "error"));
}

function _buildModal(type, identifier, displayLabel, watchlists) {
  // Remove old modal if present
  if (_modalEl) _modalEl.remove();

  const overlay = document.createElement("div");
  overlay.className = "add-modal-overlay";
  overlay.innerHTML = `
    <div class="add-modal" role="dialog" aria-modal="true">
      <div class="add-modal-title">
        <span style="margin-right:6px">👁</span> Add to Watchlist
      </div>
      <div class="add-modal-subtitle">${_escHtml(displayLabel || _shortAddr(identifier))}</div>

      <!-- Inline create new -->
      <div class="add-modal-new-row" id="amNewRow">
        <button class="btn btn--sm btn--outline" id="amNewToggle">+ New Watchlist</button>
        <input type="text" class="wallet-search-input" id="amNewInput"
               placeholder="Watchlist name…" maxlength="60" style="display:none;flex:1"/>
        <button class="btn btn--sm btn--primary" id="amNewCreate" style="display:none">Create</button>
      </div>
      <hr class="add-modal-divider" />

      <!-- Watchlist list with checkboxes -->
      <div class="add-modal-list" id="amList">
        ${watchlists.length === 0
          ? `<div style="color:var(--text-muted);font-size:12px;padding:8px">No watchlists yet — create one above.</div>`
          : watchlists.map(wl => `
            <label class="add-modal-row">
              <input type="checkbox" class="am-check" data-wl-id="${wl.id}" data-wl-name="${_escHtml(wl.name)}" />
              <span>${_escHtml(wl.name)}</span>
              ${wl.is_default ? '<span style="font-size:10px;color:var(--text-muted)">(default)</span>' : ''}
            </label>`).join("")
        }
      </div>

      <div class="add-modal-footer">
        <button class="btn btn--sm btn--ghost" id="amCancel">Cancel</button>
        <button class="btn btn--sm btn--primary" id="amDone">Done</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  _modalEl = overlay;

  // Pre-check watchlists the item is already in
  checkWatchStatus(type, identifier).then(status => {
    (status.watchlist_ids || []).forEach(wlId => {
      const cb = overlay.querySelector(`.am-check[data-wl-id="${wlId}"]`);
      if (cb) cb.checked = true;
    });
  });

  // Toggle inline create form
  overlay.querySelector("#amNewToggle").addEventListener("click", () => {
    const inp = overlay.querySelector("#amNewInput");
    const btn = overlay.querySelector("#amNewCreate");
    const tog = overlay.querySelector("#amNewToggle");
    const hidden = inp.style.display === "none";
    inp.style.display = hidden ? "" : "none";
    btn.style.display = hidden ? "" : "none";
    tog.style.display = hidden ? "none" : "";
    if (hidden) inp.focus();
  });

  overlay.querySelector("#amNewCreate").addEventListener("click", () => {
    const inp  = overlay.querySelector("#amNewInput");
    const name = inp.value.trim();
    if (!name) return;
    _apiFetch("/api/watchlists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, type }),
    })
      .then(newWl => {
        _toast(`Created "${newWl.name}"`, "success");
        // Reload modal with updated list
        _apiFetch(`/api/watchlists?type=${encodeURIComponent(type)}`)
          .then(wls => {
            _buildModal(type, identifier, displayLabel, wls);
          });
      })
      .catch(e => _toast(e.message || "Create failed", "error"));
  });

  overlay.querySelector("#amNewInput").addEventListener("keydown", e => {
    if (e.key === "Enter") overlay.querySelector("#amNewCreate").click();
    if (e.key === "Escape") overlay.querySelector("#amCancel").click();
  });

  // Close on overlay click (outside modal box)
  overlay.addEventListener("click", e => {
    if (e.target === overlay) _closeModal();
  });
  overlay.querySelector("#amCancel").addEventListener("click", _closeModal);

  // Done: add to all checked watchlists, remove from unchecked watchlists
  overlay.querySelector("#amDone").addEventListener("click", async () => {
    const checkboxes = Array.from(overlay.querySelectorAll(".am-check"));
    let changed = false;

    await Promise.allSettled(checkboxes.map(async cb => {
      const wlId   = parseInt(cb.dataset.wlId, 10);
      const wlName = cb.dataset.wlName;
      const status = await checkWatchStatus(type, identifier);
      const inList = status.watchlist_ids.includes(wlId);

      if (cb.checked && !inList) {
        // Add
        await _apiFetch(`/api/watchlists/${wlId}/items`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ identifier, label: displayLabel || null }),
        });
        changed = true;
        _toast(`Added to "${wlName}"`, "success");
      } else if (!cb.checked && inList) {
        // Find item_id and remove
        const items = await _apiFetch(`/api/watchlists/${wlId}/items`);
        const item  = items.find(i => i.identifier === identifier);
        if (item) {
          await _apiFetch(`/api/watchlists/${wlId}/items/${item.id}`, { method: "DELETE" });
          changed = true;
          _toast(`Removed from "${wlName}"`, "info");
        }
      }
    }));

    _closeModal();
    if (changed && typeof _modalOnAdded === "function") _modalOnAdded();

    // Update eye icon on the current page if present
    _refreshEyeOnPage(type, identifier);
  });
}

function _closeModal() {
  if (_modalEl) {
    _modalEl.remove();
    _modalEl = null;
  }
}

async function _refreshEyeOnPage(type, identifier) {
  const status = await checkWatchStatus(type, identifier);
  if (type === "market") {
    _applyEyeState(document.getElementById("marketEyeBtn"), status.is_watched, "market");
  } else if (type === "wallet") {
    _applyEyeState(document.getElementById("walletEyeBtn"), status.is_watched, "wallet");
  }
}

function _applyEyeState(btn, isWatched, type) {
  if (!btn) return;
  btn.classList.toggle("eye-btn--watched",   isWatched);
  btn.classList.toggle("eye-btn--unwatched", !isWatched);
  btn.innerHTML = isWatched
    ? "Watchlist 👁"
    : (type === "wallet" ? "Add Wallet to Watchlist 👁" : "Add Market to Watchlist 👁");
}

// Export shared API for other pages
window.WL = { showAddModal, checkWatchStatus };

// ================================================================
// Watchlist page — runs only on /watchlist
// ================================================================
if (!window.WL_PAGE) {
  // Not the watchlist page — nothing more to do
} else {
  // ── Page state ────────────────────────────────────────────────
  let _activeType  = "wallet";
  let _watchlists  = [];
  let _activeWlId  = null;
  let _activeWlName = "";

  // Dismiss overflow menu on outside click
  let _openOverflowMenu = null;
  document.addEventListener("click", () => {
    if (_openOverflowMenu) {
      _openOverflowMenu.remove();
      _openOverflowMenu = null;
    }
  });

  // ── Sidebar toggle ────────────────────────────────────────────
  function _initSidebar() {
    const btn     = document.getElementById("sidebarToggle");
    const sidebar = document.getElementById("sidebar");
    if (!btn || !sidebar) return;
    btn.addEventListener("click", e => {
      e.stopPropagation();
      sidebar.classList.toggle("sidebar--open");
    });
    document.addEventListener("click", () => {
      if (window.innerWidth < 768) sidebar.classList.remove("sidebar--open");
    });
  }

  // ── Load watchlists and render selector pills ─────────────────
  async function _loadWatchlists(type) {
    _activeType = type;
    try {
      _watchlists = await _apiFetch(`/api/watchlists?type=${encodeURIComponent(type)}`);
    } catch (_) {
      _watchlists = [];
    }
    _renderSelector();

    // Auto-select first watchlist
    if (_watchlists.length > 0) {
      _selectWatchlist(_watchlists[0].id, _watchlists[0].name);
    } else {
      _renderCards([]);
      document.getElementById("wlActions").style.display = "none";
    }
  }

  function _renderSelector() {
    const sel = document.getElementById("wlSelector");
    if (!sel) return;
    if (_watchlists.length === 0) {
      sel.innerHTML = '<span style="color:var(--text-muted);font-size:12px">No watchlists yet.</span>';
      return;
    }
    sel.innerHTML = _watchlists.map(wl => `
      <button
        class="wl-pill${wl.id === _activeWlId ? " wl-pill--active" : ""}"
        data-wl-id="${wl.id}"
        data-wl-name="${_escHtml(wl.name)}"
        data-is-default="${wl.is_default}"
      >${_escHtml(wl.name)}</button>
    `).join("");

    sel.querySelectorAll(".wl-pill").forEach(pill => {
      pill.addEventListener("click", () => {
        _selectWatchlist(parseInt(pill.dataset.wlId, 10), pill.dataset.wlName);
      });
    });
  }

  function _selectWatchlist(wlId, wlName) {
    _activeWlId   = wlId;
    _activeWlName = wlName;
    // Re-render selector to update active pill
    _renderSelector();
    // Show actions bar (only if not only default remains)
    const wl = _watchlists.find(w => w.id === wlId);
    const actionsBar = document.getElementById("wlActions");
    if (actionsBar) {
      actionsBar.style.display = wl ? "" : "none";
      const delBtn = document.getElementById("wlDeleteBtn");
      if (delBtn) delBtn.style.display = wl && wl.is_default ? "none" : "";
    }
    _loadItems(wlId);
  }

  // ── Load items (enriched) ─────────────────────────────────────
  async function _loadItems(wlId) {
    const cards = document.getElementById("wlCards");
    const empty = document.getElementById("wlEmpty");
    if (!cards) return;

    // Show loading state
    cards.innerHTML = '<div style="color:var(--text-muted);padding:20px 0">Loading…</div>';
    if (empty) empty.style.display = "none";

    let items = [];
    try {
      items = await _apiFetch(`/api/watchlists/${wlId}/enriched`);
    } catch (_) {
      cards.innerHTML = '<div style="color:var(--red);padding:20px 0">Failed to load items.</div>';
      return;
    }

    if (items.length === 0) {
      cards.innerHTML = "";
      if (empty) empty.style.display = "";
      return;
    }

    if (empty) empty.style.display = "none";
    cards.innerHTML = items.map(item =>
      _activeType === "wallet" ? _walletCardHtml(item) : _marketCardHtml(item)
    ).join("");

    // Wire up card clicks + overflow buttons
    cards.querySelectorAll(".wl-card[data-identifier]").forEach(card => {
      const idf = card.dataset.identifier;
      card.addEventListener("click", e => {
        // Ignore clicks on overflow btn or menu
        if (e.target.closest(".overflow-btn")) return;
        if (_activeType === "wallet") {
          window.location.href = `/wallet-dashboard?address=${encodeURIComponent(idf)}`;
        } else {
          window.location.href = `/?market_id=${encodeURIComponent(idf)}`;
        }
      });
    });

    cards.querySelectorAll(".overflow-btn").forEach(btn => {
      btn.addEventListener("click", e => {
        e.stopPropagation();
        _showOverflowMenu(
          parseInt(btn.dataset.itemId, 10),
          wlId,
          btn.dataset.identifier,
          e.currentTarget,
        );
      });
    });
  }

  // ── Card renderers ────────────────────────────────────────────
  function _walletCardHtml(item) {
    const name    = _escHtml(item.display_name || _shortAddr(item.identifier));
    const addr    = _escHtml(_shortAddr(item.identifier));
    const age     = _fmtAgeFromTs(item.first_seen);
    const pnl     = item.realized_pnl;
    const pnlStr  = pnl == null ? "—" : (pnl >= 0 ? "+" : "") + _fmtUSD_WL(pnl);
    const pnlColor = pnl == null ? "" : pnl >= 0 ? "var(--green)" : "var(--red)";
    const lastTrade = _fmtRelative(item.last_seen);
    const numPred   = item.num_active_positions ?? "—";

    return `
      <div class="wl-card" data-identifier="${_escHtml(item.identifier)}">
        <div class="wl-card-header">
          <div>
            <div class="wl-card-name">${name}</div>
            <div class="wl-card-addr">${addr}</div>
          </div>
          <button class="overflow-btn" data-item-id="${item.id}" data-identifier="${_escHtml(item.identifier)}" title="Options">•••</button>
        </div>
        <div class="wl-card-stats">
          <div class="wl-card-stat">
            <div class="wl-card-stat-label">Wallet Age</div>
            <div class="wl-card-stat-value">${age}</div>
          </div>
          <div class="wl-card-stat">
            <div class="wl-card-stat-label">Realized PnL</div>
            <div class="wl-card-stat-value" style="color:${pnlColor}">${pnlStr}</div>
          </div>
          <div class="wl-card-stat">
            <div class="wl-card-stat-label">Last Trade</div>
            <div class="wl-card-stat-value">${lastTrade}</div>
          </div>
          <div class="wl-card-stat">
            <div class="wl-card-stat-label">Open Positions</div>
            <div class="wl-card-stat-value">${numPred}</div>
          </div>
        </div>
      </div>
    `;
  }

  function _marketCardHtml(item) {
    const title   = item.display_name || item.identifier;
    const titleSh = title.length > 60 ? title.slice(0, 57) + "…" : title;
    const vol     = _fmtUSD_WL(item.total_volume);
    const cat     = item.category || null;
    const idf     = item.identifier;

    return `
      <div class="wl-card" data-identifier="${_escHtml(idf)}">
        <div class="wl-card-header">
          <div class="wl-card-name" title="${_escHtml(title)}">${_escHtml(titleSh)}</div>
          <div style="display:flex;align-items:center;gap:6px">
            <button class="btn btn--sm btn--ghost copy-cid-btn" data-cid="${_escHtml(idf)}" title="Copy condition ID">📋</button>
            <button class="overflow-btn" data-item-id="${item.id}" data-identifier="${_escHtml(idf)}" title="Options">•••</button>
          </div>
        </div>
        <div class="wl-card-stats">
          ${cat ? `<div class="wl-card-stat"><div class="wl-card-stat-label">Category</div><div class="wl-card-stat-value"><span class="category-badge">${_escHtml(cat)}</span></div></div>` : ""}
          <div class="wl-card-stat">
            <div class="wl-card-stat-label">Total Volume</div>
            <div class="wl-card-stat-value">${vol}</div>
          </div>
          ${item.end_date ? `<div class="wl-card-stat"><div class="wl-card-stat-label">End Date</div><div class="wl-card-stat-value">${_escHtml(item.end_date.slice(0, 10))}</div></div>` : ""}
          ${item.resolved ? `<div class="wl-card-stat"><div class="wl-card-stat-label">Status</div><div class="wl-card-stat-value" style="color:var(--text-muted)">Resolved</div></div>` : ""}
        </div>
      </div>
    `;
  }

  // Wire copy buttons after rendering (delegated)
  function _wireCopyButtons(container) {
    container.querySelectorAll(".copy-cid-btn").forEach(btn => {
      btn.addEventListener("click", e => {
        e.stopPropagation();
        const cid = btn.dataset.cid;
        navigator.clipboard.writeText(cid).then(() => {
          _toast("Condition ID copied!", "success", 2000);
        }).catch(() => _toast("Copy failed", "error"));
      });
    });
  }

  // ── Overflow menu ─────────────────────────────────────────────
  function _showOverflowMenu(itemId, wlId, identifier, anchorEl) {
    if (_openOverflowMenu) {
      _openOverflowMenu.remove();
      _openOverflowMenu = null;
      return;
    }

    const others = _watchlists.filter(w => w.id !== wlId);
    const moveItems = others.length > 0
      ? `<div class="overflow-menu-sep"></div>
         <div class="overflow-menu-label">Move to…</div>
         ${others.map(w => `
           <div class="overflow-menu-item" data-action="move" data-to-id="${w.id}" data-to-name="${_escHtml(w.name)}">
             ${_escHtml(w.name)}
           </div>`).join("")}`
      : "";

    const menu = document.createElement("div");
    menu.className = "overflow-menu";
    menu.innerHTML = `
      <div class="overflow-menu-item" data-action="remove">Remove from watchlist</div>
      ${moveItems}
    `;

    // Position below the anchor
    document.body.appendChild(menu);
    const rect = anchorEl.getBoundingClientRect();
    menu.style.top  = `${rect.bottom + window.scrollY + 4}px`;
    menu.style.left = `${Math.max(rect.right - menu.offsetWidth, 8) + window.scrollX}px`;
    _openOverflowMenu = menu;

    menu.addEventListener("click", async e => {
      e.stopPropagation();
      const item = e.target.closest(".overflow-menu-item");
      if (!item) return;
      const action = item.dataset.action;

      if (action === "remove") {
        try {
          await _apiFetch(`/api/watchlists/${wlId}/items/${itemId}`, { method: "DELETE" });
          _toast(`Removed from "${_activeWlName}"`, "info");
          _loadItems(wlId);
        } catch (ex) {
          _toast(ex.message || "Remove failed", "error");
        }
      } else if (action === "move") {
        const toId   = parseInt(item.dataset.toId, 10);
        const toName = item.dataset.toName;
        try {
          await _apiFetch("/api/watchlists/move-item", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ item_id: itemId, to_watchlist_id: toId }),
          });
          _toast(`Moved to "${toName}"`, "success");
          _loadItems(wlId);
        } catch (ex) {
          _toast(ex.message || "Move failed", "error");
        }
      }

      menu.remove();
      _openOverflowMenu = null;
    });
  }

  // ── Create / rename / delete watchlist UI ─────────────────────
  function _initToolbar() {
    const newBtn    = document.getElementById("wlNewBtn");
    const newForm   = document.getElementById("wlNewForm");
    const newInput  = document.getElementById("wlNewInput");
    const newConfirm = document.getElementById("wlNewConfirm");
    const newCancel = document.getElementById("wlNewCancel");

    newBtn.addEventListener("click", () => {
      newBtn.style.display   = "none";
      newForm.style.display  = "";
      newInput.value = "";
      newInput.focus();
    });

    newCancel.addEventListener("click", () => {
      newForm.style.display = "none";
      newBtn.style.display  = "";
    });

    async function doCreate() {
      const name = newInput.value.trim();
      if (!name) return;
      try {
        const wl = await _apiFetch("/api/watchlists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name, type: _activeType }),
        });
        _toast(`Created "${wl.name}"`, "success");
        newForm.style.display = "none";
        newBtn.style.display  = "";
        await _loadWatchlists(_activeType);
        _selectWatchlist(wl.id, wl.name);
      } catch (e) {
        _toast(e.message || "Create failed", "error");
      }
    }

    newConfirm.addEventListener("click", doCreate);
    newInput.addEventListener("keydown", e => {
      if (e.key === "Enter")  doCreate();
      if (e.key === "Escape") newCancel.click();
    });

    // Rename
    document.getElementById("wlRenameBtn")?.addEventListener("click", async () => {
      const name = prompt("New watchlist name:", _activeWlName);
      if (!name || !name.trim() || name.trim() === _activeWlName) return;
      try {
        await _apiFetch(`/api/watchlists/${_activeWlId}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: name.trim() }),
        });
        _toast("Renamed", "success");
        await _loadWatchlists(_activeType);
      } catch (e) {
        _toast(e.message || "Rename failed", "error");
      }
    });

    // Delete
    document.getElementById("wlDeleteBtn")?.addEventListener("click", async () => {
      if (!confirm(`Delete "${_activeWlName}"? This cannot be undone.`)) return;
      try {
        await _apiFetch(`/api/watchlists/${_activeWlId}`, { method: "DELETE" });
        _toast(`Deleted "${_activeWlName}"`, "info");
        await _loadWatchlists(_activeType);
      } catch (e) {
        _toast(e.message || "Delete failed", "error");
      }
    });
  }

  // ── Sub-tab wiring ────────────────────────────────────────────
  function _initTabs() {
    document.querySelectorAll(".wl-tab").forEach(tab => {
      tab.addEventListener("click", () => {
        document.querySelectorAll(".wl-tab").forEach(t => t.classList.remove("wl-tab--active"));
        tab.classList.add("wl-tab--active");
        _loadWatchlists(tab.dataset.type);
      });
    });
  }

  // ── DOMContentLoaded ──────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", () => {
    _initSidebar();
    _initTabs();
    _initToolbar();

    // Wire copy buttons via delegation on the cards container
    const cardsEl = document.getElementById("wlCards");
    if (cardsEl) {
      cardsEl.addEventListener("click", e => {
        const btn = e.target.closest(".copy-cid-btn");
        if (!btn) return;
        e.stopPropagation();
        navigator.clipboard.writeText(btn.dataset.cid)
          .then(() => _toast("Condition ID copied!", "success", 2000))
          .catch(() => _toast("Copy failed", "error"));
      });
    }

    _loadWatchlists("wallet");
  });
}
