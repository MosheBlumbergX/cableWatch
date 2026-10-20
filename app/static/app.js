// Cable Watch dashboard: one SSE stream from the server, drawn on a MapLibre chart.
"use strict";

const COLORS = {
  contact: "#a9d1c6",
  corridor: "#e6f2ee",
  magenta: "#d24c9c",
  amber: "#f2a93b",
  red: "#ff5a4e",
  water: "#0b1f2e",
  land: "#1f2c2a",
  countryLabel: "#7f9693",
};
const NAV_STATUS = {
  0: "under way using engine", 1: "at anchor", 2: "not under command", 3: "restricted manoeuvrability",
  4: "constrained by draught", 5: "moored", 6: "aground", 7: "engaged in fishing", 8: "under way sailing",
  14: "AIS-SART", 15: "not defined",
};
// Event topics are shown as running totals; the busy ones as a rate.
const COUNTED = new Set(["loiter_events", "drag_events", "dark_events", "risk_signals", "vessel_risk", "incidents",
  "incident_briefs", "ntfy_events", "pagerduty_events", "slack_events", "ais_position_dlq",
  "ntfy_events_dlq", "pagerduty_events_dlq", "slack_events_dlq"]);
const STALE_MS = 15 * 60 * 1000;
const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const vessels = new Map();   // mmsi -> row
const risk = new Map();      // "mmsi|cable" -> row
const dark = new Map();      // mmsi -> row
const incidentKeys = new Set();
// Service craft (pilots, tugs, rescue, police) wait over cables as part of their
// job, so they are left out unless the viewer asks for them.
let includeService = localStorage.getItem("cw-include-service") === "1";
const counts = (r) => includeService || !r.service;
// A score only counts signals from the last hour, so a risk state older than that
// is NORMAL by definition, whether or not its CLEAR has arrived yet.
const RISK_TTL_MS = 65 * 60 * 1000;
const active = (r) => r.state !== "NORMAL" && now - r.t < RISK_TTL_MS;
let listMode = null;   // null = incident log; else "tracked" | "corridor" | "watch" | "dark"
let listSearch = "";
let listKey = "";      // what the list last rendered, to avoid rebuilding it every second
let selected = null;
let selectedPos = null;  // [lon, lat] of the selected vessel's latest fix (or last-seen position if dark)
let mapReady = false;
let now = Date.now();

const $ = (id) => document.getElementById(id);
const hhmm = (t) => new Date(t).toISOString().slice(11, 16);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const ago = (t) => {
  const m = Math.max(0, Math.round((now - t) / 60000));
  return m < 60 ? `${m} min` : `${Math.floor(m / 60)} h ${m % 60} min`;
};

// ---------------------------------------------------------------------------
// Map
// ---------------------------------------------------------------------------
const map = new maplibregl.Map({
  container: "map",
  style: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  bounds: [[9.5, 53.5], [30.5, 66.0]],
  fitBoundsOptions: { padding: { top: 40, bottom: 110, left: 390, right: 40 } },
  minZoom: 3,
  maxZoom: 13,
  attributionControl: false, // credited in the footer, which keeps the zoom buttons clear
});
map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "bottom-right");

// Chart-night palette over the CARTO base: deep blue water, slate land, and
// country names as the only base-map labels.
function recolorBase() {
  for (const layer of map.getStyle().layers) {
    if (layer.type === "symbol") {
      if (/^place_country/.test(layer.id)) {
        map.setLayerZoomRange(layer.id, 2, 9);
        map.setPaintProperty(layer.id, "text-color", COLORS.countryLabel);
        map.setPaintProperty(layer.id, "text-halo-color", COLORS.land);
        map.setPaintProperty(layer.id, "text-halo-width", 1.2);
      } else {
        map.setLayoutProperty(layer.id, "visibility", "none");
      }
    } else if (layer.type === "background") map.setPaintProperty(layer.id, "background-color", COLORS.land);
    else if (layer.type === "fill" && /water/.test(layer.id)) map.setPaintProperty(layer.id, "fill-color", COLORS.water);
    else if (layer.type === "line" && /water|river/.test(layer.id)) map.setPaintProperty(layer.id, "line-color", COLORS.water);
    else if (layer.type === "fill" && /landcover|park|landuse|building/.test(layer.id)) map.setPaintProperty(layer.id, "fill-opacity", 0.15);
    else if (layer.type === "line" && /boundary/.test(layer.id)) map.setPaintProperty(layer.id, "line-color", "#2c4552");
  }
}

// Chart symbols drawn on a canvas. Submarine cables are wavy magenta lines on nautical charts.
function makeImages() {
  const wave = document.createElement("canvas");
  wave.width = 24; wave.height = 10;
  let g = wave.getContext("2d");
  g.strokeStyle = COLORS.magenta; g.lineWidth = 1.6;
  g.beginPath();
  for (let x = 0; x <= 24; x++) {
    const y = 5 + 3 * Math.sin((x / 24) * Math.PI * 2);
    x ? g.lineTo(x, y) : g.moveTo(x, y);
  }
  g.stroke();
  map.addImage("cable-wave", g.getImageData(0, 0, 24, 10), { pixelRatio: 1 });

  const ship = document.createElement("canvas");
  ship.width = ship.height = 32;
  g = ship.getContext("2d");
  g.fillStyle = "#fff";
  g.beginPath(); g.moveTo(16, 2); g.lineTo(26, 29); g.lineTo(16, 23); g.lineTo(6, 29); g.closePath(); g.fill();
  map.addImage("ship", g.getImageData(0, 0, 32, 32), { sdf: true, pixelRatio: 2 });

  const dot = document.createElement("canvas");
  dot.width = dot.height = 32;
  g = dot.getContext("2d");
  g.fillStyle = "#fff";
  g.beginPath(); g.arc(16, 16, 8, 0, Math.PI * 2); g.fill();
  map.addImage("dot", g.getImageData(0, 0, 32, 32), { sdf: true, pixelRatio: 2 });
}

const empty = () => ({ type: "FeatureCollection", features: [] });

// Set up as soon as the style is parsed, without waiting for every base-map tile.
map.once("style.load", async () => {
  recolorBase();
  makeImages();

  const cables = await (await fetch("/api/cables")).json();
  map.addSource("cables", { type: "geojson", data: cables, promoteId: "cable_id" });
  const stateColor = ["match", ["feature-state", "state"], "ALERT", COLORS.red, "WATCH", COLORS.amber, COLORS.magenta];
  map.addLayer({
    id: "cable-glow", type: "line", source: "cables",
    paint: {
      "line-color": stateColor,
      "line-width": ["case", ["has", "state"], 14, 7],
      "line-blur": 6,
      "line-opacity": ["match", ["feature-state", "state"], "ALERT", 0.55, "WATCH", 0.4, 0.12],
    },
  });
  map.addLayer({
    id: "cable-wave", type: "line", source: "cables", filter: ["!=", ["get", "kind"], "pipeline"],
    layout: { "line-join": "round" },
    paint: { "line-pattern": "cable-wave", "line-width": 6, "line-opacity": 0.9 },
  });
  map.addLayer({
    id: "pipeline-dash", type: "line", source: "cables", filter: ["==", ["get", "kind"], "pipeline"],
    paint: { "line-color": COLORS.magenta, "line-width": 1.6, "line-dasharray": [4, 3], "line-opacity": 0.9 },
  });
  map.addLayer({
    id: "cable-alert", type: "line", source: "cables",
    filter: ["boolean", false],
    paint: { "line-color": stateColor, "line-width": 2.5 },
  });
  map.addLayer({
    id: "cable-names", type: "symbol", source: "cables", minzoom: 6.5,
    layout: {
      "symbol-placement": "line", "text-field": ["get", "name"], "text-size": 11,
      "text-font": ["Open Sans Regular"], "symbol-spacing": 400,
    },
    paint: { "text-color": "#c98ab3", "text-halo-color": COLORS.water, "text-halo-width": 1.5 },
  });

  map.addSource("track", { type: "geojson", data: empty() });
  map.addLayer({
    id: "track", type: "line", source: "track",
    paint: { "line-color": COLORS.corridor, "line-width": 1.5, "line-opacity": 0.7, "line-dasharray": [1, 1.5] },
  });

  // A ring around the selected vessel, so "Show current position" lands somewhere visible.
  map.addSource("selected", { type: "geojson", data: empty() });
  map.addLayer({
    id: "selected", type: "circle", source: "selected",
    paint: { "circle-radius": 14, "circle-color": "rgba(0,0,0,0)", "circle-stroke-color": COLORS.corridor, "circle-stroke-width": 1.5 },
  });

  map.addSource("ghosts", { type: "geojson", data: empty() });
  map.addLayer({
    id: "ghosts", type: "circle", source: "ghosts",
    paint: {
      "circle-radius": 9, "circle-color": "rgba(0,0,0,0)",
      "circle-stroke-color": COLORS.amber, "circle-stroke-width": 1.5,
      "circle-stroke-opacity": 0.9,
    },
  });
  map.addLayer({
    id: "ghost-labels", type: "symbol", source: "ghosts",
    layout: {
      "text-field": ["concat", ["get", "name"], "\nsilent ", ["get", "silent"]], "text-size": 11,
      "text-font": ["Open Sans Regular"], "text-offset": [0, 1.6], "text-anchor": "top",
    },
    paint: { "text-color": COLORS.amber, "text-halo-color": COLORS.water, "text-halo-width": 1.5 },
  });

  map.addSource("vessels", { type: "geojson", data: empty() });
  map.addLayer({
    id: "vessels", type: "symbol", source: "vessels",
    layout: {
      "icon-image": ["case", [">=", ["get", "sog"], 0.8], "ship", "dot"],
      "icon-size": ["interpolate", ["linear"], ["zoom"], 4, ["case", [">", ["get", "score"], 0], 0.75, 0.42], 10, 1],
      "icon-rotate": ["coalesce", ["get", "hdg"], 0],
      "icon-rotation-alignment": "map",
      "icon-allow-overlap": true,
      "icon-ignore-placement": true, // never push country names off the map
      "symbol-sort-key": ["get", "score"],
    },
    paint: {
      "icon-color": ["case",
        [">=", ["get", "score"], 60], COLORS.red,
        [">=", ["get", "score"], 30], COLORS.amber,
        ["get", "corridor"], COLORS.corridor,
        COLORS.contact],
      "icon-opacity": ["case", ["get", "dim"], 0.08, ["get", "stale"], 0.25, ["get", "corridor"], 1, 0.6],
    },
  });
  map.addLayer({
    id: "vessel-labels", type: "symbol", source: "vessels",
    filter: [">", ["get", "score"], 0],
    layout: {
      "text-field": ["get", "name"], "text-size": 12, "text-font": ["Montserrat Medium", "Open Sans Bold"], // fonts the CARTO glyph server has
      "text-offset": [0, 1.3], "text-anchor": "top",
    },
    paint: {
      "text-color": ["case", [">=", ["get", "score"], 60], COLORS.red, COLORS.amber],
      "text-halo-color": COLORS.water, "text-halo-width": 1.5,
    },
  });

  // The selected vessel's own name, above the other labels: placed first, it wins
  // when ships share a spot (two pilot boats at one station, say).
  map.addLayer({
    id: "selected-label", type: "symbol", source: "selected",
    layout: {
      "text-field": ["get", "name"], "text-size": 12, "text-font": ["Montserrat Medium", "Open Sans Bold"],
      "text-offset": [0, 1.5], "text-anchor": "top", "text-allow-overlap": true,
    },
    paint: { "text-color": COLORS.corridor, "text-halo-color": COLORS.water, "text-halo-width": 1.5 },
  });

  const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12 });
  map.on("mousemove", "vessels", (e) => {
    const p = e.features[0].properties;
    map.getCanvas().style.cursor = "pointer";
    popup.setLngLat(e.lngLat).setHTML(
      `<div class="popup-name">${esc(p.name)}</div>${p.sog} kn, ${esc(NAV_STATUS[p.nav] || "status unknown")}`
    ).addTo(map);
  });
  map.on("mouseleave", "vessels", () => { map.getCanvas().style.cursor = ""; popup.remove(); });
  map.on("click", "vessels", (e) => openVessel(Number(e.features[0].properties.mmsi), true));
  map.on("click", "ghosts", (e) => openVessel(Number(e.features[0].properties.mmsi), true));
  map.on("mousemove", "cable-glow", (e) => {
    if (map.queryRenderedFeatures(e.point, { layers: ["vessels"] }).length) return;
    const p = e.features[0].properties;
    popup.setLngLat(e.lngLat).setHTML(
      `<div class="popup-name">${esc(p.name)}</div>${esc(p.kind)}, ${p.length_km} km${p.operator ? `, ${esc(p.operator)}` : ""}`
    ).addTo(map);
  });
  map.on("mouseleave", "cable-glow", () => popup.remove());

  mapReady = true;
  redraw();
  if (!reduceMotion) requestAnimationFrame(pulse);
  const wanted = Number(new URLSearchParams(location.search).get("mmsi"));
  if (wanted) openVessel(wanted, true);
});

// The one animation: cables under an ALERT breathe red.
function pulse(t) {
  if (map.getLayer("cable-alert")) {
    const k = (Math.sin(t / 380) + 1) / 2;
    map.setPaintProperty("cable-alert", "line-width", 2 + 4 * k);
    map.setPaintProperty("cable-alert", "line-opacity", 0.5 + 0.5 * k);
  }
  requestAnimationFrame(pulse);
}

// ---------------------------------------------------------------------------
// Drawing
// ---------------------------------------------------------------------------
let cableStates = new Map();

// The vessels behind each counter; the counters, the list and the map dimming all use these.
function vesselSets() {
  const live = [...vessels.values()].filter((v) => now - v.t < STALE_MS && !dark.has(v.mmsi));
  const watch = new Set([...risk.values()].filter((r) => active(r) && counts(r)).map((r) => r.mmsi));
  return {
    tracked: new Set(live.map((v) => v.mmsi)),
    corridor: new Set(live.filter((v) => v.corridor).map((v) => v.mmsi)),
    watch,
    dark: new Set(dark.keys()),
  };
}

function redraw() {
  const sets = vesselSets();
  $("count-vessels").textContent = sets.tracked.size.toLocaleString();
  $("count-corridor").textContent = sets.corridor.size;
  $("count-watch").textContent = sets.watch.size;
  $("count-dark").textContent = sets.dark.size;
  renderList(sets);
  const focus = listMode && listMode !== "tracked" ? sets[listMode] : null;
  if (!mapReady) return;

  const features = [];
  for (const v of vessels.values()) {
    if (dark.has(v.mmsi)) continue;
    const stale = now - v.t > STALE_MS;
    features.push({
      type: "Feature",
      geometry: { type: "Point", coordinates: [v.lon, v.lat] },
      properties: {
        mmsi: v.mmsi, sog: v.sog, hdg: v.hdg, nav: v.nav, score: counts(v) ? v.score : 0, corridor: !!v.corridor,
        stale, dim: !!focus && !focus.has(v.mmsi), name: v.name || `MMSI ${v.mmsi}`,
      },
    });
  }
  map.getSource("vessels").setData({ type: "FeatureCollection", features });

  map.getSource("ghosts").setData({
    type: "FeatureCollection",
    features: [...dark.values()].map((d) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [d.lon, d.lat] },
      properties: { mmsi: d.mmsi, name: d.name || `MMSI ${d.mmsi}`, silent: ago(d.last_seen) },
    })),
  });

  // Corridor state = worst open state of any vessel over that cable.
  const next = new Map();
  for (const r of risk.values()) {
    if (!active(r) || !counts(r)) continue;
    if (next.get(r.cable_id) !== "ALERT") next.set(r.cable_id, r.state);
  }
  for (const id of cableStates.keys()) if (!next.has(id)) map.removeFeatureState({ source: "cables", id }, "state");
  for (const [id, s] of next) map.setFeatureState({ source: "cables", id }, { state: s });
  cableStates = next;
  const alerting = [...next].filter(([, s]) => s === "ALERT").map(([id]) => id);
  map.setFilter("cable-alert", ["in", ["get", "cable_id"], ["literal", alerting]]);
}

function renderPipeline(rates) {
  const items = rates.topics.map((s) => {
    const counted = COUNTED.has(s.topic);
    const value = counted ? s.total.toLocaleString() : s.rate == null ? "…" : `${s.rate.toFixed(s.rate < 10 ? 1 : 0)}/s`;
    const busy = s.rate > 0 ? " busy" : "";
    const dlq = /dlq/.test(s.topic) ? " dlq" : "";
    return `<li class="stage${busy}${dlq}" title="${esc(s.topic)}"><span class="rate">${value}</span><span class="label">${esc(s.label)}</span></li>`;
  });
  $("pipeline").innerHTML = items.join("");
}

function addIncident(i) {
  const key = `${i.mmsi}|${i.cable_id}|${i.t}|${i.action}`;
  const existing = document.querySelector(`[data-key="${CSS.escape(key)}"]`);
  const label = { TRIGGER: "Alert", WATCH: "Watch", RESOLVE: "Resolved" }[i.action] || i.action;
  const html = `<time datetime="${new Date(i.t).toISOString()}">${hhmm(i.t)}</time>
    <div><div class="what"><span class="tag tag-${esc(i.action)}">${label}</span>${esc(i.summary)}</div>
    <p class="brief">${esc(i.brief)}</p></div>`;
  if (existing) { existing.innerHTML = html; return; }
  incidentKeys.add(key);
  const li = document.createElement("li");
  li.className = "incident" + (i.service ? " service" : "") + (incidentKeys.size > 1 && Date.now() - i.t < 120000 ? " fresh" : "");
  li.dataset.key = key;
  li.dataset.t = i.t;
  li.tabIndex = 0;
  li.innerHTML = html;
  li.addEventListener("click", () => openVessel(i.mmsi, true, [i.lon, i.lat]));
  li.addEventListener("keydown", (e) => { if (e.key === "Enter") openVessel(i.mmsi, true, [i.lon, i.lat]); });
  const list = $("incidents");
  const after = [...list.children].find((c) => Number(c.dataset.t) < i.t);
  list.insertBefore(li, after || null);
  updateEmpty();
}

function updateEmpty() {
  if (listMode) return;
  const visible = [...$("incidents").children].some((li) => includeService || !li.classList.contains("service"));
  $("incidents-empty").hidden = visible;
}

function applyServiceToggle() {
  document.body.classList.toggle("hide-service", !includeService);
  $("include-service").checked = includeService;
  updateEmpty();
  redraw();
}
$("include-service").addEventListener("change", (e) => {
  includeService = e.target.checked;
  localStorage.setItem("cw-include-service", includeService ? "1" : "0");
  applyServiceToggle();
});

// ---------------------------------------------------------------------------
// Vessel lists behind the counters
// ---------------------------------------------------------------------------
const LIST_TITLES = { tracked: "Vessels tracked", corridor: "Over a cable now", watch: "On watch", dark: "Gone silent" };
const LIST_CAP = 300;

function listRows(mode, sets) {
  const name = (m) => vessels.get(m)?.name || dark.get(m)?.name || `MMSI ${m}`;
  if (mode === "dark") {
    return [...dark.values()].sort((a, b) => a.last_seen - b.last_seen).map((d) => ({
      mmsi: d.mmsi, name: name(d.mmsi), detail: `silent ${ago(d.last_seen)}, ${d.distance_km} km from ${d.cable_name}`,
    }));
  }
  if (mode === "watch") {
    const best = new Map();
    for (const r of risk.values()) {
      if (!sets.watch.has(r.mmsi) || !active(r)) continue;
      if (!best.has(r.mmsi) || r.score > best.get(r.mmsi).score) best.set(r.mmsi, r);
    }
    return [...best.values()].sort((a, b) => b.score - a.score).map((r) => ({
      mmsi: r.mmsi, name: name(r.mmsi), score: r.score, state: r.state,
      detail: `${r.cable_name}${r.watch ? `, watch list: ${r.watch}` : ""}`,
    }));
  }
  return [...sets[mode]].map((m) => vessels.get(m)).sort((a, b) => (a.name || "~").localeCompare(b.name || "~")).map((v) => ({
    mmsi: v.mmsi, name: name(v.mmsi), detail: `${v.sog} kn, ${NAV_STATUS[v.nav] || "status unknown"}`,
  }));
}

function renderList(sets) {
  if (!listMode) return;
  let rows = listRows(listMode, sets);
  const q = listSearch.trim().toLowerCase();
  if (q) rows = rows.filter((r) => r.name.toLowerCase().includes(q) || String(r.mmsi).includes(q));
  const shown = rows.slice(0, LIST_CAP);
  const key = listMode + "|" + q + "|" + shown.map((r) => `${r.mmsi}:${r.detail}:${r.score ?? ""}`).join(",");
  if (key === listKey) return;
  listKey = key;
  $("vessel-list").innerHTML = shown.map((r) => `
    <li tabindex="0" data-mmsi="${r.mmsi}">
      <span><span class="vname">${esc(r.name)}</span><br><span class="muted">${esc(r.detail)}</span></span>
      ${r.score != null ? `<span class="num state-${esc(r.state)}">${r.score}</span>` : ""}
    </li>`).join("");
  const note = rows.length === 0 ? (q ? "No vessel matches that search." : "None right now.")
    : rows.length > LIST_CAP ? `Showing ${LIST_CAP} of ${rows.length.toLocaleString()}. Search to narrow it down.` : "";
  $("vessel-list-note").textContent = note;
  $("vessel-list-note").hidden = !note;
}

function setListMode(mode) {
  listMode = mode === listMode ? null : mode;
  listKey = "";
  listSearch = "";
  $("list-search").value = "";
  for (const b of document.querySelectorAll(".stat")) b.setAttribute("aria-pressed", String(b.dataset.mode === listMode));
  $("log-title").textContent = listMode ? LIST_TITLES[listMode] : "Incident log";
  $("list-back").hidden = !listMode;
  $("list-search").hidden = listMode !== "tracked";
  $("vessel-list").hidden = !listMode;
  $("vessel-list-note").hidden = true;
  $("incidents").hidden = !!listMode;
  if (listMode) $("incidents-empty").hidden = true; else updateEmpty();
  redraw();
}
for (const b of document.querySelectorAll(".stat")) b.addEventListener("click", () => setListMode(b.dataset.mode));
$("list-back").addEventListener("click", () => setListMode(null));
$("list-search").addEventListener("input", (e) => { listSearch = e.target.value; listKey = ""; redraw(); });
$("vessel-list").addEventListener("click", (e) => {
  const li = e.target.closest("li[data-mmsi]");
  if (li) openVessel(Number(li.dataset.mmsi), true);
});
$("vessel-list").addEventListener("keydown", (e) => {
  const li = e.target.closest("li[data-mmsi]");
  if (li && e.key === "Enter") openVessel(Number(li.dataset.mmsi), true);
});

// ---------------------------------------------------------------------------
// Vessel panel
// ---------------------------------------------------------------------------
async function openVessel(mmsi, fly = false, at = null) {
  selected = mmsi;
  const res = await fetch(`/api/vessel/${mmsi}`);
  if (!res.ok) {
    if (fly && at) map.flyTo({ center: at, zoom: 9 });
    return;
  }
  const d = await res.json();
  if (selected !== mmsi) return;
  const panel = $("vessel");
  panel.hidden = false;
  $("vessel-name").textContent = d.name || `MMSI ${mmsi}`;
  const last = d.track[d.track.length - 1];
  selectedPos = d.dark ? [d.dark.lon, d.dark.lat] : last ? [last[0], last[1]] : null;
  $("vessel-locate").textContent = d.dark ? "Show last known position" : "Show current position";
  $("vessel-locate").hidden = !selectedPos;
  map.getSource("selected")?.setData(selectedPos
    ? { type: "Feature", geometry: { type: "Point", coordinates: selectedPos },
        properties: { name: d.name || `MMSI ${mmsi}` } } : empty());
  const status = d.dark ? `not heard for ${ago(d.dark.last_seen)}, last seen ${d.dark.distance_km} km from ${d.dark.cable_name}. `
      + "Our receivers (aisstream.io, volunteer stations) have not picked up its AIS since; it may also be out of their range."
    : last ? `${last[2]} kn, ${NAV_STATUS[d.nav] || "status unknown"}, fix at ${hhmm(last[3])} UTC` : "";
  $("vessel-meta").textContent = `MMSI ${mmsi}. ${status}`;
  $("vessel-about").textContent = d.about;
  const s = d.static || {};
  const watch = d.risk.map((r) => r.watch).find(Boolean);
  const facts = [
    ["Watch list", watch],
    ["Flag", d.flag],
    ["Type", d.type],
    ["Call sign", s.call_sign],
    ["Destination", s.destination],
    ["Draught", s.draught_m ? `${s.draught_m} m` : null],
  ].filter(([, v]) => v);
  $("vessel-facts").innerHTML = facts.map(([k, v]) => `<li><span class="muted">${k}</span><span>${esc(v)}</span></li>`).join("");

  const sogs = d.track.map((p) => p[2]);
  const max = Math.max(2, ...sogs);
  const pts = sogs.map((s, i) => `${(i / Math.max(1, sogs.length - 1)) * 300},${62 - (s / max) * 58}`).join(" ");
  const band = (lo, hi) => `<rect x="0" width="300" y="${62 - (hi / max) * 58}" height="${((hi - lo) / max) * 58}" fill="${COLORS.amber}" opacity="0.12"/>`;
  $("spark").innerHTML = `${band(1.5, Math.min(7, max))}<polyline points="${pts}" fill="none" stroke="${COLORS.corridor}" stroke-width="1.5" vector-effect="non-scaling-stroke"/>`;
  $("spark-caption").textContent = sogs.length
    ? `Speed over the last ${sogs.length} fixes, 0 to ${max.toFixed(1)} kn. Shaded: the 1.5 to 7 kn band where a dragged anchor shows up.`
    : "No recent fixes.";

  $("vessel-risk").innerHTML = d.risk.length ? d.risk.map((r) => (active(r) ? r : { ...r, score: 0, state: "NORMAL" })).map((r) =>
    `<li><span>${esc(r.cable_name)}<br><span class="muted">${r.loiter} loiter, ${r.drag} drag, ${r.dark} dark${
       r.watch ? `, +${r.watch_points} watch list (${esc(r.watch)})` : ""}</span></span>
     <span class="num state-${esc(r.state)}">${r.score}</span></li>`).join("")
    : `<li class="muted">Not scored against any cable.</li>`;
  $("vessel-signals").innerHTML = d.signals.length ? d.signals.slice().reverse().map((s) =>
    `<li><span>${hhmm(s.t)} ${esc(s.signal.toLowerCase())} over ${esc(s.cable_name)}</span><span class="num">+${s.points}</span></li>`).join("")
    : `<li class="muted">None yet.</li>`;
  $("vessel-nav").innerHTML = d.nav_changes.length ? d.nav_changes.slice().reverse().map((n) =>
    `<li><span>${hhmm(n.t)}</span><span class="muted">${esc(NAV_STATUS[n.from] || n.from)} to ${esc(NAV_STATUS[n.to] || n.to)}</span></li>`).join("")
    : `<li class="muted">No changes in the recent track.</li>`;

  map.getSource("track")?.setData({
    type: "FeatureCollection",
    features: d.track.length > 1 ? [{ type: "Feature", geometry: { type: "LineString", coordinates: d.track.map((p) => [p[0], p[1]]) } }] : [],
  });
  if (fly) flyToVessel(at);
}

function flyToVessel(fallback = null) {
  const center = selectedPos || fallback;
  if (center) map.flyTo({ center, zoom: Math.max(map.getZoom(), 9.5), speed: 1.6 });
}
$("vessel-locate").addEventListener("click", () => flyToVessel());
$("vessel-name").addEventListener("click", () => flyToVessel());

function closeVessel() {
  selected = null;
  selectedPos = null;
  map.getSource("selected")?.setData(empty());
  $("vessel").hidden = true;
  map.getSource("track")?.setData(empty());
}
$("vessel-close").addEventListener("click", closeVessel);
document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeVessel(); });
setInterval(() => { if (selected) openVessel(selected); }, 5000);

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------
async function init() {
  const cfg = await (await fetch("/api/config")).json();
  if (cfg.ntfy_url) {
    $("subscribe").hidden = false;
    $("qr").src = "/api/qr.svg";
    $("ntfy-link").href = cfg.ntfy_url;
    $("ntfy-link").textContent = cfg.ntfy_url.replace(/^https?:\/\//, "");
  }
  if (cfg.repo_url) {
    $("pipeline-link").href = cfg.repo_url;
    $("repo").href = cfg.repo_url;
    $("repo-credit").hidden = false;
  }

  const es = new EventSource("/api/stream");
  es.onmessage = (e) => {
    const d = JSON.parse(e.data);
    now = d.now;
    for (const [mmsi, lat, lon, sog, hdg, nav, name, t, score, corridor, service] of d.vessels) {
      vessels.set(mmsi, { mmsi, lat, lon, sog, hdg, nav, name, t, score, corridor, service: !!service });
    }
    for (const r of d.risk) {
      risk.set(`${r.mmsi}|${r.cable_id}`, r);
      const v = vessels.get(r.mmsi);
      if (v) v.score = Math.max(0, ...[...risk.values()].filter((x) => x.mmsi === r.mmsi && active(x)).map((x) => x.score || 0));
    }
    for (const g of d.dark) dark.set(g.mmsi, g);
    for (const id of [...dark.keys()]) if (!d.dark_ids.includes(id)) dark.delete(id);
    for (const i of d.incidents) addIncident(i);
    if (d.rates.topics.length) renderPipeline(d.rates);
    redraw();
  };
}
applyServiceToggle();
init();
