import { HAZARDS, REGIONS, SOURCES, formatTime, hazardKey, number, typeLabel } from './data.js';
import { icon } from './icons.js';

const L = window.L;
const worldBounds = [[-90, -180], [90, 180]];
const hotspotGradient = { 0.18: '#306de0', 0.42: '#2bd0d2', 0.63: '#ffe163', 0.82: '#fb7a32', 1: '#de2c49' };
const MAX_CELLS = 8000;

function text(tag, value, className) {
  const node = document.createElement(tag);
  node.textContent = value;
  if (className) node.className = className;
  return node;
}

export function createActivityMap(onEvent) {
  const map = L.map('map', {
    crs: L.CRS.EPSG4326, attributionControl: false, zoomControl: false,
    preferCanvas: true, worldCopyJump: false, maxBounds: worldBounds,
    maxBoundsViscosity: 0.85, minZoom: -2, maxZoom: 6,
    zoomSnap: 0, zoomDelta: .75, scrollWheelZoom: false,
  });
  L.imageOverlay('/assets/world.jpg', worldBounds, { alt: 'Natural Earth shaded relief world basemap' }).addTo(map);

  const eventLayer = L.layerGroup().addTo(map);
  const fireLayer = L.heatLayer([], {
    minOpacity: 0.10, maxZoom: 2, gradient: hotspotGradient,
  });
  const labels = L.layerGroup().addTo(map);
  const oceans = [
    [0, -142, 'Pacific<br>Ocean'], [9, -37, 'Atlantic<br>Ocean'],
    [-23, 77, 'Indian<br>Ocean'], [5, 158, 'Pacific<br>Ocean'],
  ];
  for (const [lat, lon, label] of oceans) {
    L.marker([lat, lon], {
      interactive: false, keyboard: false,
      icon: L.divIcon({ className: 'ocean-label', html: label, iconSize: [76, 27], iconAnchor: [38, 13] }),
    }).addTo(labels);
  }

  const mapNode = document.querySelector('#map');
  let region = 'world';
  let movedByUser = false;
  let activeMode = 'events';
  let visibleCells = [];
  const legend = document.querySelector('#map-legend');
  const mapMessage = document.querySelector('#map-message');
  const summary = document.querySelector('#map-summary');

  function sizeHeatLayer() {
    if (!mapNode.clientWidth) return;
    const radius = Math.max(7, Math.min(15, Math.round(mapNode.clientWidth / 70)));
    const blur = Math.round(radius * 1.15);
    const max = Math.max(19, 56 / 2 ** Math.max(0, map.getZoom() - .75));
    fireLayer.setOptions({ radius, blur, max });
  }

  function fitRegion() {
    movedByUser = false;
    if (!mapNode.clientWidth || !mapNode.clientHeight) return;
    map.invalidateSize({ pan: false });
    const [box] = REGIONS[region].boxes;
    const worldZoom = Math.log2(map.getSize().x / 512);
    // Raising Leaflet's minimum can trigger an animated zoom during a resize.
    if (map.getZoom() < worldZoom) map.setZoom(worldZoom, { animate: false });
    map.setMinZoom(worldZoom);
    if (!box) {
      map.setView([11, 0], worldZoom, { animate: false });
    } else {
      map.fitBounds([[box[1], box[0]], [box[3], box[2]]], { padding: [16, 18], animate: false });
    }
  }

  map.on('dragstart', () => { movedByUser = true; });
  map.on('zoomend', () => {
    if (map.getZoom() > 1.65) labels.remove();
    else labels.addTo(map);
    if (activeMode === 'hotspots') sizeHeatLayer();
  });
  map.on('resize', () => {
    if (activeMode === 'hotspots') sizeHeatLayer();
  });
  document.querySelector('#zoom-in').addEventListener('click', () => {
    movedByUser = true;
    map.zoomIn();
  });
  document.querySelector('#zoom-out').addEventListener('click', () => {
    movedByUser = true;
    map.zoomOut();
  });
  document.querySelector('#reset-map').addEventListener('click', fitRegion);
  const resize = new ResizeObserver(() => {
    if (!mapNode.clientWidth || !mapNode.clientHeight) return;
    if (movedByUser) map.invalidateSize({ pan: false });
    else fitRegion();
  });
  resize.observe(mapNode);
  fitRegion();

  function setLegend() {
    if (activeMode === 'hotspots') {
      const scale = text('i', '', 'density-scale');
      const stops = Object.entries(hotspotGradient)
        .sort(([a], [b]) => Number(a) - Number(b))
        .map(([stop, color]) => `${color} ${Number(stop) * 100}%`);
      scale.style.background = `linear-gradient(90deg, ${stops.join(', ')})`;
      legend.setAttribute('aria-label', 'Relative hotspot density');
      legend.replaceChildren(text('span', 'Low density'), scale, text('span', 'High'));
      return;
    }
    legend.setAttribute('aria-label', 'Event hazard types');
    legend.replaceChildren(...Object.values(HAZARDS).map(({ label, color }) => {
      const item = text('span', label);
      const dot = text('i', '', 'legend-dot');
      dot.style.setProperty('--hazard', color);
      item.prepend(dot);
      return item;
    }));
  }

  function setMessage(message, subtitle) {
    mapMessage.textContent = message || '';
    mapMessage.hidden = !message;
    summary.textContent = subtitle;
  }

  function showEvents(events, message) {
    eventLayer.clearLayers();
    if (!events) {
      setMessage(message, 'Event locations');
      return;
    }
    const points = events.filter(row => Number.isFinite(row.latitude) && Number.isFinite(row.longitude));
    const compact = points.length > 550;
    for (const row of points) {
      const { color, icon: glyph } = HAZARDS[hazardKey(row.disaster_type)];
      const point = [row.latitude, row.longitude];
      const marker = compact ?
        L.circleMarker(point, { radius: 4, color: '#fff', weight: 1, fillColor: color, fillOpacity: .88 }) :
        L.marker(point, {
          title: row.title || typeLabel(row.disaster_type),
          icon: L.divIcon({
            className: 'event-pin',
            html: `<span class="hazard-marker" style="--hazard:${color}">${icon(glyph)}</span>`,
            iconSize: [23, 23], iconAnchor: [11, 11], popupAnchor: [0, -11],
          }),
        });
      marker.bindPopup(() => {
        const content = document.createElement('div');
        content.append(
          text('strong', row.title || typeLabel(row.disaster_type), 'popup-title'),
          text('span', [SOURCES[row.source], row.country].filter(Boolean).join(' · '), 'popup-meta'),
          text('span', formatTime(row.started_at), 'popup-meta'),
        );
        const button = text('button', 'Event details', 'text-button');
        button.type = 'button';
        button.addEventListener('click', () => onEvent(row));
        content.append(button);
        return content;
      }).addTo(eventLayer);
    }
    setMessage(points.length ? null : 'No mapped events in this selection',
      `${number.format(points.length)} mapped event records`);
  }

  function showHotspots(cells, message) {
    map.closePopup();
    visibleCells = (cells || []).slice(0, MAX_CELLS)
      .map(cell => ({ cell, center: [cell.lat + .25, cell.lon + .25] }));
    fireLayer.setLatLngs(visibleCells.map(({ cell, center }) => [...center, Math.sqrt(cell.count)]));
    if (!cells) {
      setMessage(message, 'NASA FIRMS satellite detections');
      return;
    }
    const detail = cells.length > MAX_CELLS ? `Top ${number.format(MAX_CELLS)} of ${number.format(cells.length)} cells` :
      `${number.format(cells.length)} hotspot cells · NASA FIRMS`;
    setMessage(cells.length ? null : 'No hotspots in this selection', detail);
  }

  map.on('click', event => {
    if (activeMode !== 'hotspots') return;
    let closest;
    let distance = fireLayer.options.radius + fireLayer.options.blur;
    for (const point of visibleCells) {
      const proximity = event.containerPoint.distanceTo(map.latLngToContainerPoint(point.center));
      if (proximity < distance) {
        closest = point;
        distance = proximity;
      }
    }
    if (!closest) return;
    const { cell, center } = closest;
    const content = document.createElement('div');
    content.append(
      text('strong', `${number.format(cell.count)} satellite detections`, 'popup-title'),
      text('span', `${number.format(cell.highConfidence)} high-confidence detections`, 'popup-meta'),
      text('span', cell.peak === null ? 'Peak power unavailable' : `Peak FRP ${number.format(cell.peak)} MW`, 'popup-meta'),
      text('span', `${cell.days.size} UTC day(s) · 0.5° grid`, 'popup-meta'),
    );
    L.popup().setLatLng(center).setContent(content).openOn(map);
  });

  let lastContent;
  let lastMessage;
  setLegend();

  return {
    update(mode, selectedRegion, content, message) {
      if (selectedRegion !== region) {
        region = selectedRegion;
        fitRegion();
      }
      const modeChanged = activeMode !== mode;
      if (modeChanged) {
        activeMode = mode;
        map.closePopup();
        if (mode === 'events') {
          fireLayer.remove();
          eventLayer.addTo(map);
        } else {
          eventLayer.remove();
          sizeHeatLayer();
          fireLayer.addTo(map);
        }
        setLegend();
      }
      if (!modeChanged && lastContent === content && lastMessage === message) return;
      lastContent = content;
      lastMessage = message;
      if (mode === 'events') showEvents(content, message);
      else showHotspots(content, message);
    },
  };
}
