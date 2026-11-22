export const DAY = 86_400_000;
const PAGE_SIZE = 1000;
export const number = new Intl.NumberFormat('en-US');
export const dayLabel = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' });
const dateTime = new Intl.DateTimeFormat('en-GB', {
  day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', hourCycle: 'h23', timeZone: 'UTC',
});

export const SOURCES = {
  usgs: 'USGS', gdacs: 'GDACS', firms: 'NASA FIRMS', eonet: 'NASA EONET', openmeteo: 'Open-Meteo',
};

export const HAZARDS = {
  earthquake: { label: 'Earthquake', color: '#ce4f59', icon: 'activity' },
  flood: { label: 'Flood', color: '#2585cd', icon: 'drop' },
  storm: { label: 'Storm / cyclone', color: '#7957c9', icon: 'storm' },
  wildfire: { label: 'Wildfire', color: '#d27125', icon: 'flame' },
  volcano: { label: 'Volcano', color: '#ab770f', icon: 'volcano' },
  other: { label: 'Other', color: '#697e96', icon: 'dots' },
};

export const REGIONS = {
  world: { label: 'All regions', boxes: [null] },
  africa: { label: 'Africa', boxes: [[-26, -36, 57, 38]] },
  asia: { label: 'Asia', boxes: [[34, 0, 180, 80], [-180, 50, -168, 72]] },
  europe: { label: 'Europe', boxes: [[-32, 35, 47, 72]] },
  'north-america': { label: 'North America', boxes: [[-180, 7, -49, 84]] },
  'south-america': { label: 'South America', boxes: [[-93, -59, -32, 14]] },
  oceania: { label: 'Oceania', boxes: [[110, -53, 180, 0], [-180, -30, -125, 22]] },
};

export function utcDay(date) {
  return new Date(date).toISOString().slice(0, 10);
}

function eventDay(value) {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? utcDay(timestamp) : null;
}

export function formatTime(value) {
  return value && Number.isFinite(Date.parse(value)) ? dateTime.format(new Date(value)) + ' UTC' : '—';
}

export function dateRange(selection, now = Date.now()) {
  const today = Date.parse(utcDay(now));
  const start = selection.custom ? Date.parse(selection.custom.from) : today - (selection.days - 1) * DAY;
  const end = selection.custom ? Date.parse(selection.custom.to) : today;
  const days = Math.round((end - start) / DAY) + 1;
  return { from: utcDay(start), to: utcDay(end), previous: utcDay(start - days * DAY), days };
}

export function hazardKey(type) {
  if (type === 'tropical_cyclone' || type === 'severe_storm') return 'storm';
  return Object.hasOwn(HAZARDS, type) ? type : 'other';
}

export function typeLabel(type) {
  return type ? type.replaceAll('_', ' ').replace(/^./, letter => letter.toUpperCase()) : 'Other hazard';
}

export async function get(path, params, signal) {
  const url = new URL('/api/v1' + path, window.location.origin);
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined) url.searchParams.set(key, value);
  }
  const response = await fetch(url, { signal, cache: 'no-store' });
  if (!response.ok) throw new Error(`${path} · HTTP ${response.status}`);
  return response.json();
}

async function pages(path, params, signal, limit) {
  const rows = [];
  for (let offset = 0; ; ) {
    const page = await get(path, { ...params, limit, offset }, signal);
    if (!Array.isArray(page.data)) throw new Error('Invalid API response for ' + path);
    rows.push(...page.data);
    if (page.data.length < limit) return rows;
    offset += page.data.length;
  }
}

const filtered = (rows, hazard) => rows.filter(row => hazard === 'all' || hazardKey(row.disaster_type) === hazard);
const uniqueEvents = rows => [...new Map(rows.map(row => [row.event_id, row])).values()];

function disasterType(hazard) {
  return ['earthquake', 'flood', 'wildfire', 'volcano'].includes(hazard) ? hazard : null;
}

async function regionRows(path, params, selection, signal, limit = PAGE_SIZE) {
  const lists = await Promise.all(REGIONS[selection.region].boxes.map(box =>
    pages(path, { ...params, bbox: box?.join(',') }, signal, limit)
  ));
  return lists.flat();
}

export async function loadEvents(selection, range, signal) {
  const rows = await regionRows('/events', {
    from: range.previous + 'T00:00:00Z',
    to: range.to + 'T23:59:59.999999Z',
    disaster_type: disasterType(selection.hazard),
    alert_level: selection.alertLevel,
  }, selection, signal);
  return filtered(uniqueEvents(rows), selection.hazard);
}

export async function loadDaily(selection, range, signal) {
  const rows = await pages('/analytics/daily', {
    from: range.previous, to: range.to, disaster_type: disasterType(selection.hazard),
  }, signal, PAGE_SIZE);
  return filtered(rows, selection.hazard);
}

export async function loadAlerts(selection, signal) {
  const rows = uniqueEvents(await regionRows('/alerts/active', {
    disaster_type: disasterType(selection.hazard),
    alert_level: selection.alertLevel,
  }, selection, signal));
  const importance = { red: 3, orange: 2, green: 1 };
  return filtered(rows, selection.hazard).sort((a, b) =>
    (importance[b.alert_level] || 0) - (importance[a.alert_level] || 0) ||
    (Date.parse(b.updated_at) || 0) - (Date.parse(a.updated_at) || 0)
  );
}

export async function loadClosedAlerts(selection, range, signal) {
  const rows = await regionRows('/events', {
    source: 'gdacs', status: 'closed',
    to: range.to + 'T23:59:59.999999Z',
    disaster_type: disasterType(selection.hazard),
    alert_level: selection.alertLevel,
  }, selection, signal);
  return filtered(uniqueEvents(rows), selection.hazard);
}

export async function loadHotspots(selection, range, signal) {
  const rows = await regionRows('/fires/heatmap', { from: range.from, to: range.to }, selection, signal, 10_000);
  const cells = new Map();
  for (const row of rows) {
    const key = row.grid_lat + ',' + row.grid_lon;
    const cell = cells.get(key) || {
      lat: row.grid_lat, lon: row.grid_lon, count: 0, highConfidence: 0, peak: null, days: new Set(),
    };
    if (cell.days.has(row.day)) continue;
    cell.days.add(row.day);
    cell.count += row.detection_count;
    cell.highConfidence += row.high_confidence_count;
    if (row.max_frp != null) cell.peak = Math.max(cell.peak ?? -Infinity, row.max_frp);
    cells.set(key, cell);
  }
  return [...cells.values()].sort((a, b) => b.count - a.count);
}

function daysInRange(range) {
  const start = Date.parse(range.from);
  return Array.from({ length: range.days }, (_, index) => utcDay(start + index * DAY));
}

export function summarize(rows, range) {
  const counts = Object.fromEntries(Object.keys(HAZARDS).map(key => [key, 0]));
  const types = new Set();
  const previousTypes = new Set();
  const days = daysInRange(range).map(day => ({ day, count: 0, types: new Set() }));
  const byDay = new Map(days.map(day => [day.day, day]));
  let previous = 0;
  let total = 0;
  for (const row of rows) {
    const day = row.day ?? eventDay(row.started_at);
    if (!day || day < range.previous) continue;
    const count = row.event_count ?? 1;
    if (!count) continue;
    if (day < range.from) {
      previous += count;
      previousTypes.add(row.disaster_type);
      continue;
    }
    const entry = byDay.get(day);
    if (!entry) continue;
    entry.count += count;
    entry.types.add(row.disaster_type);
    total += count;
    counts[hazardKey(row.disaster_type)] += count;
    types.add(row.disaster_type);
  }
  return {
    days: days.map(({ day, count }) => ({ day, count })),
    counts, total, previous, types: types.size, previousTypes: previousTypes.size,
    typeSeries: days.map(day => day.types.size),
  };
}

export function currentEvents(rows, range) {
  return rows.filter(row => eventDay(row.started_at) >= range.from);
}

export function summarizeRegions(rows, range) {
  const current = new Set();
  const previous = new Set();
  const daily = new Map(daysInRange(range).map(day => [day, new Set()]));
  for (const row of rows) {
    const day = eventDay(row.started_at);
    if (!day || day < range.previous || day > range.to) continue;
    const period = day < range.from ? previous : current;
    for (const name of row.country?.split(/[,;]+/) || []) {
      const label = name.trim().replace(/\s+/g, ' ').toLowerCase();
      if (!label) continue;
      period.add(label);
      daily.get(day)?.add(label);
    }
  }
  return { total: current.size, previous: previous.size, series: [...daily.values()].map(day => day.size) };
}

export function summarizeAlertTrend(active, closed, range) {
  const activeIds = new Set(active.map(row => row.event_id));
  const intervals = active.map(row => [Date.parse(row.started_at), Infinity]);
  for (const row of closed) {
    const end = Date.parse(row.ended_at);
    if (!activeIds.has(row.event_id) && Number.isFinite(end)) {
      intervals.push([Date.parse(row.started_at), end]);
    }
  }
  const countAt = cutoff => intervals.reduce((count, [start, end]) =>
    count + Number(start <= cutoff && cutoff < end), 0);
  return {
    previous: countAt(Date.parse(range.from) - 1),
    series: daysInRange(range).map(day =>
      day === range.to ? active.length : countAt(Date.parse(day) + DAY - 1)),
  };
}

export function ageLabel(minutes) {
  if (minutes == null || !Number.isFinite(Number(minutes))) return 'Awaiting data';
  const value = Math.max(0, Number(minutes));
  if (value < 1) return 'Just now';
  if (value < 60) return `${value} min ago`;
  if (value < 1440) return `${Math.floor(value / 60)} h ${value % 60} min ago`;
  return `${Math.floor(value / 1440)} d ago`;
}
