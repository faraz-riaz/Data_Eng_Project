import {
  DAY, HAZARDS, REGIONS, SOURCES, ageLabel, currentEvents, dateRange, formatTime,
  get, hazardKey, loadAlerts, loadClosedAlerts, loadDaily, loadEvents, loadHotspots, number,
  summarize, summarizeAlertTrend, summarizeRegions, typeLabel, utcDay,
} from './data.js';
import { createActivityMap } from './map.js';
import { createCharts, createSparklines } from './charts.js';
import { icon, installIcons } from './icons.js';

/**
 * Find the first dashboard element that matches a CSS selector.
 * @param {string} selector - CSS selector to query.
 * @returns {Element|null} Matching element, if present.
 */
const $ = selector => document.querySelector(selector);
const defaultSelection = { days: 7, hazard: 'all', region: 'world', alertLevel: null, custom: null };
const state = {
  selection: { ...defaultSelection },
  mode: 'events',
  data: {},
  keys: {},
  connection: 'loading',
  lastRefresh: null,
  busy: false,
  errors: new Set(),
};
const REFRESH_INTERVAL = 60_000;
const MAX_RANGE = 90;
const ALERT_PREVIEW = 7;

/**
 * Create an element with optional plain text and a CSS class.
 * @param {string} tag - HTML tag name.
 * @param {string} [text] - Text to display.
 * @param {string} [className] - CSS class to apply.
 * @returns {HTMLElement} The new element.
 */
function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text != null) element.textContent = text;
  if (className) element.className = className;
  return element;
}

/**
 * Format a dashboard count or show a placeholder for missing data.
 * @param {number|null|undefined} value - Count to display.
 * @returns {string} Formatted count or an em dash.
 */
function displayNumber(value) {
  return value == null ? '—' : number.format(value);
}

/**
 * Create a message for an empty, loading, or unavailable panel.
 * @param {string} title - Short status heading.
 * @param {string} description - Detail to display below the heading.
 * @returns {HTMLElement} Status message element.
 */
function emptyBlock(title, description) {
  const block = node('div', null, 'empty-block');
  block.append(node('strong', title), node('p', description));
  return block;
}

const allAlerts = $('#all-alerts');
const eventDetail = $('#event-detail');
const dateRangeDialog = $('#date-range-dialog');
let detailController;
let requestController;

installIcons();
const map = createActivityMap(openEvent);
const updateCharts = createCharts();
const updateSparklines = createSparklines();

/**
 * Build a clickable GDACS alert row.
 * @param {Object} alert - Active alert from the pipeline API.
 * @returns {HTMLButtonElement} Row that opens the event details.
 */
function alertRow(alert) {
  const { color, icon: glyph } = HAZARDS[hazardKey(alert.disaster_type)];
  const row = node('button', null, 'alert-row');
  row.type = 'button';
  const thumbnail = node('span', null, 'alert-thumb');
  thumbnail.style.setProperty('--hazard', color);
  thumbnail.innerHTML = icon(glyph);
  const main = node('span', null, 'alert-main');
  const name = node('span', alert.title || typeLabel(alert.disaster_type), 'alert-name');
  name.title = name.textContent;
  main.append(name, node('span', alert.country || typeLabel(alert.disaster_type), 'alert-place'));
  const level = ['red', 'orange', 'green'].includes(alert.alert_level) ? alert.alert_level : null;
  const extra = node('span', null, 'alert-extra');
  const updated = node('time', formatTime(alert.updated_at), 'alert-time');
  if (alert.updated_at) updated.dateTime = alert.updated_at;
  extra.append(node('span', level || 'Unknown', 'badge' + (level ? ' ' + level : '')), updated);
  row.append(thumbnail, main, extra);
  row.addEventListener('click', () => openEvent(alert));
  return row;
}

/**
 * Create a definition list for event details.
 * @param {Array} entries - Pairs of field labels and display values.
 * @returns {HTMLDListElement} List of event details.
 */
function detailsGrid(entries) {
  const grid = node('dl', null, 'detail-grid');
  for (const [label, value] of entries) {
    const cell = node('div');
    cell.append(node('dt', label), node('dd', value ?? '—'));
    grid.append(cell);
  }
  return grid;
}

/**
 * Create a table for alert history, geometry observations, or forecasts.
 * @param {string[]} headers - Column labels.
 * @param {Array[]} rows - Display values for each row.
 * @returns {HTMLTableElement} Table with a header and body.
 */
function detailTable(headers, rows) {
  const table = node('table', null, 'detail-table');
  const head = table.createTHead().insertRow();
  for (const label of headers) {
    const column = node('th', label);
    column.scope = 'col';
    head.append(column);
  }
  const body = table.createTBody();
  for (const values of rows) {
    const row = body.insertRow();
    for (const value of values) row.append(node('td', value ?? '—'));
  }
  return table;
}

/**
 * Format a measurement with its unit.
 * @param {number|null|undefined} value - Measurement to display.
 * @param {string} unit - Display unit.
 * @returns {string} Formatted measurement or an em dash.
 */
function measurement(value, unit) {
  return value == null ? '—' : `${number.format(value)} ${unit}`;
}

/**
 * Open the event dialog and load details and upcoming weather.
 * @param {Object} record - Event or alert with an event ID.
 * @returns {Promise<void>} Resolves when the dialog has been updated.
 */
async function openEvent(record) {
  detailController?.abort();
  detailController = new AbortController();
  const signal = detailController.signal;
  const { event_id: eventId } = record;
  const header = $('#event-title');
  const subtitle = $('#event-subtitle');
  const content = $('#event-content');
  header.textContent = record.title || typeLabel(record.disaster_type);
  subtitle.textContent = [SOURCES[record.source || eventId.split(':')[0]], typeLabel(record.disaster_type)].join(' · ');
  content.replaceChildren(node('p', 'Loading event details…'));
  if (!eventDetail.open) eventDetail.showModal();
  // A failed forecast request still lets the dialog show event details.
  const [eventResult, weatherResult] = await Promise.allSettled([
    get('/events/' + encodeURIComponent(eventId), {}, signal),
    get('/weather', { event_id: eventId, from: new Date().toISOString(), limit: 6 }, signal),
  ]);
  if (signal.aborted || !eventDetail.open) return;
  const event = eventResult.value;
  const weather = weatherResult.value?.data;
  content.replaceChildren();
  if (event) {
    header.textContent = event.title || header.textContent;
    const fields = [
      ['Status', event.status ? typeLabel(event.status) : null],
      ['Reported location', event.country], ['Started', formatTime(event.started_at)],
      ['Source update', formatTime(event.updated_at)],
    ];
    if (event.ended_at) fields.push(['Ended', formatTime(event.ended_at)]);
    if (Number.isFinite(event.latitude) && Number.isFinite(event.longitude)) {
      fields.push(['Coordinates', `${event.latitude.toFixed(2)}°, ${event.longitude.toFixed(2)}°`]);
    }
    content.append(detailsGrid(fields));
    if (event.earthquake) {
      const quake = event.earthquake;
      content.append(node('h3', 'Seismic details'), detailsGrid([
        ['Magnitude', quake.magnitude == null ? null : [quake.magnitude, quake.magnitude_type].filter(Boolean).join(' ')],
        ['Depth', measurement(quake.depth_km, 'km')],
        ['Magnitude band', quake.severity ? typeLabel(quake.severity) : null],
        ['USGS tsunami flag', quake.tsunami_flag == null ? null : quake.tsunami_flag ? 'Yes' : 'No'],
      ]));
    }
    if (event.alerts?.length) {
      content.append(node('h3', 'GDACS alert history'), detailTable(
        ['Updated', 'Episode', 'Level', 'Score'],
        event.alerts.slice(-5).reverse().map(alert => [
          formatTime(alert.observed_at), alert.episode_id, typeLabel(alert.alert_level), alert.alert_score,
        ]),
      ));
    }
    if (event.geometries?.length) {
      content.append(node('h3', 'Recent EONET observations'), detailTable(
        ['Observed', 'Geometry', 'Reported magnitude'],
        event.geometries.slice(-4).reverse().map(geometry => [
          formatTime(geometry.observed_at), geometry.geometry_type,
          geometry.magnitude_value == null ? '—' : measurement(geometry.magnitude_value, geometry.magnitude_unit || ''),
        ]),
      ));
    }
  } else {
    content.append(node('p', 'Event details are temporarily unavailable.'));
  }
  content.append(node('h3', 'Open-Meteo forecast'));
  if (weather?.length) {
    content.append(node('p', 'Retrieved ' + formatTime(weather[0].retrieved_at)), detailTable(
      ['Valid hour', 'Temp.', 'Precip.', 'Wind', 'Humidity'],
      weather.map(hour => [
        formatTime(hour.forecast_time), measurement(hour.temperature, '°C'), measurement(hour.precipitation, 'mm'),
        measurement(hour.wind_speed, 'm/s'), measurement(hour.humidity, '%'),
      ]),
    ));
  } else {
    content.append(node('p', weatherResult.status === 'fulfilled'
      ? 'No upcoming forecast is available for this event.'
      : 'The weather forecast is temporarily unavailable.'));
  }
}

eventDetail.addEventListener('close', () => detailController?.abort());
for (const dialog of [eventDetail, allAlerts, dateRangeDialog]) {
  dialog.addEventListener('click', event => {
    if (event.target === dialog) {
      const rect = dialog.getBoundingClientRect();
      if (event.clientX < rect.left || event.clientX > rect.right ||
          event.clientY < rect.top || event.clientY > rect.bottom) dialog.close();
    }
  });
}

let renderedAlerts;
let renderedAlertState;
/**
 * Update the active alert count and preview list.
 * @param {Object[]|undefined} alerts - Matching current alerts, if loaded.
 * @param {string} status - Loading, unavailable, or ready state.
 */
function renderAlerts(alerts, status) {
  $('#alert-count').textContent = displayNumber(alerts?.length);
  const tag = $('#alert-total');
  tag.hidden = !alerts;
  tag.textContent = alerts ? displayNumber(alerts.length) : '';
  $('#view-alerts').disabled = !alerts?.length;
  if (renderedAlerts === alerts && renderedAlertState === status) return;
  renderedAlerts = alerts;
  renderedAlertState = status;
  const list = $('#alert-list');
  if (alerts?.length) list.replaceChildren(...alerts.slice(0, ALERT_PREVIEW).map(alertRow));
  else if (alerts) list.replaceChildren(emptyBlock('No active alerts', 'No current GDACS alerts match these filters.'));
  else if (status === 'loading') list.replaceChildren(emptyBlock('Loading active alerts', 'Checking the GDACS feed.'));
  else list.replaceChildren(emptyBlock('Alerts unavailable', 'Refresh to retry the connection to the pipeline.'));
  if (allAlerts.open) renderAllAlerts();
}

/** Populate the dialog with all active alerts matching the current filters. */
function renderAllAlerts() {
  const alerts = state.data.alerts || [];
  $('#all-alerts-note').textContent = `${displayNumber(alerts.length)} current alerts · ${REGIONS[state.selection.region].label}`;
  $('#all-alerts-list').replaceChildren(...alerts.map(alertRow));
  if (!alerts.length) $('#all-alerts-list').append(emptyBlock('No active alerts', 'No current GDACS alerts match these filters.'));
}

$('#view-alerts').addEventListener('click', () => {
  renderAllAlerts();
  allAlerts.showModal();
});

let renderedSources;
let renderedSourceState;
/**
 * Show each source's last run and processing status.
 * @param {Object[]|undefined} sources - Source freshness records, if loaded.
 * @param {string} status - Loading, unavailable, or ready state.
 */
function renderSources(sources, status) {
  if (renderedSources === sources && renderedSourceState === status) return;
  renderedSources = sources;
  renderedSourceState = status;
  const bySource = new Map((sources || []).map(source => [source.source, source]));
  $('#source-list').replaceChildren(...Object.entries(SOURCES).map(([key, name]) => {
    const source = bySource.get(key);
    const item = node('li');
    const dot = node('i', null, 'source-dot');
    let label = status === 'loading' ? 'Checking…' : 'Unavailable';
    dot.dataset.status = 'unknown';
    if (source) {
      if (source.last_run_status === 'failed' || source.is_stale === true) {
        dot.dataset.status = 'stale';
        label = source.last_run_status === 'failed' ? 'Last run failed' : 'Stale';
      } else if (source.last_run_status === 'skipped') {
        dot.dataset.status = 'skipped';
        label = 'Last run skipped';
      } else {
        dot.dataset.status = source.last_success_at ? 'fresh' : 'unknown';
        label = ageLabel(source.minutes_since_success);
      }
      item.title = [
        `Last successful check ${formatTime(source.last_success_at)}`,
        `Last processed ${formatTime(source.last_processed_at)}`,
        `${displayNumber(source.unprocessed_records)} pending records`,
      ].join(' · ');
    }
    const details = node('span', name, 'source-label');
    details.append(node('small', label));
    item.append(dot, details);
    return item;
  }));
}

/**
 * Resolve the display status for one dashboard section.
 * @param {string} key - Key in the dashboard data state.
 * @returns {string} Loading, unavailable, or ready state.
 */
function sectionStatus(key) {
  if (state.busy && state.data[key] == null) return 'loading';
  return state.errors.has(key) || state.connection === 'offline' ? 'unavailable' : 'ready';
}

/**
 * Show the percentage change from the previous period when a baseline exists.
 * @param {string} id - CSS selector for the comparison caption.
 * @param {number|undefined} current - Selected-period value.
 * @param {number|undefined} previous - Comparison-period value.
 * @param {number} days - Length of each comparison period.
 * @param {string} fallback - Default caption while data is unavailable.
 * @param {boolean} [estimated=false] - Whether to mark the change as an estimate.
 */
function showComparison(id, current, previous, days, fallback, estimated = false) {
  const caption = $(id);
  if (current == null || !previous) {
    caption.textContent = previous === 0 ? 'No prior baseline' : fallback;
    caption.className = '';
    return;
  }
  const change = Math.round((current / previous - 1) * 100) || 0;
  const comparison = `${change > 0 ? '+' : ''}${number.format(change)}%`;
  caption.textContent = `${estimated ? '≈' : ''}${comparison} vs previous ${days} ${days === 1 ? 'day' : 'days'}`;
  caption.className = change < 0 ? 'comparison negative' : change > 0 ? 'comparison' : '';
}

/** Update the status, metrics, alerts, map, and charts from dashboard state. */
function render() {
  const { data, mode, selection, busy, lastRefresh, connection } = state;
  const status = $('#connection');
  status.dataset.state = connection;
  status.replaceChildren(node('i'), document.createTextNode({
    loading: 'Connecting', live: 'API connected', partial: 'Partial data', offline: 'API offline',
  }[connection]));
  const lastTime = $('#last-refresh');
  lastTime.textContent = lastRefresh ? 'Refreshed ' + formatTime(lastRefresh.toISOString()) : 'Waiting for first update';
  if (lastRefresh) lastTime.dateTime = lastRefresh.toISOString();
  $('#refresh').dataset.busy = String(busy);
  $('#refresh').setAttribute('aria-busy', String(busy));

  const warning = $('#status-banner');
  warning.hidden = !['offline', 'partial'].includes(connection);
  if (!warning.hidden) {
    const carryingPriorData = ['events', 'alerts', 'summary'].some(key => data[key] != null && state.errors.has(key));
    $('#status-message').textContent = connection === 'offline'
      ? carryingPriorData ? 'Pipeline API is unreachable. Showing the last successful data.'
        : 'Pipeline API is unreachable. The dashboard will retry automatically.'
      : carryingPriorData ? 'Some feeds could not refresh. Their last successful data is still shown.'
        : 'Some sections could not load from the pipeline API.';
  }

  $('#total-events').textContent = displayNumber(data.summary?.total);
  $('#hazard-count').textContent = displayNumber(data.summary?.types);
  const regions = data.events?.regions;
  $('#region-count').textContent = displayNumber(regions?.total);
  const { days } = dateRange(selection);
  showComparison('#event-comparison', data.summary?.total, data.summary?.previous, days, 'Event records in range');
  showComparison('#alert-comparison', data.alerts?.length, data.alertTrend?.previous, days, 'Current GDACS alerts', true);
  showComparison('#region-comparison', regions?.total, regions?.previous, days, 'Distinct source locations');
  showComparison('#hazard-comparison', data.summary?.types, data.summary?.previousTypes, days, 'Types reported in range');
  updateSparklines({
    events: data.summary?.days.map(day => day.count),
    alerts: data.alertTrend?.series,
    regions: regions?.series,
    hazards: data.summary?.typeSeries,
  });
  renderAlerts(data.alerts, sectionStatus('alerts'));
  renderSources(data.sources, sectionStatus('sources'));
  const dataKey = mode === 'events' ? 'events' : 'hotspots';
  const mapStatus = sectionStatus(dataKey);
  map.update(mode, selection.region, mode === 'events' ? data.events?.rows : data.hotspots, mapStatus === 'loading'
    ? mode === 'events' ? 'Loading event locations…' : 'Loading satellite detections…'
    : mode === 'events' ? 'Event locations unavailable' : 'Satellite detections unavailable');
  updateCharts(data.summary, sectionStatus('summary') === 'loading' ? 'Loading event records…' : 'Event records unavailable');
}

/**
 * Load dashboard data for the current filters and render the results.
 * @param {Object} [options] - Request behavior.
 * @param {boolean} [options.replace=false] - Whether to cancel an in-flight refresh.
 * @returns {Promise<void>} Resolves after successful sections or errors are rendered.
 */
async function refresh({ replace = false } = {}) {
  if (state.busy && !replace) return;
  requestController?.abort();
  requestController = new AbortController();
  const { signal } = requestController;
  const selection = { ...state.selection };
  const range = dateRange(selection);
  // The active-alert trend ends today, even when the event filter selects past dates.
  const alertRange = dateRange({ days: range.days });
  const alertKey = [selection.hazard, selection.region, selection.alertLevel].join('/');
  const eventsKey = [alertKey, range.from, range.to].join('/');
  const newKeys = {
    events: eventsKey, summary: eventsKey, alerts: alertKey,
    alertTrend: [alertKey, alertRange.from, alertRange.to].join('/'),
    hotspots: [selection.region, range.from, range.to].join('/'),
  };
  // Retain the last successful data only when its filters still match.
  for (const [key, value] of Object.entries(newKeys)) {
    if (state.keys[key] !== value) delete state.data[key];
  }
  state.keys = newKeys;
  state.busy = true;
  state.errors.clear();

  const rows = loadEvents(selection, range, signal);
  // Global daily counts lack regional and aid-level detail.
  const summary = selection.region === 'world' && !selection.alertLevel
    ? loadDaily(selection, range, signal).catch(() => rows)
    : rows;
  const alerts = loadAlerts(selection, signal);
  const requests = {
    events: rows.then(events => ({ rows: currentEvents(events, range), regions: summarizeRegions(events, range) })),
    summary: summary.then(items => summarize(items, range)),
    alerts,
    alertTrend: Promise.all([alerts, loadClosedAlerts(selection, alertRange, signal)])
      .then(([active, closed]) => summarizeAlertTrend(active, closed, alertRange)),
    sources: get('/system/freshness', {}, signal).then(result => {
      if (!Array.isArray(result.sources)) throw new Error('Invalid source freshness response');
      return result.sources;
    }),
  };
  if (state.mode === 'hotspots') requests.hotspots = loadHotspots(selection, range, signal);

  render();
  const entries = Object.entries(requests);
  const results = await Promise.allSettled(entries.map(([, promise]) => promise));
  if (signal.aborted) return;
  state.busy = false;
  results.forEach((result, index) => {
    const [key] = entries[index];
    if (result.status === 'fulfilled') state.data[key] = result.value;
    else state.errors.add(key);
  });
  if (state.errors.size < results.length) state.lastRefresh = new Date();
  state.connection = state.errors.size === results.length ? 'offline' : state.errors.size ? 'partial' : 'live';
  render();
}

/** Update filter controls and start a refresh for the new selection. */
function selectionChanged() {
  const { selection } = state;
  $('#edit-dates').hidden = !selection.custom;
  $('#reset').hidden = selection.days === 7 && !selection.custom &&
    selection.hazard === 'all' && selection.region === 'world' && !selection.alertLevel;
  refresh({ replace: true });
}

$('#hazard').addEventListener('change', event => {
  state.selection.hazard = event.target.value;
  selectionChanged();
});
$('#region').addEventListener('change', event => {
  state.selection.region = event.target.value;
  selectionChanged();
});
$('#aid-level').addEventListener('change', event => {
  state.selection.alertLevel = event.target.value || null;
  selectionChanged();
});

/** Open the custom date dialog with the current range filled in. */
function openDateRange() {
  const range = dateRange(state.selection);
  $('#date-error').textContent = '';
  $('#custom-from').value = range.from;
  $('#custom-to').value = range.to;
  dateRangeDialog.showModal();
  $('#custom-from').focus();
}

$('#edit-dates').addEventListener('click', openDateRange);
dateRangeDialog.addEventListener('close', () => {
  $('#period').value = state.selection.custom ? 'custom' : String(state.selection.days);
});
$('#period').addEventListener('change', event => {
  if (event.target.value === 'custom') {
    openDateRange();
    return;
  }
  state.selection.days = Number(event.target.value);
  state.selection.custom = null;
  selectionChanged();
});
$('#custom-range').addEventListener('submit', event => {
  event.preventDefault();
  const from = $('#custom-from').value;
  const to = $('#custom-to').value;
  const span = (Date.parse(to) - Date.parse(from)) / DAY + 1;
  if (span < 1 || span > MAX_RANGE || to > utcDay(Date.now())) {
    $('#date-error').textContent = 'Choose up to 90 days ending today or earlier.';
    return;
  }
  $('#date-error').textContent = '';
  state.selection.custom = { from, to };
  dateRangeDialog.close();
  selectionChanged();
});
$('#reset').addEventListener('click', () => {
  state.selection = { ...defaultSelection };
  $('#period').value = '7';
  $('#hazard').value = 'all';
  $('#region').value = 'world';
  $('#aid-level').value = '';
  selectionChanged();
});

const viewButtons = document.querySelectorAll('[data-view-target]');
for (const button of viewButtons) {
  button.addEventListener('click', () => {
    $('#overview').dataset.view = button.dataset.viewTarget;
    viewButtons.forEach(item => item.setAttribute('aria-pressed', String(item === button)));
  });
}

for (const button of document.querySelectorAll('[data-layer]')) {
  button.addEventListener('click', () => {
    if (button.dataset.layer === state.mode) return;
    state.mode = button.dataset.layer;
    document.querySelectorAll('[data-layer]').forEach(item => {
      item.setAttribute('aria-pressed', String(item === button));
    });
    if (state.mode === 'hotspots' && !state.data.hotspots) refresh({ replace: true });
    else render();
  });
}

$('#refresh').addEventListener('click', () => refresh({ replace: true }));
$('#retry').addEventListener('click', () => refresh({ replace: true }));
window.setInterval(() => {
  if (!document.hidden) refresh();
}, REFRESH_INTERVAL);
document.addEventListener('visibilitychange', () => {
  if (!document.hidden && Date.now() - (state.lastRefresh?.getTime() || 0) > REFRESH_INTERVAL) refresh();
});
refresh();
