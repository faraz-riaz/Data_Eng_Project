import { dayLabel, HAZARDS, number } from './data.js';

const chartColors = {
  earthquake: '#f4515b',
  flood: '#2d9be8',
  storm: '#955eea',
  wildfire: '#f28b17',
  volcano: '#f5c435',
  other: '#99a8ba',
};

/**
 * Fit a canvas to its CSS size and scale it for the display density.
 * @param {HTMLCanvasElement} canvas - Canvas to size.
 * @returns {Object|null} Drawing context and CSS dimensions, or null for a hidden canvas.
 */
function canvasFrame(canvas) {
  const { width, height } = canvas.getBoundingClientRect();
  if (!width || !height) return null;
  const scale = window.devicePixelRatio || 1;
  canvas.width = Math.round(width * scale);
  canvas.height = Math.round(height * scale);
  const ctx = canvas.getContext('2d');
  ctx.scale(scale, scale);
  return { ctx, width, height };
}

/**
 * Choose a positive integer interval for the count axis.
 * @param {number} maximum - Largest value the axis needs to show.
 * @returns {number} Spacing between axis ticks.
 */
function axisStep(maximum) {
  const target = maximum / 4;
  const size = 10 ** Math.floor(Math.log10(target));
  return Math.max(1, [1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 6, 7.5, 8, 10].find(step =>
    Number.isInteger(step * size) && step * size >= target
  ) * size || 1);
}

/**
 * Create a canvas path through a series of points.
 * @param {CanvasRenderingContext2D} ctx - Drawing context.
 * @param {Array} points - Pairs of horizontal and vertical coordinates.
 */
function plot(ctx, points) {
  ctx.beginPath();
  points.forEach(([x, y], index) => {
    if (index) ctx.lineTo(x, y);
    else ctx.moveTo(x, y);
  });
}

/**
 * Initialize the metric sparklines and observe size changes.
 * @returns {function(Object): void} Function that updates all sparkline series.
 */
export function createSparklines() {
  const charts = document.querySelectorAll('.sparkline');
  let series = {};

  /**
   * Draw one metric sparkline from its assigned series.
   * @param {HTMLCanvasElement} canvas - Metric canvas identifying the series.
   */
  function draw(canvas) {
    const frame = canvasFrame(canvas);
    const values = series[canvas.dataset.series];
    if (!frame || !values?.length) return;
    const { ctx, height, width } = frame;
    const min = Math.min(...values);
    const span = Math.max(...values) - min;
    const points = values.map((value, index) => [
      values.length === 1 ? width / 2 : 3 + index * (width - 6) / (values.length - 1),
      span ? height - 3 - (value - min) * (height - 6) / span : height / 2,
    ]);
    const style = getComputedStyle(canvas);

    if (points.length > 1) {
      plot(ctx, points);
      ctx.lineTo(points.at(-1)[0], height);
      ctx.lineTo(points[0][0], height);
      ctx.closePath();
      ctx.fillStyle = style.getPropertyValue('--metric-tint').trim();
      ctx.fill();
      plot(ctx, points);
      ctx.strokeStyle = style.color;
      ctx.lineWidth = 1.6;
      ctx.lineJoin = 'round';
      ctx.stroke();
    }
    const [x, y] = points.at(-1);
    ctx.beginPath();
    ctx.arc(x, y, 2, 0, Math.PI * 2);
    ctx.fillStyle = style.color;
    ctx.fill();
  }

  const resize = new ResizeObserver(entries => entries.forEach(entry => draw(entry.target)));
  charts.forEach(canvas => resize.observe(canvas));
  /**
   * Replace the metric series and redraw the visible sparklines.
   * @param {Object} data - Values keyed by metric name.
   */
  return data => {
    series = data;
    charts.forEach(draw);
  };
}

/**
 * Initialize chart controls and create a renderer for event summaries.
 * @returns {function(Object, string): void} Function that updates both charts.
 */
export function createCharts() {
  const canvas = document.querySelector('#trend-chart');
  const tooltip = document.querySelector('#trend-tooltip');
  const selection = document.querySelector('#trend-selection');
  const empty = document.querySelector('#trend-status');
  const donut = document.querySelector('#donut');
  const donutTotal = document.querySelector('#donut-total');
  const legend = document.querySelector('#hazard-legend');
  let summary = null;
  let pointers = [];
  let selectedIndex = -1;

  /** Draw the daily event trend and save point positions for chart interaction. */
  function drawTrend() {
    const frame = canvasFrame(canvas);
    if (!frame) return;
    const { ctx, width, height } = frame;
    const values = summary?.days || [];
    const scaleMax = Math.max(0, ...values.map(day => day.count));
    const step = axisStep(Math.max(scaleMax, 4));
    const ceiling = Math.max(4 * step, Math.ceil(scaleMax / step) * step);
    const small = width < 355;
    const layout = { left: scaleMax >= 1000 ? 41 : 34, right: width - 13, top: 12, bottom: height - 26 };
    /**
     * Convert a day index to a horizontal plot position.
     * @param {number} index - Index in the daily series.
     * @returns {number} Horizontal position in CSS pixels.
     */
    const x = index => (layout.right - layout.left) * (values.length === 1 ? .5 : index / (values.length - 1)) + layout.left;
    /**
     * Convert an event count to a vertical plot position.
     * @param {number} value - Daily event count.
     * @returns {number} Vertical position in CSS pixels.
     */
    const y = value => layout.bottom - (layout.bottom - layout.top) * value / ceiling;

    ctx.font = '700 10px Manrope, system-ui, sans-serif';
    ctx.fillStyle = '#526b86';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let n = 0; n <= ceiling; n += step) {
      ctx.strokeStyle = n === 0 ? '#b8cce0' : '#e1ebf5';
      plot(ctx, [[layout.left, y(n)], [layout.right, y(n)]]);
      ctx.stroke();
      ctx.fillText(summary ? number.format(n) : '–', layout.left - 6, y(n));
    }

    // Limit crowded date labels while keeping the first and last day visible.
    const sampleCount = Math.min(values.length, small ? 4 : values.length === 7 ? 7 : 6);
    const labels = new Set(Array.from({ length: sampleCount }, (_, index) =>
      Math.round(index * (values.length - 1) / Math.max(1, sampleCount - 1))));
    ctx.textBaseline = 'top';
    for (const index of labels) {
      ctx.textAlign = index === 0 ? 'left' : index === values.length - 1 ? 'right' : 'center';
      ctx.fillText(dayLabel.format(new Date(values[index].day)), x(index), layout.bottom + 7);
    }

    pointers = scaleMax ? values.map((day, index) => [x(index), y(day.count)]) : [];
    if (!pointers.length) return;
    const area = ctx.createLinearGradient(0, layout.top, 0, layout.bottom);
    area.addColorStop(0, '#3f9eee60');
    area.addColorStop(1, '#b6e0ff1c');
    plot(ctx, pointers);
    ctx.lineTo(pointers.at(-1)[0], layout.bottom);
    ctx.lineTo(pointers[0][0], layout.bottom);
    ctx.closePath();
    ctx.fillStyle = area;
    ctx.fill();
    plot(ctx, pointers);
    ctx.strokeStyle = '#1e79cd';
    ctx.lineWidth = 2.2;
    ctx.lineJoin = 'round';
    ctx.stroke();
    if (values.length <= 14 || selectedIndex >= 0) {
      pointers.forEach(([cx, cy], index) => {
        if (values.length > 14 && index !== selectedIndex) return;
        ctx.beginPath();
        ctx.arc(cx, cy, index === selectedIndex ? 4.2 : 3.1, 0, Math.PI * 2);
        ctx.fillStyle = '#1d7ad0';
        ctx.fill();
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = 1.5;
        ctx.stroke();
      });
    }
  }

  /**
   * Highlight one trend point and update its tooltip.
   * @param {number} index - Day index, or -1 to clear the selection.
   * @param {boolean} [announce=false] - Whether to announce the value to assistive technology.
   */
  function select(index, announce = false) {
    selectedIndex = index;
    drawTrend();
    if (index < 0 || !pointers[index]) {
      tooltip.hidden = true;
      return;
    }
    const { day, count } = summary.days[index];
    const detail = `${dayLabel.format(new Date(day))} · ${number.format(count)} event records`;
    tooltip.textContent = detail;
    tooltip.hidden = false;
    const tipWidth = tooltip.offsetWidth;
    tooltip.style.left = Math.max(0, Math.min(canvas.clientWidth - tipWidth, pointers[index][0] - tipWidth / 2)) + 'px';
    tooltip.style.top = Math.max(0, pointers[index][1] - 29) + 'px';
    if (announce) selection.textContent = detail;
  }

  // Snap the tooltip to the nearest day, even between plotted points.
  canvas.addEventListener('pointermove', event => {
    if (!pointers.length) return;
    const position = event.clientX - canvas.getBoundingClientRect().left;
    const index = pointers.reduce((closest, value, i) =>
      Math.abs(value[0] - position) < Math.abs(pointers[closest][0] - position) ? i : closest, 0);
    if (index !== selectedIndex) select(index);
  });
  canvas.addEventListener('pointerleave', () => select(-1));
  canvas.addEventListener('blur', () => select(-1));
  canvas.addEventListener('keydown', event => {
    if (!pointers.length || !['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const last = pointers.length - 1;
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? last :
      Math.max(0, Math.min(last, selectedIndex + (event.key === 'ArrowRight' ? 1 : -1)));
    select(next, true);
  });
  const resize = new ResizeObserver(drawTrend);
  resize.observe(canvas);
  document.fonts.ready.then(drawTrend);

  let lastMessage;
  /**
   * Render the event trend and hazard breakdown from the latest summary.
   * @param {Object|null|undefined} snapshot - Summary data, if available.
   * @param {string} message - Status text to show while data is unavailable.
   */
  return function updateCharts(snapshot, message) {
    if (snapshot === summary && message === lastMessage) return;
    summary = snapshot;
    lastMessage = message;
    selectedIndex = -1;
    tooltip.hidden = true;
    empty.textContent = snapshot ? snapshot.total ? '' : 'No event records in this selection' : message;
    const total = snapshot?.total || 0;
    donutTotal.textContent = snapshot ? number.format(total) : '—';
    let offset = 0;
    const slices = [];
    legend.replaceChildren(...Object.entries(HAZARDS).map(([key, { label }]) => {
      const color = chartColors[key];
      const count = snapshot?.counts[key] || 0;
      const share = total ? count * 100 / total : 0;
      if (count) {
        slices.push({ color, start: offset, end: offset + share });
        offset += share;
      }
      const item = document.createElement('li');
      const dot = document.createElement('i');
      dot.className = 'legend-dot';
      dot.style.setProperty('--hazard', color);
      const name = document.createElement('span');
      name.textContent = label;
      const percentage = document.createElement('b');
      percentage.textContent = snapshot ? share.toFixed(1) + '%' : '—';
      if (snapshot) item.title = `${number.format(count)} event records`;
      item.append(dot, name, percentage);
      return item;
    }));
    const segments = slices.map(({ color, start, end }) => {
      const boundary = end - Math.min(0.35, (end - start) / 8);
      return `${color} ${start}% ${boundary}%, white ${boundary}% ${end}%`;
    });
    const fill = slices.length > 1 ? `conic-gradient(${segments.join(', ')})` : slices[0]?.color ?? '#ecf1f7';
    donut.style.setProperty('--donut', fill);
    drawTrend();
  };
}
