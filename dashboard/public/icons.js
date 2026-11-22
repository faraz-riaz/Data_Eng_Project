const shapes = {
  activity: '<path d="M2 12h4l3-7 4 14 3-7h6"/>',
  alert: '<path d="M10.3 4.3 2.7 18a1.5 1.5 0 0 0 1.3 2h16a1.5 1.5 0 0 0 1.3-2L13.7 4.3a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4m0 3h.01"/>',
  calendar: '<rect x="3" y="5" width="18" height="16" rx="2"/><path d="M8 3v4m8-4v4M3 10h18m-13 5h.01m4 0h.01m4 0h.01"/>',
  close: '<path d="m6 6 12 12M18 6 6 18"/>',
  dots: '<circle cx="5" cy="12" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="19" cy="12" r="1"/>',
  drop: '<path d="M12 2.5c4 4.3 6.5 7.8 6.5 11.5a6.5 6.5 0 0 1-13 0C5.5 10.3 8 6.8 12 2.5Z"/><path d="M8.5 14.7c.3 1.2.8 1.8 1.9 2.4"/>',
  flame: '<path d="M12 22a7.5 7.5 0 0 0 7.5-7.5c0-3.5-2-5.8-3.5-7-1.2 2.5-2.6 3.5-3.5 3.7.2-3.4-1.6-6.5-4-8.7.6 3.8-1.2 5.6-2.6 7.6C4.9 11.5 4.5 13 4.5 14.5A7.5 7.5 0 0 0 12 22Z"/><path d="M9 18a3 3 0 0 0 6 0c0-1.3-1.4-2.9-3-4.5C10.4 15.1 9 16.7 9 18Z"/>',
  globe: '<circle cx="12" cy="12" r="9"/><ellipse cx="12" cy="12" rx="4" ry="9"/><path d="M3 12h18"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v5m0-8h.01"/>',
  layers: '<path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
  locate: '<circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="1.5"/><path d="M12 2v4m0 12v4M2 12h4m12 0h4"/>',
  minus: '<path d="M5 12h14"/>',
  pin: '<path d="M19 10c0 5-7 10.5-7 10.5S5 15 5 10a7 7 0 1 1 14 0Z"/><circle cx="12" cy="10" r="2.3"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  refresh: '<path d="M20 11a8 8 0 0 0-13-6l-3 3m0-4v4h4m-4 5a8 8 0 0 0 13 6l3-3m0 4v-4h-4"/>',
  storm: '<path d="M20 7.3c-3.7-4.5-10-3.9-13-.1-2.2 2.8-1.1 7.3 2.2 8.1 2.4.6 5-1 5-3.2M4 16.7c3.7 4.5 10 3.9 13 .1 2.2-2.8 1.1-7.3-2.2-8.1-2.4-.6-5 1-5 3.2"/>',
  volcano: '<path d="m2 20 6.4-12h7.2L22 20H2Zm8-16-.7-1.7M14 4l.8-1.7M12 13v3m0 2h.01"/>',
};

export function icon(name) {
  return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${shapes[name] || shapes.dots}</svg>`;
}

export function installIcons() {
  document.querySelectorAll('[data-icon]').forEach(node => {
    node.innerHTML = icon(node.dataset.icon);
  });
}
