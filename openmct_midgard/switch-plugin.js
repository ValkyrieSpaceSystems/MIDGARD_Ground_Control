// switch-plugin.js — Switch (boolean), Selector (multi-state), Button (stateless trigger)

// ---- Display configuration (edit here — no Python involved) ----
var SWITCH_DISPLAY_CONFIG = {
  namePosition: 'top',   // 'top' | 'bottom' | 'left' | 'right'
  nameAlign: 'center',   // 'left' | 'center' | 'right'
  valueAlign: 'center',  // 'left' | 'center' | 'right'
};

var SWITCH_DISPLAY_OVERRIDES = {
  // 'Valhala_I.Actuators.main_lock': { namePosition: 'left', nameAlign: 'right' },
};

// ---- Confirmation-key tracking (client-side, per-viewer) ----
var JS_KEY_TO_PYNPUT_NAME = {
  'Shift': 'shift', 'Control': 'ctrl', 'Alt': 'alt', 'Meta': 'cmd',
  'CapsLock': 'caps_lock', 'Tab': 'tab', 'Enter': 'enter',
  'Escape': 'esc', 'Backspace': 'backspace', ' ': 'space',
  'ArrowUp': 'up', 'ArrowDown': 'down', 'ArrowLeft': 'left', 'ArrowRight': 'right',
  'Home': 'home', 'End': 'end', 'PageUp': 'page_up', 'PageDown': 'page_down',
  'Delete': 'delete', 'Insert': 'insert',
  'F1': 'f1', 'F2': 'f2', 'F3': 'f3', 'F4': 'f4', 'F5': 'f5', 'F6': 'f6',
  'F7': 'f7', 'F8': 'f8', 'F9': 'f9', 'F10': 'f10', 'F11': 'f11', 'F12': 'f12',
  'Pause': 'pause', 'ScrollLock': 'scroll_lock', 'PrintScreen': 'print_screen', 'ContextMenu': 'menu'
};

function canonicalKeyName(jsKey) {
  return JS_KEY_TO_PYNPUT_NAME[jsKey] || jsKey.toLowerCase();
}

var pressedKeys = new Set();
document.addEventListener('keydown', function (e) { pressedKeys.add(canonicalKeyName(e.key)); });
document.addEventListener('keyup', function (e) { pressedKeys.delete(canonicalKeyName(e.key)); });
window.addEventListener('blur', function () { pressedKeys.clear(); });   // prevents a "stuck" key if the tab loses focus mid-hold

function getSwitchConfig(domainObject) {
  return Object.assign(
    {},
    SWITCH_DISPLAY_CONFIG,
    domainObject.switchDisplay || {},
    SWITCH_DISPLAY_OVERRIDES[domainObject.identifier.key] || {}
  );
}

function sendCommand(key, payload) {
  var full = Object.assign({ key: key, pressed_keys: Array.from(pressedKeys) }, payload);
  midgardSocket.send(JSON.stringify(full));
}

// ---- Hides the Display Layout hover toolbar (View Large / Save to Notebook) ----
// No official Open MCT API exists for this — it's a scoped DOM workaround.
function hideFrameOverlayControls(viewElement) {
  var frame = viewElement.closest('.c-so-view') || viewElement.closest('.l-layout__frame') || viewElement.parentElement;
  if (!frame) return null;

  var selectorsToHide = [
    '.c-so-view__view-large', '.icon-expand',
    '.c-notebook-snapshot-menubutton', '.icon-camera',
    '[aria-label="View Large"]', '[aria-label="Save to Notebook"]'
  ];
  function applyHide() {
    selectorsToHide.forEach(function (sel) {
      frame.querySelectorAll(sel).forEach(function (el) { el.style.display = 'none'; });
    });
  }
  applyHide();
  var observer = new MutationObserver(applyHide);
  observer.observe(frame, { childList: true, subtree: true });
  return observer;
}

function buildNameWrapper(cfg, domainObject) {
  var container = document.createElement('div');
  var flexDir = { top: 'column', bottom: 'column-reverse', left: 'row', right: 'row-reverse' }[cfg.namePosition] || 'column';
  container.style.display = 'flex';
  container.style.flexDirection = flexDir;
  container.style.alignItems = (flexDir === 'row' || flexDir === 'row-reverse') ? 'center' : 'stretch';
  container.style.width = '100%';
  container.style.height = '100%';
  container.style.gap = '4px';
  container.style.boxSizing = 'border-box';

  var nameEl = document.createElement('div');
  nameEl.textContent = domainObject.name;
  nameEl.style.cssText = 'font-size:12px; color:#ccc; flex-shrink:0; text-align:' + cfg.nameAlign + ';';
  if (flexDir === 'column' || flexDir === 'column-reverse') nameEl.style.width = '100%';
  container.appendChild(nameEl);

  var controlArea = document.createElement('div');
  controlArea.style.cssText = 'flex:1; min-width:0; min-height:0; display:flex; align-self:stretch;';
  container.appendChild(controlArea);

  return { container: container, controlArea: controlArea };
}

function getValueMetadata(openmct, domainObject) {
  var metadata = openmct.telemetry.getMetadata(domainObject);
  if (!metadata) return null;
  return metadata.valueMetadatas.find(function (v) { return v.key === 'value'; });
}

function fetchHistory(key) {
  var port = (typeof MIDGARD_TELEMETRY_PORT !== 'undefined') ? MIDGARD_TELEMETRY_PORT : 4001;
  var url = 'http://' + window.location.hostname + ':' + port + '/history/' + key + '?start=0&end=' + Date.now();
  return fetch(url).then(function (r) { return r.json(); });
}

function SwitchViewPlugin() {
  return function install(openmct) {

    // ============ SWITCH — exactly 2 states ============
    openmct.objectViews.addProvider({
      key: 'midgard-switch-view',
      name: 'Switch',
      cssClass: 'icon-switch',
      canView: function (domainObject) {
        if (domainObject.type !== 'midgard.control') return false;
        var v = getValueMetadata(openmct, domainObject);
        return !!(v && v.format === 'enum' && v.enumerations && v.enumerations.length === 2);
      },
      view: function (domainObject) {
        var container, unsubscribe, overlayObserver;
        var currentValue = null, currentInhibited = false, pending = false;
        var cfg = getSwitchConfig(domainObject);
        var valueDef = getValueMetadata(openmct, domainObject);
        var enumerations = valueDef.enumerations;
        var style = valueDef.style || {};
        var COLOR_SELECTED = style.selected || '#5b9bd5';
        var COLOR_UNSELECTED = style.unselected || '#41464c';
        var COLOR_INHIBITED = style.inhibited || '#2c2f33';
        var COLOR_PENDING = '#888888';

        function send(requestedValue) {
          if (pending) return;
          pending = true;
          render();
          sendCommand(domainObject.identifier.key, { cmd: 'request', requested: requestedValue });
        }

        function render() {
          if (!container) return;
          container.innerHTML = '';
          var parts = buildNameWrapper(cfg, domainObject);
          container.appendChild(parts.container);

          var btn = document.createElement('button');
          var current = enumerations.find(function (e) { return e.value === currentValue; });
          var isOn = currentValue === enumerations[1].value;
          btn.textContent = current ? current.string : '—';
          btn.style.cssText =
            'width:100%; height:100%; min-height:32px; border:none; border-radius:4px; ' +
            'font-size:14px; cursor:pointer; color:#fff; padding:0 10px; box-sizing:border-box; ' +
            'text-align:' + cfg.valueAlign + '; background:' +
            (pending ? COLOR_PENDING : (isOn ? COLOR_SELECTED : (currentInhibited ? COLOR_INHIBITED : COLOR_UNSELECTED)));
          btn.onclick = function () {
            send(currentValue === enumerations[0].value ? enumerations[1].value : enumerations[0].value);
          };
          parts.controlArea.appendChild(btn);
        }

        return {
          show: function (element) {
            container = document.createElement('div');
            container.style.cssText = 'width:100%; height:100%; padding:4px; box-sizing:border-box;';
            element.appendChild(container);
            render();
            overlayObserver = hideFrameOverlayControls(element);

            openmct.telemetry.request(domainObject, { start: 0, end: Date.now() }).then(function (points) {
              if (points && points.length) {
                currentValue = points[points.length - 1].value;
                currentInhibited = !!points[points.length - 1].inhibited;
                render();
              }
            });
            unsubscribe = openmct.telemetry.subscribe(domainObject, function (datum) {
              currentValue = datum.value;
              currentInhibited = !!datum.inhibited;
              pending = false;
              render();
            });
          },
          destroy: function () {
            if (unsubscribe) unsubscribe();
            if (overlayObserver) overlayObserver.disconnect();
          }
        };
      },
      priority: function () { return 1; }
    });

    // ============ SELECTOR — 3+ states, segmented track ============
    openmct.objectViews.addProvider({
      key: 'midgard-selector-view',
      name: 'Selector',
      cssClass: 'icon-dial',
      canView: function (domainObject) {
        if (domainObject.type !== 'midgard.control') return false;
        var v = getValueMetadata(openmct, domainObject);
        return !!(v && v.format === 'enum' && v.enumerations && v.enumerations.length > 2);
      },
      view: function (domainObject) {
        var container, unsubscribe, overlayObserver;
        var currentValue = null, currentInhibited = null, pendingValue = null;
        var cfg = getSwitchConfig(domainObject);
        var valueDef = getValueMetadata(openmct, domainObject);
        var enumerations = valueDef.enumerations;
        var style = valueDef.style || {};
        var COLOR_SELECTED = style.selected || '#5b9bd5';
        var COLOR_UNSELECTED = style.unselected || '#41464c';
        var COLOR_INHIBITED = style.inhibited || '#2c2f33';
        var COLOR_PENDING = '#888888';

        function send(requestedValue) {
					if (pendingValue !== null) return;
					pendingValue = requestedValue;
					render();
					sendCommand(domainObject.identifier.key, { cmd: 'request', requested: requestedValue });
				}

        function render() {
          if (!container) return;
          container.innerHTML = '';
          var parts = buildNameWrapper(cfg, domainObject);
          container.appendChild(parts.container);

          var track = document.createElement('div');
          track.style.cssText =
            'display:flex; width:100%; height:100%; min-height:32px; border-radius:4px; overflow:hidden;';
          enumerations.forEach(function (e, i) {
            var seg = document.createElement('div');
            var isSelected = e.value === currentValue;
            var segInhibited = Array.isArray(currentInhibited) && !!currentInhibited[i];
            seg.textContent = e.string;
            var isPending = pendingValue === e.value;
						seg.style.cssText =
							'flex:1; display:flex; align-items:center; justify-content:' +
							({ left: 'flex-start', center: 'center', right: 'flex-end' }[cfg.valueAlign] || 'center') + '; ' +
							'padding:0 8px; font-size:12px; cursor:pointer; color:#fff; box-sizing:border-box; ' +
							(i < enumerations.length - 1 ? 'border-right:1px solid rgba(0,0,0,0.4); ' : '') +
							'background:' + (isPending ? COLOR_PENDING : (isSelected ? COLOR_SELECTED : (segInhibited ? COLOR_INHIBITED : COLOR_UNSELECTED)));
            seg.onclick = function () { send(e.value); };
            track.appendChild(seg);
          });
          parts.controlArea.appendChild(track);
        }

        return {
          show: function (element) {
            container = document.createElement('div');
            container.style.cssText = 'width:100%; height:100%; padding:4px; box-sizing:border-box;';
            element.appendChild(container);
            render();
            overlayObserver = hideFrameOverlayControls(element);

            openmct.telemetry.request(domainObject, { start: 0, end: Date.now() }).then(function (points) {
              if (points && points.length) {
                currentValue = points[points.length - 1].value;
                currentInhibited = points[points.length - 1].inhibited;
                render();
              }
            });
            unsubscribe = openmct.telemetry.subscribe(domainObject, function (datum) {
							currentValue = datum.value;
							currentInhibited = datum.inhibited;
							pendingValue = null;
							render();
						});
          },
          destroy: function () {
            if (unsubscribe) unsubscribe();
            if (overlayObserver) overlayObserver.disconnect();
          }
        };
      },
      priority: function () { return 1; }
    });

    // ============ BUTTON — stateless trigger, but can still be inhibited ============
    // Type 'midgard.trigger' is intentionally NOT part of openmct.telemetry (no
    // real value/history to plot) — so inhibited status is read via a direct
    // history fetch + raw midgardSocket listener, bypassing the telemetry API.
    openmct.types.addType('midgard.trigger', { name: 'Trigger', cssClass: 'icon-button' });

    openmct.objectViews.addProvider({
      key: 'midgard-button-view',
      name: 'Button',
      cssClass: 'icon-button',
      canView: function (domainObject) { return domainObject.type === 'midgard.trigger'; },
      view: function (domainObject) {
        var container, btn, onMessage;
        var currentInhibited = false, flashing = false;
        var cfg = getSwitchConfig(domainObject);
        var COLOR_BASE = '#41464c';
        var COLOR_INHIBITED = (cfg && cfg.inhibitedColor) || '#2c2f33';
        var COLOR_FLASH = '#888888';
        var overlayObserver;

        function render() {
          if (!btn || flashing) return;
          btn.style.background = currentInhibited ? COLOR_INHIBITED : COLOR_BASE;
        }

        function fire() {
          sendCommand(domainObject.identifier.key, { cmd: 'request', requested: true });
          flashing = true;
          btn.style.background = COLOR_FLASH;
          setTimeout(function () { flashing = false; render(); }, 150);
        }

        return {
          show: function (element) {
            container = document.createElement('div');
            container.style.cssText = 'width:100%; height:100%; padding:4px; box-sizing:border-box;';
            btn = document.createElement('button');
            btn.textContent = domainObject.name;
            btn.style.cssText =
              'width:100%; height:100%; min-height:32px; border:none; border-radius:4px; ' +
              'font-size:14px; cursor:pointer; color:#fff; background:' + COLOR_BASE + ';';
            btn.onclick = fire;
            container.appendChild(btn);
            element.appendChild(container);
            overlayObserver = hideFrameOverlayControls(element);

            fetchHistory(domainObject.identifier.key).then(function (points) {
              if (points && points.length) {
                currentInhibited = !!points[points.length - 1].inhibited;
                render();
              }
            });

            onMessage = function (event) {
              var point = JSON.parse(event.data);
              if (point.key === domainObject.identifier.key) {
                currentInhibited = !!point.inhibited;
                render();
              }
            };
            midgardSocket.addEventListener('message', onMessage);
          },
          destroy: function () {
            if (onMessage) midgardSocket.removeEventListener('message', onMessage);
            if (overlayObserver) overlayObserver.disconnect();
          }
        };
      },
      priority: function () { return 1; }
    });
    // ============ MESSAGE — scrolling log view for string-formatted telemetry ============
		openmct.objectViews.addProvider({
			key: 'midgard-terminal-view',
			name: 'Message',
			cssClass: 'icon-terminal',
			canView: function (domainObject) {
				if (domainObject.type !== 'midgard.telemetry') return false;
				var v = getValueMetadata(openmct, domainObject);
				return !!(v && v.format === 'string');
			},
			view: function (domainObject) {
				var container, onMessage;

				function appendLine(text, utc) {
				  var line = document.createElement('div');
				  var time = new Date(utc).toISOString().substr(11, 8);
				  line.textContent = text;
				  container.appendChild(line);
				  container.scrollTop = container.scrollHeight;
				}

				return {
				  show: function (element) {
				    container = document.createElement('div');
				    container.style.cssText =
				      'height:100%; width:100%; overflow-y:auto; background:#41464c; color:#fff; ' +
				      'font-family:monospace; font-size:13px; padding:8px; box-sizing:border-box; white-space:pre-wrap;';
				    element.appendChild(container);

				    fetchHistory(domainObject.identifier.key).then(function (points) {
				      points.forEach(function (p) { appendLine(p.value, p.utc); });
				    });

				    onMessage = function (event) {
				      var point = JSON.parse(event.data);
				      if (point.key === domainObject.identifier.key) appendLine(point.value, point.utc);
				    };
				    midgardSocket.addEventListener('message', onMessage);
				  },
				  destroy: function () {
				    if (onMessage) midgardSocket.removeEventListener('message', onMessage);
				  }
				};
			},
			priority: function () { return 1; }
		});
  };
}