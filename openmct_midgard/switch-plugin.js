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

function getSwitchConfig(domainObject) {
  return Object.assign(
    {},
    SWITCH_DISPLAY_CONFIG,
    domainObject.switchDisplay || {},
    SWITCH_DISPLAY_OVERRIDES[domainObject.identifier.key] || {}
  );
}

function sendCommand(key, payload) {
  midgardSocket.send(JSON.stringify(Object.assign({ key: key }, payload)));
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

function SwitchViewPlugin() {
  return function install(openmct) {

    // ============ SWITCH — exactly 2 states ============
    openmct.objectViews.addProvider({
      key: 'midgard-switch-view',
      name: 'Switch',
      cssClass: 'icon-switch',
      canView: function (domainObject) {
        if (domainObject.type !== 'midgard.telemetry') return false;
        var v = getValueMetadata(openmct, domainObject);
        return !!(v && v.format === 'enum' && v.enumerations && v.enumerations.length === 2);
      },
      view: function (domainObject) {
        var container, unsubscribe, overlayObserver;
        var currentValue = null, pending = false;
        var cfg = getSwitchConfig(domainObject);
        var valueDef = getValueMetadata(openmct, domainObject);
        var enumerations = valueDef.enumerations;
        var style = valueDef.style || {};
        var COLOR_SELECTED = style.selected || '#5b9bd5';
        var COLOR_UNSELECTED = style.unselected || '#41464c';
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
          btn.textContent = current ? current.string : '—';
          btn.style.cssText =
            'width:100%; height:100%; min-height:32px; border:none; border-radius:4px; ' +
            'font-size:14px; cursor:pointer; color:#fff; padding:0 10px; box-sizing:border-box; ' +
            'text-align:' + cfg.valueAlign + '; background:' +
            (pending ? COLOR_PENDING : (currentValue === enumerations[1].value ? COLOR_SELECTED : COLOR_UNSELECTED));
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
            	console.log('[MIDGARD DEBUG]', domainObject.identifier.key, 'points:', points, 'enumerations:', enumerations);
              if (points && points.length) { currentValue = points[points.length - 1].value; render(); }
            });
            unsubscribe = openmct.telemetry.subscribe(domainObject, function (datum) {
              currentValue = datum.value; pending = false; render();
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
        if (domainObject.type !== 'midgard.telemetry') return false;
        var v = getValueMetadata(openmct, domainObject);
        return !!(v && v.format === 'enum' && v.enumerations && v.enumerations.length > 2);
      },
      view: function (domainObject) {
        var container, unsubscribe, overlayObserver;
        var currentValue = null, pending = false;
        var cfg = getSwitchConfig(domainObject);
        var valueDef = getValueMetadata(openmct, domainObject);
        var enumerations = valueDef.enumerations;
        var style = valueDef.style || {};
        var COLOR_SELECTED = style.selected || '#5b9bd5';
        var COLOR_UNSELECTED = style.unselected || '#2c2f33';
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

          var track = document.createElement('div');
          track.style.cssText =
            'display:flex; width:100%; height:100%; min-height:32px; border-radius:4px; overflow:hidden;';
          enumerations.forEach(function (e, i) {
            var seg = document.createElement('div');
            var isSelected = e.value === currentValue;
            seg.textContent = e.string;
            seg.style.cssText =
              'flex:1; display:flex; align-items:center; justify-content:' +
              ({ left: 'flex-start', center: 'center', right: 'flex-end' }[cfg.valueAlign] || 'center') + '; ' +
              'padding:0 8px; font-size:12px; cursor:pointer; color:#fff; box-sizing:border-box; ' +
              (i < enumerations.length - 1 ? 'border-right:1px solid rgba(0,0,0,0.4); ' : '') +
              'background:' + (pending ? COLOR_PENDING : (isSelected ? COLOR_SELECTED : COLOR_UNSELECTED));
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
              if (points && points.length) { currentValue = points[points.length - 1].value; render(); }
            });
            unsubscribe = openmct.telemetry.subscribe(domainObject, function (datum) {
              currentValue = datum.value; pending = false; render();
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

    // ============ BUTTON — stateless trigger ============
    openmct.types.addType('midgard.trigger', { name: 'Trigger', cssClass: 'icon-button' });

    openmct.objectViews.addProvider({
      key: 'midgard-button-view',
      name: 'Button',
      cssClass: 'icon-button',
      canView: function (domainObject) { return domainObject.type === 'midgard.trigger'; },
      view: function (domainObject) {
        var container, overlayObserver;

        function fire() {
		  sendCommand(domainObject.identifier.key, { cmd: 'request', requested: true });
		  btn.style.background = '#888888';
		  setTimeout(function () { btn.style.background = '#41464c'; }, 150);
		}

        var btn;
        return {
          show: function (element) {
            container = document.createElement('div');
            container.style.cssText = 'width:100%; height:100%; padding:4px; box-sizing:border-box;';
            btn = document.createElement('button');
            btn.textContent = domainObject.name;
            btn.style.cssText =
              'width:100%; height:100%; min-height:32px; border:none; border-radius:4px; ' +
              'font-size:14px; cursor:pointer; color:#fff; background:#41464c;';
            btn.onclick = fire;
            container.appendChild(btn);
            element.appendChild(container);
            overlayObserver = hideFrameOverlayControls(element);
          },
          destroy: function () {
            if (overlayObserver) overlayObserver.disconnect();
          }
        };
      },
      priority: function () { return 1; }
    });
  };
}