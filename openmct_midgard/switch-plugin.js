// switch-plugin.js

// ---- Display configuration (edit here — no Python involved) ----
var SWITCH_DISPLAY_CONFIG = {
  namePosition: 'top',   // 'top' | 'bottom' | 'left' | 'right'
  nameAlign: 'center',   // 'left' | 'center' | 'right'
  valueAlign: 'center',  // 'left' | 'center' | 'right'
};

// Optional per-switch overrides, keyed by the element's tree key.
// Leave empty to use SWITCH_DISPLAY_CONFIG everywhere.
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

// ---- Hides the Display Layout hover toolbar (View Large / Save to Notebook) ----
// No official Open MCT API exists for this — it's a scoped DOM workaround.
// If these class names don't match your Open MCT version, right-click the icon
// in the browser -> Inspect, and update selectorsToHide below.
function hideFrameOverlayControls(viewElement) {
  var frame = viewElement.closest('.c-so-view') || viewElement.closest('.l-layout__frame') || viewElement.parentElement;
  if (!frame) return null;

  var selectorsToHide = [
    '.c-so-view__view-large',
    '.icon-expand',
    '.c-notebook-snapshot-menubutton',
    '.icon-camera',
    '[aria-label="View Large"]',
    '[aria-label="Save to Notebook"]'
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

function SwitchViewPlugin() {
  return function install(openmct) {

    function isSwitchable(domainObject) {
      if (domainObject.type !== 'midgard.telemetry') return false;
      var metadata = openmct.telemetry.getMetadata(domainObject);
      if (!metadata) return false;
      var valueDef = metadata.valueMetadatas.find(function (v) { return v.key === 'value'; });
      return !!(valueDef && valueDef.format === 'enum' && valueDef.enumerations && valueDef.enumerations.length >= 2);
    }

    openmct.objectViews.addProvider({
      key: 'midgard-switch-view',
      name: 'Switch',
      cssClass: 'icon-switch',
      canView: isSwitchable,
      view: function (domainObject) {
        var container, unsubscribe, overlayObserver;
        var currentValue = null;
        var pending = false;

        var cfg = getSwitchConfig(domainObject);
        var metadata = openmct.telemetry.getMetadata(domainObject);
        var valueDef = metadata.valueMetadatas.find(function (v) { return v.key === 'value'; });
        var enumerations = valueDef.enumerations;
        var isBoolean = enumerations.length === 2;

        var style = valueDef.style || {};
        var COLOR_SELECTED = style.selected || '#5b9bd5';
        var COLOR_UNSELECTED = style.unselected || '#41464c';
        var COLOR_PENDING = '#888888';

        function send(requestedValue) {
          if (pending) return;
          pending = true;
          render();
          midgardSocket.send(JSON.stringify({
            cmd: 'request',
            key: domainObject.identifier.key,
            requested: requestedValue
          }));
        }

        function render() {
          if (!container) return;
          container.innerHTML = '';

          var flexDir = { top: 'column', bottom: 'column-reverse', left: 'row', right: 'row-reverse' }[cfg.namePosition] || 'column';
		  container.style.display = 'flex';
		  container.style.flexDirection = flexDir;
		  container.style.alignItems = (flexDir === 'row' || flexDir === 'row-reverse') ? 'center' : 'stretch';   // ← add this line
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
		  controlArea.style.cssText = 'flex:1; min-width:0; min-height:0; display:flex; align-self:stretch;';   // ← added align-self:stretch
		  container.appendChild(controlArea);

          if (isBoolean) {
            var btn = document.createElement('button');
            var current = enumerations.find(function (e) { return e.value === currentValue; });
            btn.textContent = current ? current.string : '—';
            btn.style.cssText =
              'width:100%; height:100%; min-height:32px; border:none; border-radius:4px; ' +
              'font-size:14px; cursor:pointer; color:#fff; padding:0 10px; box-sizing:border-box; ' +
              'text-align:' + cfg.valueAlign + '; background:' +
              (pending ? COLOR_PENDING : (currentValue === enumerations[1].value ? COLOR_SELECTED : COLOR_UNSELECTED));
            btn.onclick = function () {
              var next = currentValue === enumerations[0].value ? enumerations[1].value : enumerations[0].value;
              send(next);
            };
            controlArea.appendChild(btn);
          } else {
            var row = document.createElement('div');
            row.style.cssText = 'display:flex; gap:4px; width:100%; height:100%;';
            enumerations.forEach(function (e) {
              var btn = document.createElement('button');
              btn.textContent = e.string;
              var isSelected = e.value === currentValue;
              btn.style.cssText =
                'flex:1; border:none; border-radius:4px; font-size:13px; cursor:pointer; color:#fff; ' +
                'padding:0 8px; box-sizing:border-box; text-align:' + cfg.valueAlign + '; background:' +
                (pending ? COLOR_PENDING : (isSelected ? COLOR_SELECTED : COLOR_UNSELECTED));
              btn.onclick = function () { send(e.value); };
              row.appendChild(btn);
            });
            controlArea.appendChild(row);
          }
        }

        return {
          show: function (element) {
            container = document.createElement('div');
            container.style.cssText = 'width:100%; height:100%; padding:4px; box-sizing:border-box;';
            element.appendChild(container);
            render();

            overlayObserver = hideFrameOverlayControls(element);

            openmct.telemetry.request(domainObject, { start: 0, end: Date.now() })
              .then(function (points) {
                if (points && points.length) {
                  currentValue = points[points.length - 1].value;
                  render();
                }
              });

            unsubscribe = openmct.telemetry.subscribe(domainObject, function (datum) {
              currentValue = datum.value;
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
  };
}