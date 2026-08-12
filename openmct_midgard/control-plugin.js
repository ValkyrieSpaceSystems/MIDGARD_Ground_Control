// Edit this list to add more switches. Each becomes its own item you can
// drag into a Display Layout, same as your telemetry points.
var SWITCHES = [
  { key: 'rocket1.arm', name: 'Arm Switch' }
];

function ControlPlugin() {
  return function install(openmct) {
    var NAMESPACE = 'rocket-control';

    openmct.objects.addProvider(NAMESPACE, {
      get: function (identifier) {
        var sw = SWITCHES.filter(function (s) { return s.key === identifier.key; })[0];
        return Promise.resolve({
          identifier: identifier,
          name: sw.name,
          type: 'rocket.switch',
          location: 'ROOT'
        });
      }
    });

    SWITCHES.forEach(function (sw) {
      openmct.objects.addRoot({ namespace: NAMESPACE, key: sw.key });
    });

    openmct.types.addType('rocket.switch', {
      name: 'Switch',
      description: 'A commandable switch, confirmed by Python before it visually changes',
      cssClass: 'icon-switch'
    });

    openmct.objectViews.addProvider({
      key: 'rocket-switch-view',
      name: 'Switch View',
      canView: function (domainObject) { return domainObject.type === 'rocket.switch'; },
      view: function (domainObject) {
        var button, currentState = null, pending = false;

        function render() {
          if (!button) return;
          button.textContent = pending ? 'Pending...' : (currentState ? 'ON' : 'OFF');
          button.style.backgroundColor = pending ? '#888' : (currentState ? '#2ecc71' : '#555');
        }

        function onMessage(event) {
          var msg = JSON.parse(event.data);
          if (msg.key === domainObject.identifier.key) {
            currentState = msg.value;
            pending = false;
            render();
          }
        }

        return {
          show: function (element) {
            button = document.createElement('button');
            button.style.cssText =
              'width:100%;height:60px;font-size:18px;color:white;border:none;border-radius:4px;cursor:pointer;';
            button.addEventListener('click', function () {
              if (pending || midgardSocket.readyState !== WebSocket.OPEN) return;
              pending = true;
              render();
              midgardSocket.send(JSON.stringify({
                key: domainObject.identifier.key,
                requested: !currentState
              }));
            });
            element.appendChild(button);
            midgardSocket.addEventListener('message', onMessage);
            render();
          },
          destroy: function () {
            midgardSocket.removeEventListener('message', onMessage);
          }
        };
      }
    });
  };
}
