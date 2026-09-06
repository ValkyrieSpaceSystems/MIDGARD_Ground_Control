var midgardSocket = new WebSocket(
  'ws://' + window.location.hostname + ':' + (typeof MIDGARD_TELEMETRY_PORT !== 'undefined' ? MIDGARD_TELEMETRY_PORT : 4001) + '/realtime'
);

function RealtimeTelemetryPlugin() {
  return function install(openmct) {
    var listeners = {};

    midgardSocket.addEventListener('message', function (event) {
      var point = JSON.parse(event.data);
      var callbacks = listeners[point.key] || [];
      callbacks.forEach(function (callback) { callback(point); });
    });

    openmct.telemetry.addProvider({
      supportsSubscribe: function (domainObject) {
				return domainObject.type === 'midgard.telemetry' || domainObject.type === 'midgard.control';
			},
			subscribe: function (domainObject, callback) {
        var key = domainObject.identifier.key;
        listeners[key] = listeners[key] || [];
        listeners[key].push(callback);
        return function unsubscribe() {
          listeners[key] = listeners[key].filter(function (cb) { return cb !== callback; });
        };
      }
    });
  };
}