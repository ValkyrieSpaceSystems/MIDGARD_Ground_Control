function HistoricalTelemetryPlugin() {
  return function install(openmct) {
    openmct.telemetry.addProvider({
      supportsRequest: function (domainObject) {
				return domainObject.type === 'midgard.telemetry' || domainObject.type === 'midgard.control';
			},
      request: function (domainObject, options) {
				var port = (typeof MIDGARD_TELEMETRY_PORT !== 'undefined') ? MIDGARD_TELEMETRY_PORT : 4001;
				var key = domainObject.identifier.key;
				var url = 'http://' + window.location.hostname + ':' + port + '/history/' + key +
						       '?start=' + options.start + '&end=' + options.end;
				return fetch(url).then(function (response) {
					return response.json();
				});
			}
    });
  };
}