function HistoricalTelemetryPlugin() {
  return function install(openmct) {
    openmct.telemetry.addProvider({
      supportsRequest: function (domainObject) {
		return domainObject.type === 'midgard.telemetry';
  		},
      request: function (domainObject, options) {
        var key = domainObject.identifier.key;
        var url = 'http://localhost:4001/history/' + key +
                   '?start=' + options.start + '&end=' + options.end;
        return fetch(url).then(function (response) {
          return response.json();
        });
      }
    });
  };
}
