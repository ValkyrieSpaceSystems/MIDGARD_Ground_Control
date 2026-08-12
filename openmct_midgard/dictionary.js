// Edit this tree to match your actual peripherals/sensors.
// Any node with "children" becomes a folder.
// Any node with "measurement" becomes a telemetry point.
var TELEMETRY_TREE = {
  name: 'Ground Station',
  key: 'root',
  children: [
    {
      name: 'Rocket 1',
      key: 'rocket1',
      children: [
        { name: 'Altitude', key: 'rocket1.altitude', measurement: { units: 'm', format: 'float' } },
        { name: 'Velocity', key: 'rocket1.velocity', measurement: { units: 'm/s', format: 'float' } },
        { name: 'Arm Switch', key: 'rocket1.arm', measurement: { format: 'boolean' } }
      ]
    }
  ]
};

function DictionaryPlugin() {
  return function install(openmct) {
    var NAMESPACE = 'rocket';
    var nodesByKey = {};
    var parentByKey = {};

    (function index(node, parentKey) {
      nodesByKey[node.key] = node;
      parentByKey[node.key] = parentKey;
      (node.children || []).forEach(function (child) { index(child, node.key); });
    })(TELEMETRY_TREE, null);

    function toDomainObject(node) {
      var identifier = { namespace: NAMESPACE, key: node.key };
      var location = parentByKey[node.key] ? NAMESPACE + ':' + parentByKey[node.key] : 'ROOT';

      if (node.children) {
        return { identifier: identifier, name: node.name, type: 'folder', location: location };
      }
      return {
        identifier: identifier,
        name: node.name,
        type: 'rocket.telemetry',
        location: location,
        telemetry: {
          values: [
            { key: 'value', name: node.name, units: node.measurement.units,
              format: node.measurement.format || 'float', hints: { range: 1 } },
            { key: 'utc', name: 'Timestamp', format: 'utc', hints: { domain: 1 } }
          ]
        }
      };
    }

    openmct.objects.addProvider(NAMESPACE, {
      get: function (identifier) { return Promise.resolve(toDomainObject(nodesByKey[identifier.key])); }
    });

    openmct.composition.addProvider({
      appliesTo: function (domainObject) {
        var node = nodesByKey[domainObject.identifier.key];
        return domainObject.identifier.namespace === NAMESPACE && node && node.children;
      },
      load: function (domainObject) {
        var node = nodesByKey[domainObject.identifier.key];
        return Promise.resolve(node.children.map(function (c) { return { namespace: NAMESPACE, key: c.key }; }));
      }
    });

    openmct.types.addType('rocket.telemetry', { name: 'Telemetry Point', cssClass: 'icon-telemetry' });
    openmct.objects.addRoot({ namespace: NAMESPACE, key: TELEMETRY_TREE.key });
  };
}