// summary-widget-patch.js
// Overrides the built-in Summary Widget type to make it directly creatable
// from the Create menu, without touching openmct source files.
function SummaryWidgetCreatablePatch() {
  return function install(openmct) {
    var existing = openmct.types.get('summary-widget');
    if (!existing) {
      console.warn('[SummaryWidgetCreatablePatch] summary-widget type not found — check install order');
      return;
    }

    var patchedDefinition = Object.assign({}, existing.definition, { creatable: true });
    openmct.types.addType('summary-widget', patchedDefinition);
  };
}
