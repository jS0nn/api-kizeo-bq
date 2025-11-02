//Version 5.0.1

var majAppBindingsInstance = null;

function majAppGetGlobal() {
  if (typeof globalThis !== 'undefined') return globalThis;
  if (typeof self !== 'undefined') return self;
  if (typeof window !== 'undefined') return window;
  return Function('return this')();
}

function majAppResolveGlobal(name) {
  var globalObject = majAppGetGlobal();
  var module = globalObject[name];
  if (!module) {
    throw new Error('MAJ Listes Externes/Code -> module ' + name + ' indisponible');
  }
  return module;
}

function majAppResolveBindings() {
  if (majAppBindingsInstance) {
    return majAppBindingsInstance;
  }

  if (typeof libKizeo === 'undefined' || libKizeo === null) {
    throw new Error('libKizeo indisponible (MAJ Listes Externes/Code)');
  }

  var bindingsFactory =
    libKizeo.SheetAppBindings && typeof libKizeo.SheetAppBindings.create === 'function'
      ? libKizeo.SheetAppBindings.create
      : null;
  if (!bindingsFactory) {
    throw new Error('SheetAppBindings indisponible via libKizeo');
  }

  majAppBindingsInstance = bindingsFactory({
    config: majAppResolveGlobal('majConfig'),
    triggers: majAppResolveGlobal('majTriggers'),
    pipeline: majAppResolveGlobal('majPipeline'),
    exports: majAppResolveGlobal('majExports')
  });

  return majAppBindingsInstance;
}

function majAppInvoke(fnName) {
  var bindings = majAppResolveBindings();
  var targetFn = bindings && bindings[fnName];
  if (typeof targetFn !== 'function') {
    throw new Error('SheetAppBindings -> fonction ' + fnName + ' indisponible');
  }
  var args = Array.prototype.slice.call(arguments, 1);
  return targetFn.apply(bindings, args);
}

function sanitizeBatchLimitValue(raw) {
  return majAppInvoke('sanitizeBatchLimitValue', raw);
}

function getConfiguredBatchLimit(config) {
  return majAppInvoke('getConfiguredBatchLimit', config);
}

function sanitizeBooleanConfigFlag(raw, defaultValue) {
  return majAppInvoke('sanitizeBooleanConfigFlag', raw, defaultValue);
}

function sanitizeTriggerFrequency(raw) {
  return majAppInvoke('sanitizeTriggerFrequency', raw);
}

function getTriggerOption(key) {
  return majAppInvoke('getTriggerOption', key);
}

function describeTriggerOption(key) {
  return majAppInvoke('describeTriggerOption', key);
}

function configureTriggerFromKey(key) {
  return majAppInvoke('configureTriggerFromKey', key);
}

function parseCustomDailyHour(key) {
  return majAppInvoke('parseCustomDailyHour', key);
}

function formatHourLabel(hour) {
  return majAppInvoke('formatHourLabel', hour);
}

function parseCustomWeekly(key) {
  return majAppInvoke('parseCustomWeekly', key);
}

function formatWeekdayLabel(dayCode) {
  return majAppInvoke('formatWeekdayLabel', dayCode);
}

function getStoredTriggerFrequency() {
  return majAppInvoke('getStoredTriggerFrequency');
}

function setStoredTriggerFrequency(key) {
  return majAppInvoke('setStoredTriggerFrequency', key);
}

function persistTriggerFrequencyToSheet(frequencyKey) {
  return majAppInvoke('persistTriggerFrequencyToSheet', frequencyKey);
}

function onOpen() {
  return majAppInvoke('onOpen');
}

function setScriptProperties(etat) {
  return majAppInvoke('setScriptProperties', etat);
}

function getEtatExecution() {
  return majAppInvoke('getEtatExecution');
}

function initBigQueryConfigFromSheet() {
  return majAppInvoke('initBigQueryConfigFromSheet');
}

function getOrCreateSubFolder(parentFolderId, subFolderName) {
  return majAppInvoke('getOrCreateSubFolder', parentFolderId, subFolderName);
}

function buildMediaDisplayName(media) {
  return majAppInvoke('buildMediaDisplayName', media);
}

function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  return majAppInvoke('exportPdfBlob', formulaireNom, dataId, pdfBlob, targetFolderId);
}

function exportMedias(mediaList, targetFolderId) {
  return majAppInvoke('exportMedias', mediaList, targetFolderId);
}

function readFormConfigFromSheet(sheet) {
  return majAppInvoke('readFormConfigFromSheet', sheet);
}

function writeFormConfigToSheet(sheet, config) {
  return majAppInvoke('writeFormConfigToSheet', sheet, config);
}

function resolveFormulaireContext(spreadsheetBdD) {
  return majAppInvoke('resolveFormulaireContext', spreadsheetBdD);
}

function createActionCode() {
  return majAppInvoke('createActionCode');
}

function validateFormConfig(rawConfig, sheet) {
  return majAppInvoke('validateFormConfig', rawConfig, sheet);
}

function notifyConfigErrors(validation) {
  return majAppInvoke('notifyConfigErrors', validation);
}

function main(options) {
  return majAppInvoke('main', options);
}

function runBigQueryDeduplication() {
  return majAppInvoke('runBigQueryDeduplication');
}

function launchManualDeduplication() {
  return majAppInvoke('launchManualDeduplication');
}
