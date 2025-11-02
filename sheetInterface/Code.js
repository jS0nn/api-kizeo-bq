//Version 5.0.1

var sheetAppBindingsInstance = null;

function sheetAppGetGlobal() {
  if (typeof globalThis !== 'undefined') return globalThis;
  if (typeof self !== 'undefined') return self;
  if (typeof window !== 'undefined') return window;
  return Function('return this')();
}

function sheetAppResolveGlobal(name) {
  var globalObject = sheetAppGetGlobal();
  var module = globalObject[name];
  if (!module) {
    throw new Error('sheetInterface/Code -> module ' + name + ' indisponible');
  }
  return module;
}

function sheetAppResolveBindings() {
  if (sheetAppBindingsInstance) {
    return sheetAppBindingsInstance;
  }

  if (typeof libKizeo === 'undefined' || libKizeo === null) {
    throw new Error('libKizeo indisponible (sheetInterface/Code)');
  }

  var bindingsFactory =
    libKizeo.SheetAppBindings && typeof libKizeo.SheetAppBindings.create === 'function'
      ? libKizeo.SheetAppBindings.create
      : null;
  if (!bindingsFactory) {
    throw new Error('SheetAppBindings indisponible via libKizeo');
  }

  sheetAppBindingsInstance = bindingsFactory({
    config: sheetAppResolveGlobal('sheetConfig'),
    triggers: sheetAppResolveGlobal('sheetTriggers'),
    pipeline: sheetAppResolveGlobal('sheetPipeline'),
    exports: sheetAppResolveGlobal('sheetExports')
  });

  return sheetAppBindingsInstance;
}

function sheetAppInvoke(fnName) {
  var bindings = sheetAppResolveBindings();
  var targetFn = bindings && bindings[fnName];
  if (typeof targetFn !== 'function') {
    throw new Error('SheetAppBindings -> fonction ' + fnName + ' indisponible');
  }
  var args = Array.prototype.slice.call(arguments, 1);
  return targetFn.apply(bindings, args);
}

function sanitizeBatchLimitValue(raw) {
  return sheetAppInvoke('sanitizeBatchLimitValue', raw);
}

function getConfiguredBatchLimit(config) {
  return sheetAppInvoke('getConfiguredBatchLimit', config);
}

function sanitizeBooleanConfigFlag(raw, defaultValue) {
  return sheetAppInvoke('sanitizeBooleanConfigFlag', raw, defaultValue);
}

function sanitizeTriggerFrequency(raw) {
  return sheetAppInvoke('sanitizeTriggerFrequency', raw);
}

function getTriggerOption(key) {
  return sheetAppInvoke('getTriggerOption', key);
}

function describeTriggerOption(key) {
  return sheetAppInvoke('describeTriggerOption', key);
}

function configureTriggerFromKey(key) {
  return sheetAppInvoke('configureTriggerFromKey', key);
}

function parseCustomDailyHour(key) {
  return sheetAppInvoke('parseCustomDailyHour', key);
}

function formatHourLabel(hour) {
  return sheetAppInvoke('formatHourLabel', hour);
}

function parseCustomWeekly(key) {
  return sheetAppInvoke('parseCustomWeekly', key);
}

function formatWeekdayLabel(dayCode) {
  return sheetAppInvoke('formatWeekdayLabel', dayCode);
}

function getStoredTriggerFrequency() {
  return sheetAppInvoke('getStoredTriggerFrequency');
}

function setStoredTriggerFrequency(key) {
  return sheetAppInvoke('setStoredTriggerFrequency', key);
}

function persistTriggerFrequencyToSheet(frequencyKey) {
  return sheetAppInvoke('persistTriggerFrequencyToSheet', frequencyKey);
}

function onOpen() {
  return sheetAppInvoke('onOpen');
}

function setScriptProperties(etat) {
  return sheetAppInvoke('setScriptProperties', etat);
}

function getEtatExecution() {
  return sheetAppInvoke('getEtatExecution');
}

function initBigQueryConfigFromSheet() {
  return sheetAppInvoke('initBigQueryConfigFromSheet');
}

function getOrCreateSubFolder(parentFolderId, subFolderName) {
  return sheetAppInvoke('getOrCreateSubFolder', parentFolderId, subFolderName);
}

function buildMediaDisplayName(media) {
  return sheetAppInvoke('buildMediaDisplayName', media);
}

function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  return sheetAppInvoke('exportPdfBlob', formulaireNom, dataId, pdfBlob, targetFolderId);
}

function exportMedias(mediaList, targetFolderId) {
  return sheetAppInvoke('exportMedias', mediaList, targetFolderId);
}

function readFormConfigFromSheet(sheet) {
  return sheetAppInvoke('readFormConfigFromSheet', sheet);
}

function writeFormConfigToSheet(sheet, config) {
  return sheetAppInvoke('writeFormConfigToSheet', sheet, config);
}

function resolveFormulaireContext(spreadsheetBdD) {
  return sheetAppInvoke('resolveFormulaireContext', spreadsheetBdD);
}

function createActionCode() {
  return sheetAppInvoke('createActionCode');
}

function validateFormConfig(rawConfig, sheet) {
  return sheetAppInvoke('validateFormConfig', rawConfig, sheet);
}

function notifyConfigErrors(validation) {
  return sheetAppInvoke('notifyConfigErrors', validation);
}

function main(options) {
  return sheetAppInvoke('main', options);
}

function runBigQueryDeduplication() {
  return sheetAppInvoke('runBigQueryDeduplication');
}

function launchManualDeduplication() {
  return sheetAppInvoke('launchManualDeduplication');
}
