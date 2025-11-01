//Version 4.9.1

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

function sanitizeBatchLimitValue(raw) {
  return sheetAppResolveBindings().sanitizeBatchLimitValue(raw);
}

function getConfiguredBatchLimit(config) {
  return sheetAppResolveBindings().getConfiguredBatchLimit(config);
}

function sanitizeBooleanConfigFlag(raw, defaultValue) {
  return sheetAppResolveBindings().sanitizeBooleanConfigFlag(raw, defaultValue);
}

function sanitizeTriggerFrequency(raw) {
  return sheetAppResolveBindings().sanitizeTriggerFrequency(raw);
}

function getTriggerOption(key) {
  return sheetAppResolveBindings().getTriggerOption(key);
}

function describeTriggerOption(key) {
  return sheetAppResolveBindings().describeTriggerOption(key);
}

function configureTriggerFromKey(key) {
  return sheetAppResolveBindings().configureTriggerFromKey(key);
}

function parseCustomDailyHour(key) {
  return sheetAppResolveBindings().parseCustomDailyHour(key);
}

function formatHourLabel(hour) {
  return sheetAppResolveBindings().formatHourLabel(hour);
}

function parseCustomWeekly(key) {
  return sheetAppResolveBindings().parseCustomWeekly(key);
}

function formatWeekdayLabel(dayCode) {
  return sheetAppResolveBindings().formatWeekdayLabel(dayCode);
}

function getStoredTriggerFrequency() {
  return sheetAppResolveBindings().getStoredTriggerFrequency();
}

function setStoredTriggerFrequency(key) {
  return sheetAppResolveBindings().setStoredTriggerFrequency(key);
}

function persistTriggerFrequencyToSheet(frequencyKey) {
  return sheetAppResolveBindings().persistTriggerFrequencyToSheet(frequencyKey);
}

function onOpen() {
  return sheetAppResolveBindings().onOpen();
}

function setScriptProperties(etat) {
  return sheetAppResolveBindings().setScriptProperties(etat);
}

function getEtatExecution() {
  return sheetAppResolveBindings().getEtatExecution();
}

function initBigQueryConfigFromSheet() {
  return sheetAppResolveBindings().initBigQueryConfigFromSheet();
}

function getOrCreateSubFolder(parentFolderId, subFolderName) {
  return sheetAppResolveBindings().getOrCreateSubFolder(parentFolderId, subFolderName);
}

function buildMediaDisplayName(media) {
  return sheetAppResolveBindings().buildMediaDisplayName(media);
}

function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  return sheetAppResolveBindings().exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId);
}

function exportMedias(mediaList, targetFolderId) {
  return sheetAppResolveBindings().exportMedias(mediaList, targetFolderId);
}

function readFormConfigFromSheet(sheet) {
  return sheetAppResolveBindings().readFormConfigFromSheet(sheet);
}

function writeFormConfigToSheet(sheet, config) {
  return sheetAppResolveBindings().writeFormConfigToSheet(sheet, config);
}

function resolveFormulaireContext(spreadsheetBdD) {
  return sheetAppResolveBindings().resolveFormulaireContext(spreadsheetBdD);
}

function createActionCode() {
  return sheetAppResolveBindings().createActionCode();
}

function validateFormConfig(rawConfig, sheet) {
  return sheetAppResolveBindings().validateFormConfig(rawConfig, sheet);
}

function notifyConfigErrors(validation) {
  return sheetAppResolveBindings().notifyConfigErrors(validation);
}

function main(options) {
  return sheetAppResolveBindings().main(options);
}

function runBigQueryDeduplication() {
  return sheetAppResolveBindings().runBigQueryDeduplication();
}

function launchManualDeduplication() {
  return sheetAppResolveBindings().launchManualDeduplication();
}
