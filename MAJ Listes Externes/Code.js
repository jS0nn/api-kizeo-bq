//Version 4.7.1

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

function sanitizeBatchLimitValue(raw) {
  return majAppResolveBindings().sanitizeBatchLimitValue(raw);
}

function getConfiguredBatchLimit(config) {
  return majAppResolveBindings().getConfiguredBatchLimit(config);
}

function sanitizeBooleanConfigFlag(raw, defaultValue) {
  return majAppResolveBindings().sanitizeBooleanConfigFlag(raw, defaultValue);
}

function sanitizeTriggerFrequency(raw) {
  return majAppResolveBindings().sanitizeTriggerFrequency(raw);
}

function getTriggerOption(key) {
  return majAppResolveBindings().getTriggerOption(key);
}

function describeTriggerOption(key) {
  return majAppResolveBindings().describeTriggerOption(key);
}

function configureTriggerFromKey(key) {
  return majAppResolveBindings().configureTriggerFromKey(key);
}

function parseCustomDailyHour(key) {
  return majAppResolveBindings().parseCustomDailyHour(key);
}

function formatHourLabel(hour) {
  return majAppResolveBindings().formatHourLabel(hour);
}

function parseCustomWeekly(key) {
  return majAppResolveBindings().parseCustomWeekly(key);
}

function formatWeekdayLabel(dayCode) {
  return majAppResolveBindings().formatWeekdayLabel(dayCode);
}

function getStoredTriggerFrequency() {
  return majAppResolveBindings().getStoredTriggerFrequency();
}

function setStoredTriggerFrequency(key) {
  return majAppResolveBindings().setStoredTriggerFrequency(key);
}

function persistTriggerFrequencyToSheet(frequencyKey) {
  return majAppResolveBindings().persistTriggerFrequencyToSheet(frequencyKey);
}

function onOpen() {
  return majAppResolveBindings().onOpen();
}

function setScriptProperties(etat) {
  return majAppResolveBindings().setScriptProperties(etat);
}

function getEtatExecution() {
  return majAppResolveBindings().getEtatExecution();
}

function initBigQueryConfigFromSheet() {
  return majAppResolveBindings().initBigQueryConfigFromSheet();
}

function getOrCreateSubFolder(parentFolderId, subFolderName) {
  return majAppResolveBindings().getOrCreateSubFolder(parentFolderId, subFolderName);
}

function buildMediaDisplayName(media) {
  return majAppResolveBindings().buildMediaDisplayName(media);
}

function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  return majAppResolveBindings().exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId);
}

function exportMedias(mediaList, targetFolderId) {
  return majAppResolveBindings().exportMedias(mediaList, targetFolderId);
}

function readFormConfigFromSheet(sheet) {
  return majAppResolveBindings().readFormConfigFromSheet(sheet);
}

function writeFormConfigToSheet(sheet, config) {
  return majAppResolveBindings().writeFormConfigToSheet(sheet, config);
}

function resolveFormulaireContext(spreadsheetBdD) {
  return majAppResolveBindings().resolveFormulaireContext(spreadsheetBdD);
}

function createActionCode() {
  return majAppResolveBindings().createActionCode();
}

function validateFormConfig(rawConfig, sheet) {
  return majAppResolveBindings().validateFormConfig(rawConfig, sheet);
}

function notifyConfigErrors(validation) {
  return majAppResolveBindings().notifyConfigErrors(validation);
}

function main(options) {
  return majAppResolveBindings().main(options);
}

function runBigQueryDeduplication() {
  return majAppResolveBindings().runBigQueryDeduplication();
}

function launchManualDeduplication() {
  return majAppResolveBindings().launchManualDeduplication();
}
