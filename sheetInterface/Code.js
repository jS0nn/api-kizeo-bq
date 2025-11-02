//Version 6.1.0

function resolveSheetDriveExports() {
  if (typeof libKizeo === 'undefined' || libKizeo === null) {
    throw new Error('libKizeo indisponible (sheetInterface/Code)');
  }
  var sheetDriveExports = libKizeo.SheetDriveExports || null;
  if (!sheetDriveExports) {
    throw new Error('SheetDriveExports indisponible via libKizeo');
  }
  return sheetDriveExports;
}

function sanitizeBatchLimitValue(raw) {
  return sheetConfig.sanitizeBatchLimitValue(raw);
}

function getConfiguredBatchLimit(config) {
  return sheetConfig.getConfiguredBatchLimit(config);
}

function sanitizeBooleanConfigFlag(raw, defaultValue) {
  return sheetConfig.sanitizeBooleanConfigFlag(raw, defaultValue);
}

function sanitizeTriggerFrequency(raw) {
  return sheetTriggers.sanitizeTriggerFrequency(raw);
}

function getTriggerOption(key) {
  return sheetTriggers.getTriggerOption(key);
}

function describeTriggerOption(key) {
  return sheetTriggers.describeTriggerOption(key);
}

function configureTriggerFromKey(key) {
  return sheetTriggers.configureTriggerFromKey(key);
}

function parseCustomDailyHour(key) {
  return sheetTriggers.parseCustomDailyHour(key);
}

function formatHourLabel(hour) {
  return sheetTriggers.formatHourLabel(hour);
}

function parseCustomWeekly(key) {
  return sheetTriggers.parseCustomWeekly(key);
}

function formatWeekdayLabel(dayCode) {
  return sheetTriggers.formatWeekdayLabel(dayCode);
}

function getStoredTriggerFrequency() {
  return sheetTriggers.getStoredTriggerFrequency();
}

function setStoredTriggerFrequency(key) {
  return sheetTriggers.setStoredTriggerFrequency(key);
}

function persistTriggerFrequencyToSheet(frequencyKey) {
  return sheetTriggers.persistTriggerFrequencyToSheet(frequencyKey);
}

function onOpen() {
  return sheetPipeline.onOpen();
}

function setScriptProperties(etat) {
  return sheetPipeline.setScriptProperties(etat);
}

function getEtatExecution() {
  return sheetPipeline.getEtatExecution();
}

function initBigQueryConfigFromSheet() {
  return sheetPipeline.initBigQueryConfigFromSheet();
}

function getOrCreateSubFolder(parentFolderId, subFolderName) {
  return resolveSheetDriveExports().getOrCreateSubFolder(parentFolderId, subFolderName);
}

function buildMediaDisplayName(media) {
  return resolveSheetDriveExports().buildMediaDisplayName(media);
}

function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  resolveSheetDriveExports().exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId);
}

function exportMedias(mediaList, targetFolderId) {
  resolveSheetDriveExports().exportMedias(mediaList, targetFolderId);
}

function readFormConfigFromSheet(sheet) {
  return sheetConfig.readFormConfigFromSheet(sheet);
}

function writeFormConfigToSheet(sheet, config) {
  return sheetConfig.writeFormConfigToSheet(sheet, config);
}

function resolveFormulaireContext(spreadsheetBdD) {
  return sheetConfig.resolveFormulaireContext(spreadsheetBdD);
}

function createActionCode() {
  return sheetConfig.createActionCode();
}

function validateFormConfig(rawConfig, sheet) {
  return sheetConfig.validateFormConfig(rawConfig, sheet);
}

function notifyConfigErrors(validation) {
  return sheetConfig.notifyConfigErrors(validation);
}

function main(options) {
  return sheetPipeline.main(options);
}

function runBigQueryDeduplication() {
  return sheetPipeline.runBigQueryDeduplication();
}

function launchManualDeduplication() {
  return sheetPipeline.launchManualDeduplication();
}
