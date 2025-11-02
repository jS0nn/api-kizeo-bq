//Version 6.1.0

function resolveSheetDriveExports() {
  if (typeof libKizeo === 'undefined' || libKizeo === null) {
    throw new Error('libKizeo indisponible (MAJ Listes Externes/Code)');
  }
  var sheetDriveExports = libKizeo.SheetDriveExports || null;
  if (!sheetDriveExports) {
    throw new Error('SheetDriveExports indisponible via libKizeo');
  }
  return sheetDriveExports;
}

function sanitizeBatchLimitValue(raw) {
  return majConfig.sanitizeBatchLimitValue(raw);
}

function getConfiguredBatchLimit(config) {
  return majConfig.getConfiguredBatchLimit(config);
}

function sanitizeBooleanConfigFlag(raw, defaultValue) {
  return majConfig.sanitizeBooleanConfigFlag(raw, defaultValue);
}

function sanitizeTriggerFrequency(raw) {
  return majTriggers.sanitizeTriggerFrequency(raw);
}

function getTriggerOption(key) {
  return majTriggers.getTriggerOption(key);
}

function describeTriggerOption(key) {
  return majTriggers.describeTriggerOption(key);
}

function configureTriggerFromKey(key) {
  return majTriggers.configureTriggerFromKey(key);
}

function parseCustomDailyHour(key) {
  return majTriggers.parseCustomDailyHour(key);
}

function formatHourLabel(hour) {
  return majTriggers.formatHourLabel(hour);
}

function parseCustomWeekly(key) {
  return majTriggers.parseCustomWeekly(key);
}

function formatWeekdayLabel(dayCode) {
  return majTriggers.formatWeekdayLabel(dayCode);
}

function getStoredTriggerFrequency() {
  return majTriggers.getStoredTriggerFrequency();
}

function setStoredTriggerFrequency(key) {
  return majTriggers.setStoredTriggerFrequency(key);
}

function persistTriggerFrequencyToSheet(frequencyKey) {
  return majTriggers.persistTriggerFrequencyToSheet(frequencyKey);
}

function onOpen() {
  return majPipeline.onOpen();
}

function setScriptProperties(etat) {
  return majPipeline.setScriptProperties(etat);
}

function getEtatExecution() {
  return majPipeline.getEtatExecution();
}

function initBigQueryConfigFromSheet() {
  return majPipeline.initBigQueryConfigFromSheet();
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
  return majConfig.readFormConfigFromSheet(sheet);
}

function writeFormConfigToSheet(sheet, config) {
  return majConfig.writeFormConfigToSheet(sheet, config);
}

function resolveFormulaireContext(spreadsheetBdD) {
  return majConfig.resolveFormulaireContext(spreadsheetBdD);
}

function createActionCode() {
  return majConfig.createActionCode();
}

function validateFormConfig(rawConfig, sheet) {
  return majConfig.validateFormConfig(rawConfig, sheet);
}

function notifyConfigErrors(validation) {
  return majConfig.notifyConfigErrors(validation);
}

function main(options) {
  return majPipeline.main(options);
}

function runBigQueryDeduplication() {
  return majPipeline.runBigQueryDeduplication();
}

function launchManualDeduplication() {
  return majPipeline.launchManualDeduplication();
}
