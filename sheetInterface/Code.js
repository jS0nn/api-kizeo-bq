//Version 4.8.0

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
  return sheetExports.getOrCreateSubFolder(parentFolderId, subFolderName);
}

function buildMediaDisplayName(media) {
  return sheetExports.buildMediaDisplayName(media);
}

function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  sheetExports.exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId);
}

function exportMedias(mediaList, targetFolderId) {
  sheetExports.exportMedias(mediaList, targetFolderId);
}

function readFormConfigFromSheet(sheet) {
  return sheetConfig.readFormConfigFromSheet(sheet);
}

function writeFormConfigToSheet(sheet, config) {
  sheetConfig.writeFormConfigToSheet(sheet, config);
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
  sheetConfig.notifyConfigErrors(validation);
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
