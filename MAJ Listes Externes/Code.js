//Version 4.6.0

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
  return majExports.getOrCreateSubFolder(parentFolderId, subFolderName);
}

function buildMediaDisplayName(media) {
  return majExports.buildMediaDisplayName(media);
}

function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  majExports.exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId);
}

function exportMedias(mediaList, targetFolderId) {
  majExports.exportMedias(mediaList, targetFolderId);
}

function readFormConfigFromSheet(sheet) {
  return majConfig.readFormConfigFromSheet(sheet);
}

function writeFormConfigToSheet(sheet, config) {
  majConfig.writeFormConfigToSheet(sheet, config);
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
  majConfig.notifyConfigErrors(validation);
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
