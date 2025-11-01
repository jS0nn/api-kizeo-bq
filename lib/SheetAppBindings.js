// SheetAppBindings Version 0.1.0

var SHEET_APP_BINDINGS_LOG_PREFIX = 'lib:SheetAppBindings';

function sheetAppEnsureModule(moduleName, moduleInstance) {
  if (!moduleInstance || typeof moduleInstance !== 'object') {
    throw new Error(SHEET_APP_BINDINGS_LOG_PREFIX + ': module ' + moduleName + ' indisponible');
  }
  return moduleInstance;
}

function sheetAppEnsureFn(moduleName, moduleInstance, fnName) {
  var fn = moduleInstance[fnName];
  if (typeof fn !== 'function') {
    throw new Error(
      SHEET_APP_BINDINGS_LOG_PREFIX + ': fonction manquante ' + moduleName + '.' + fnName
    );
  }
  return fn;
}

function sheetAppBindingsCreate(modules) {
  var configModule = sheetAppEnsureModule('config', modules && modules.config);
  var triggersModule = sheetAppEnsureModule('triggers', modules && modules.triggers);
  var pipelineModule = sheetAppEnsureModule('pipeline', modules && modules.pipeline);
  var exportsModule = sheetAppEnsureModule('exports', modules && modules.exports);

  return {
    sanitizeBatchLimitValue: function (raw) {
      return sheetAppEnsureFn('config', configModule, 'sanitizeBatchLimitValue')(raw);
    },
    getConfiguredBatchLimit: function (config) {
      return sheetAppEnsureFn('config', configModule, 'getConfiguredBatchLimit')(config);
    },
    sanitizeBooleanConfigFlag: function (raw, defaultValue) {
      return sheetAppEnsureFn('config', configModule, 'sanitizeBooleanConfigFlag')(
        raw,
        defaultValue
      );
    },
    readFormConfigFromSheet: function (sheet) {
      return sheetAppEnsureFn('config', configModule, 'readFormConfigFromSheet')(sheet);
    },
    writeFormConfigToSheet: function (sheet, config) {
      return sheetAppEnsureFn('config', configModule, 'writeFormConfigToSheet')(sheet, config);
    },
    resolveFormulaireContext: function (spreadsheet) {
      return sheetAppEnsureFn('config', configModule, 'resolveFormulaireContext')(spreadsheet);
    },
    createActionCode: function () {
      return sheetAppEnsureFn('config', configModule, 'createActionCode')();
    },
    validateFormConfig: function (rawConfig, sheet) {
      return sheetAppEnsureFn('config', configModule, 'validateFormConfig')(rawConfig, sheet);
    },
    notifyConfigErrors: function (validation) {
      return sheetAppEnsureFn('config', configModule, 'notifyConfigErrors')(validation);
    },
    sanitizeTriggerFrequency: function (raw) {
      return sheetAppEnsureFn('triggers', triggersModule, 'sanitizeTriggerFrequency')(raw);
    },
    getTriggerOption: function (key) {
      return sheetAppEnsureFn('triggers', triggersModule, 'getTriggerOption')(key);
    },
    describeTriggerOption: function (key) {
      return sheetAppEnsureFn('triggers', triggersModule, 'describeTriggerOption')(key);
    },
    configureTriggerFromKey: function (key) {
      return sheetAppEnsureFn('triggers', triggersModule, 'configureTriggerFromKey')(key);
    },
    parseCustomDailyHour: function (key) {
      return sheetAppEnsureFn('triggers', triggersModule, 'parseCustomDailyHour')(key);
    },
    formatHourLabel: function (hour) {
      return sheetAppEnsureFn('triggers', triggersModule, 'formatHourLabel')(hour);
    },
    parseCustomWeekly: function (key) {
      return sheetAppEnsureFn('triggers', triggersModule, 'parseCustomWeekly')(key);
    },
    formatWeekdayLabel: function (dayCode) {
      return sheetAppEnsureFn('triggers', triggersModule, 'formatWeekdayLabel')(dayCode);
    },
    getStoredTriggerFrequency: function () {
      return sheetAppEnsureFn('triggers', triggersModule, 'getStoredTriggerFrequency')();
    },
    setStoredTriggerFrequency: function (key) {
      return sheetAppEnsureFn('triggers', triggersModule, 'setStoredTriggerFrequency')(key);
    },
    persistTriggerFrequencyToSheet: function (frequencyKey) {
      return sheetAppEnsureFn('triggers', triggersModule, 'persistTriggerFrequencyToSheet')(
        frequencyKey
      );
    },
    onOpen: function () {
      return sheetAppEnsureFn('pipeline', pipelineModule, 'onOpen')();
    },
    setScriptProperties: function (value) {
      return sheetAppEnsureFn('pipeline', pipelineModule, 'setScriptProperties')(value);
    },
    getEtatExecution: function () {
      return sheetAppEnsureFn('pipeline', pipelineModule, 'getEtatExecution')();
    },
    initBigQueryConfigFromSheet: function () {
      return sheetAppEnsureFn('pipeline', pipelineModule, 'initBigQueryConfigFromSheet')();
    },
    main: function (options) {
      return sheetAppEnsureFn('pipeline', pipelineModule, 'main')(options);
    },
    runBigQueryDeduplication: function () {
      return sheetAppEnsureFn('pipeline', pipelineModule, 'runBigQueryDeduplication')();
    },
    launchManualDeduplication: function () {
      return sheetAppEnsureFn('pipeline', pipelineModule, 'launchManualDeduplication')();
    },
    getOrCreateSubFolder: function (parentFolderId, subFolderName) {
      return sheetAppEnsureFn('exports', exportsModule, 'getOrCreateSubFolder')(
        parentFolderId,
        subFolderName
      );
    },
    buildMediaDisplayName: function (media) {
      return sheetAppEnsureFn('exports', exportsModule, 'buildMediaDisplayName')(media);
    },
    exportPdfBlob: function (formulaireNom, dataId, pdfBlob, targetFolderId) {
      return sheetAppEnsureFn('exports', exportsModule, 'exportPdfBlob')(
        formulaireNom,
        dataId,
        pdfBlob,
        targetFolderId
      );
    },
    exportMedias: function (mediaList, targetFolderId) {
      return sheetAppEnsureFn('exports', exportsModule, 'exportMedias')(mediaList, targetFolderId);
    }
  };
}

var SheetAppBindings = {
  create: sheetAppBindingsCreate
};
