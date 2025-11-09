// SheetInterfaceHelpers Version 0.2.0

var SHEET_HELPERS_LOG_PREFIX = 'lib:SheetInterfaceHelpers';

function sheetApplyConfigLayout(sheet, rowCount, options) {
  if (!sheet) return;

  var opts = options || {};
  var headerLabels =
    Array.isArray(opts.headerLabels) && opts.headerLabels.length === 2
      ? opts.headerLabels
      : ['Paramètre', 'Valeur'];
  var rowIndexMap = opts.rowIndexMap || {};
  var triggerOptions = opts.triggerOptions || {};
  var batchLimitKey = opts.batchLimitKey || 'batch_limit';
  var ingestFlagKey = opts.ingestFlagKey || 'ingest_bigquery';

  var headerRange = sheet.getRange(1, 1, 1, 2);
  var headerValues = headerRange.getValues();
  if (
    !headerValues ||
    headerValues.length === 0 ||
    headerValues[0][0] !== headerLabels[0] ||
    headerValues[0][1] !== headerLabels[1]
  ) {
    headerRange.setValues([headerLabels]);
  }

  sheet.setFrozenRows(1);
  sheet.setFrozenColumns(1);
  sheet.setColumnWidth(1, 220);
  sheet.setColumnWidth(2, 360);

  var totalRows = Math.max(rowCount + 1, 2);
  var tableRange = sheet.getRange(1, 1, totalRows, 2);

  var bandings = sheet.getBandings();
  for (var index = 0; index < bandings.length; index++) {
    var banding = bandings[index];
    if (banding.getRange().getSheet().getSheetId() === sheet.getSheetId()) {
      banding.remove();
    }
  }

  var newBanding = tableRange.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);
  newBanding.setHeaderRowColor('#1a73e8');
  newBanding.setFirstRowColor('#ffffff');
  newBanding.setSecondRowColor('#f5f5f5');

  headerRange
    .setFontWeight('bold')
    .setFontColor('#ffffff')
    .setHorizontalAlignment('left')
    .setVerticalAlignment('middle')
    .setWrap(false);

  if (rowCount > 0) {
    var keysRange = sheet.getRange(2, 1, rowCount, 1);
    keysRange.setFontWeight('bold').setFontColor('#174ea6').setWrap(false);

    var valuesRange = sheet.getRange(2, 2, rowCount, 1);
    valuesRange.setWrap(true).setHorizontalAlignment('left').setVerticalAlignment('middle');
  }

  tableRange.setBorder(true, true, true, true, true, true, '#dadce0', SpreadsheetApp.BorderStyle.SOLID);

  var existingFilter = sheet.getFilter();
  if (existingFilter && existingFilter.getRange().getSheet().getSheetId() === sheet.getSheetId()) {
    existingFilter.remove();
  }

  tableRange.createFilter();

  if (rowIndexMap && Object.prototype.hasOwnProperty.call(rowIndexMap, batchLimitKey)) {
    var batchLimitRow = rowIndexMap[batchLimitKey] + 2;
    var batchLimitRange = sheet.getRange(batchLimitRow, 2);
    var validationBatch = SpreadsheetApp.newDataValidation()
      .requireNumberGreaterThan(0)
      .setAllowInvalid(false)
      .setHelpText('Saisir un entier positif (1 à 5000) pour limiter le volume ingéré par lot Kizeo.')
      .build();
    batchLimitRange.setDataValidation(validationBatch);
    batchLimitRange.setNumberFormat('0');
  }

  if (rowIndexMap && Object.prototype.hasOwnProperty.call(rowIndexMap, ingestFlagKey)) {
    var ingestRow = rowIndexMap[ingestFlagKey] + 2;
    var ingestRange = sheet.getRange(ingestRow, 2);
    var validationIngest = SpreadsheetApp.newDataValidation()
      .requireValueInList(['true', 'false'], true)
      .setAllowInvalid(false)
      .setHelpText("true = ingestion BigQuery active. false = suspension temporaire de l'écriture.")
      .build();
    ingestRange.setDataValidation(validationIngest);
    ingestRange.setHorizontalAlignment('center');
  }

  if (rowIndexMap && Object.prototype.hasOwnProperty.call(rowIndexMap, 'trigger_frequency')) {
    var triggerRow = rowIndexMap.trigger_frequency + 2;
    var triggerRange = sheet.getRange(triggerRow, 2);
    var optionKeys = Object.keys(triggerOptions).map(function (key) {
      return String(key || '').trim();
    });
    var validPatternParts = ['none'];
    if (optionKeys.length) {
      validPatternParts = validPatternParts.concat(optionKeys);
    }
    var uppercaseKeys = validPatternParts.map(function (key) {
      return String(key || '').trim().toUpperCase();
    });
    var regexPattern =
      '^(?:NONE|' +
      uppercaseKeys
        .filter(function (value, index, self) {
          return value && self.indexOf(value) === index;
        })
        .join('|') +
      '|H24@(0[0-9]|1[0-9]|2[0-3])|WD1@[A-Z]{3}@(0[0-9]|1[0-9]|2[0-3]))$';
    var formula =
      '=OR(B' +
      triggerRow +
      '="", REGEXMATCH(UPPER(B' +
      triggerRow +
      '), "' +
      regexPattern +
      '"))';
    var uniqueKeys = uppercaseKeys.filter(function (value, index, self) {
      return value && self.indexOf(value) === index;
    });
    var regexAlternatives = uniqueKeys.slice();
    regexAlternatives.push('H24@(0[0-9]|1[0-9]|2[0-3])');
    regexAlternatives.push('WD1@[A-Z]{3}@(0[0-9]|1[0-9]|2[0-3])');
    var triggerPattern = '^(?:' + regexAlternatives.join('|') + ')?$';
    var validationBuilder = SpreadsheetApp.newDataValidation();
    if (typeof validationBuilder.requireTextMatchesPattern === 'function') {
      validationBuilder = validationBuilder.requireTextMatchesPattern(triggerPattern).setAllowInvalid(false);
      validationBuilder = validationBuilder.setHelpText(
        "Utiliser une valeur standard (M1, H1, H24…) ou un code personnalisé H24@HH / WD1@JOUR@HH (ex.: WD1@SUN@02)."
      );
      triggerRange.setDataValidation(validationBuilder.build());
    } else {
      try {
        if (typeof triggerRange.clearDataValidations === 'function') {
          triggerRange.clearDataValidations();
        } else if (typeof triggerRange.clearDataValidation === 'function') {
          triggerRange.clearDataValidation();
        }
      } catch (clearError) {
        console.log(SHEET_HELPERS_LOG_PREFIX + ': clearDataValidation impossible -> ' + clearError);
      }
      triggerRange.setNote(
        "Validation avancée indisponible dans cette version d'Apps Script. Utiliser un code standard (M1, H1…) ou un format H24@HH / WD1@JOUR@HH."
      );
    }
    triggerRange.setNote(
      'Utiliser le menu "Configuration Kizeo" pour régler le déclencheur ou saisir un code H24@HH / WD1@JOUR@HH.'
    );
  }
}

function sheetNotifyExecutionAlreadyRunning(options) {
  var opts = options || {};
  var toastMessage =
    opts.toastMessage ||
    "Une mise à jour est déjà en cours. Patientez avant de relancer ou exécutez setScriptProperties('termine').";
  var alertMessage =
    opts.alertMessage ||
    "Une mise à jour est déjà en cours. Patientez avant de relancer.\nEn cas de blocage, réinitialisez l'état manuellement ou exécutez setScriptProperties('termine').";
  var toastDuration = Number.isFinite(opts.toastSeconds) ? Math.max(1, Number(opts.toastSeconds)) : 10;

  if (opts.showToast !== false) {
    try {
      var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
      if (spreadsheet) {
        spreadsheet.toast(toastMessage, 'Mise à jour en cours', toastDuration);
      }
    } catch (toastError) {
      console.log(SHEET_HELPERS_LOG_PREFIX + ': toast KO -> ' + toastError);
    }
  }

  if (opts.showAlert !== false) {
    try {
      var ui = SpreadsheetApp.getUi();
      if (ui) {
        ui.alert('Mise à jour en cours', alertMessage, ui.ButtonSet.OK);
      }
    } catch (uiError) {
      console.log(SHEET_HELPERS_LOG_PREFIX + ': alerte UI indisponible -> ' + uiError);
    }
  }

  if (opts.shouldThrow) {
    throw new Error(opts.errorMessage || 'EXECUTION_EN_COURS');
  }
}

function sheetEnsureBigQueryConfigAvailability(ingestFlag, sheetName) {
  if (ingestFlag === 'false') {
    return true;
  }
  if (typeof getBigQueryConfig !== 'function') {
    return true;
  }
  try {
    getBigQueryConfig({ throwOnMissing: true });
    return true;
  } catch (configError) {
    var missingKeys =
      Array.isArray(configError.missingKeys) && configError.missingKeys.length
        ? configError.missingKeys.join(', ')
        : 'BQ_PROJECT_ID / BQ_DATASET';
    var message =
      'Configuration BigQuery incomplète (' +
      missingKeys +
      ').\nUtilisez le menu « Configurer BigQuery » avant de relancer le traitement.';

    try {
      var ui = SpreadsheetApp.getUi();
      if (ui) {
        ui.alert('Configuration BigQuery manquante', message, ui.ButtonSet.OK);
      }
    } catch (uiError) {
      try {
        var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
        if (spreadsheet) {
          spreadsheet.toast(message, 'Configuration BigQuery', 10);
        }
      } catch (toastError) {
        console.log(SHEET_HELPERS_LOG_PREFIX + ': notification impossible -> ' + toastError);
      }
    }

    if (typeof handleException === 'function') {
      try {
        handleException('sheetEnsureBigQueryConfigAvailability', configError, {
          missingKeys: missingKeys,
          sheet: sheetName || ''
        });
      } catch (handlerError) {
        console.log(SHEET_HELPERS_LOG_PREFIX + ': handleException KO -> ' + handlerError);
      }
    }
    return false;
  }
}

function sheetDescribeBigQueryTableStatus(options) {
  var opts = options || {};
  var tableName = opts.tableName ? String(opts.tableName).trim() : '';
  var result = {
    tableName: tableName,
    formId: opts.formId ? String(opts.formId).trim() : '',
    formName: opts.formName ? String(opts.formName).trim() : '',
    projectId: '',
    datasetId: '',
    fullTableId: '',
    exists: false,
    checkSkipped: false,
    skipReason: '',
    error: ''
  };

  if (!tableName) {
    result.checkSkipped = true;
    result.skipReason = 'MISSING_TABLE_NAME';
    return result;
  }

  if (typeof getBigQueryConfig !== 'function') {
    result.checkSkipped = true;
    result.skipReason = 'MISSING_GET_BIGQUERY_CONFIG';
    return result;
  }

  var config;
  try {
    config = getBigQueryConfig({ throwOnMissing: false });
  } catch (configError) {
    result.error = configError && configError.message ? configError.message : 'CONFIG_ERROR';
    return result;
  }

  if (!config || !config.projectId || !config.datasetId) {
    result.checkSkipped = true;
    result.skipReason = 'MISSING_CONFIG';
    return result;
  }

  result.projectId = config.projectId;
  result.datasetId = config.datasetId;
  result.fullTableId = result.projectId + '.' + result.datasetId + '.' + result.tableName;

  if (typeof bqTableExists !== 'function') {
    result.checkSkipped = true;
    result.skipReason = 'MISSING_TABLE_LOOKUP';
    return result;
  }

  try {
    result.exists = bqTableExists(config, tableName);
  } catch (lookupError) {
    result.error = lookupError && lookupError.message ? lookupError.message : 'BQ_LOOKUP_ERROR';
  }

  return result;
}

function sheetNotifyExecutionCompleted(options) {
  var opts = options || {};
  var rowCount = typeof opts.rowCount === 'number' ? opts.rowCount : 0;
  var runSeconds =
    typeof opts.runDurationSeconds === 'number' && !Number.isNaN(opts.runDurationSeconds)
      ? Math.max(0, Math.round(opts.runDurationSeconds))
      : null;
  var latestRecord = opts.latestRecord && (opts.latestRecord.data_id || opts.latestRecord.id) ? opts.latestRecord : null;

  var toastMessage =
    'Mise à jour terminée' +
    (rowCount ? ' • ' + rowCount + ' enregistrement(s) traité(s)' : '') +
    (latestRecord ? ' • dernier ID : ' + (latestRecord.data_id || latestRecord.id) : '') +
    (runSeconds !== null ? ' • durée : ' + runSeconds + ' s' : '');

  try {
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    if (spreadsheet) {
      spreadsheet.toast(toastMessage, 'Exécution réussie', 10);
    }
  } catch (toastError) {
    console.log(SHEET_HELPERS_LOG_PREFIX + ': toast succès impossible -> ' + toastError);
  }

  if (opts.showAlert) {
    try {
      var ui = SpreadsheetApp.getUi();
      if (ui) {
        ui.alert('Exécution terminée', toastMessage, ui.ButtonSet.OK);
      }
    } catch (uiError) {
      console.log(SHEET_HELPERS_LOG_PREFIX + ': alerte succès impossible -> ' + uiError);
    }
  }
}

function sheetNotifyExecutionFailed(error, options) {
  var message =
    error && error.message ? error.message : 'Une erreur est survenue pendant la mise à jour. Consultez les journaux.';
  var opts = options || {};

  try {
    var ui = SpreadsheetApp.getUi();
    if (ui) {
      ui.alert('Erreur de mise à jour', message, ui.ButtonSet.OK);
      return;
    }
  } catch (uiError) {
    console.log(SHEET_HELPERS_LOG_PREFIX + ': alerte échec impossible -> ' + uiError);
  }

  try {
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    if (spreadsheet) {
      spreadsheet.toast(message, 'Erreur', 10);
    }
  } catch (toastError) {
    console.log(SHEET_HELPERS_LOG_PREFIX + ': toast échec impossible -> ' + toastError);
  }
}

function normalizeTimestamp(value) {
  if (!value) return '';
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return '';
    return value.toISOString();
  }
  var date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  return date.toISOString();
}

function sheetPersistExecutionMetadata(options) {
  if (!options || typeof options !== 'object') return;
  var sheet = options.sheet;
  var configService = options.configService;
  var summary = options.summary || {};
  var formulaire = options.formulaire || {};
  var runDurationSeconds = options.runDurationSeconds;

  if (
    !sheet ||
    !configService ||
    typeof configService.readConfigFromSheet !== 'function' ||
    typeof configService.writeConfigToSheet !== 'function'
  ) {
    console.log(SHEET_HELPERS_LOG_PREFIX + ': persistExecutionMetadata -> prérequis manquants.');
    return;
  }

  var existingConfig = configService.readConfigFromSheet(sheet) || {};
  var updatedConfig = Object.assign({}, existingConfig);

  var latestRecord = summary.latestRecord || null;
  if (latestRecord) {
    var latestId = latestRecord.data_id || latestRecord.id || latestRecord._id || '';
    if (latestId) {
      updatedConfig.last_data_id = String(latestId);
    }
    var latestUpdate =
      latestRecord.update_time ||
      latestRecord._update_time ||
      latestRecord.last_update_time ||
      latestRecord.updateTime ||
      '';
    var latestAnswer =
      latestRecord.answer_time ||
      latestRecord._answer_time ||
      latestRecord.last_answer_time ||
      latestRecord.answerTime ||
      '';
    var updateIso = normalizeTimestamp(latestUpdate);
    if (updateIso) {
      updatedConfig.last_update_time = updateIso;
    }
    var answerIso = normalizeTimestamp(latestAnswer);
    if (answerIso) {
      updatedConfig.last_answer_time = answerIso;
    }
  }

  var runTimestamp = summary.runTimestamp || new Date().toISOString();
  updatedConfig.last_run_at = normalizeTimestamp(runTimestamp) || new Date().toISOString();

  if (typeof summary.rowCount === 'number' && !Number.isNaN(summary.rowCount)) {
    updatedConfig.last_saved_row_count = summary.rowCount;
  }

  if (typeof runDurationSeconds === 'number' && !Number.isNaN(runDurationSeconds)) {
    updatedConfig.last_run_duration_s = Math.max(0, Math.round(runDurationSeconds));
  }

  if (formulaire && formulaire.action) {
    updatedConfig.action = formulaire.action;
  }

  try {
    configService.writeConfigToSheet(sheet, updatedConfig);
  } catch (writeError) {
    console.log(SHEET_HELPERS_LOG_PREFIX + ': persistExecutionMetadata write KO -> ' + writeError);
    throw writeError;
  }
}

var SheetInterfaceHelpers = {
  applyConfigLayout: sheetApplyConfigLayout,
  notifyExecutionAlreadyRunning: sheetNotifyExecutionAlreadyRunning,
  ensureBigQueryConfigAvailability: sheetEnsureBigQueryConfigAvailability,
  describeBigQueryTableStatus: sheetDescribeBigQueryTableStatus,
  notifyExecutionCompleted: sheetNotifyExecutionCompleted,
  notifyExecutionFailed: sheetNotifyExecutionFailed,
  persistExecutionMetadata: sheetPersistExecutionMetadata
};
