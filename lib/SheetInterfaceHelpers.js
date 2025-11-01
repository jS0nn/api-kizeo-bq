// SheetInterfaceHelpers Version 0.1.0

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
    var listValues = ['none'].concat(Object.keys(triggerOptions));
    var validationTrigger = SpreadsheetApp.newDataValidation()
      .requireValueInList(listValues, true)
      .setAllowInvalid(true)
      .setHelpText(
        "Sélectionner une fréquence standard (M1, H1, H24…) ou saisir un code personnalisé (ex. H24@06 ou WD1@MON@08)."
      )
      .build();
    triggerRange.setDataValidation(validationTrigger);
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

var SheetInterfaceHelpers = {
  applyConfigLayout: sheetApplyConfigLayout,
  notifyExecutionAlreadyRunning: sheetNotifyExecutionAlreadyRunning,
  ensureBigQueryConfigAvailability: sheetEnsureBigQueryConfigAvailability
};

this.SheetInterfaceHelpers = SheetInterfaceHelpers;

