
/**
 * Harness manuel pour vérifier l’intégration sans passer par une façade locale.
 * Renseigner les identifiants nécessaires ci-dessous. Sans configuration explicite,
 * le dossier parent du script (ou du classeur actif pour un script lié) est utilisé.
 * Si aucun dossier n’est fourni, le parent du classeur actif sera utilisé.
 */
var MANUAL_TEST_CONFIG = Object.freeze({
  driveFolderId: ''
});

var DEFAULT_DRIVE_EXPORT_FOLDER_ID = (function () {
  try {
    var scriptFile = DriveApp.getFileById(ScriptApp.getScriptId());
    var parents = scriptFile.getParents();
    if (parents.hasNext()) {
      return parents.next().getId();
    }
  } catch (scriptError) {
    Logger.log('DEFAULT_DRIVE_EXPORT_FOLDER_ID (script) -> ' + scriptError);
  }
  try {
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    if (spreadsheet) {
      var file = DriveApp.getFileById(spreadsheet.getId());
      var sheetParents = file.getParents();
      if (sheetParents.hasNext()) {
        return sheetParents.next().getId();
      }
      return file.getId();
    }
  } catch (sheetError) {
    Logger.log('DEFAULT_DRIVE_EXPORT_FOLDER_ID (sheet) -> ' + sheetError);
  }
  return '';
})();

function zzDescribeScenarioDriveExport() {
  if (typeof libKizeo === 'undefined' || libKizeo === null) {
    throw new Error('libKizeo requis pour zzDescribeScenarioDriveExport.');
  }

  var spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  if (!spreadsheetBdD) {
    throw new Error('Aucun classeur actif pour déterminer le formulaire.');
  }
  var activeSheet = spreadsheetBdD.getActiveSheet();
  if (!activeSheet) {
    throw new Error('Aucun onglet actif pour déterminer le formulaire.');
  }
  var formulaire = resolveFormulaireDepuisSheet(activeSheet);
  if (!formulaire || !formulaire.id) {
    throw new Error(
      "Impossible de déterminer le formulaire. Vérifiez la configuration (nom d'onglet « Nom || ID » ou feuille Config renseignée)."
    );
  }

  var exportsResponse = libKizeo.requeteAPIDonnees('GET', '/forms/' + formulaire.id + '/exports');
  var exportsList = exportsResponse && exportsResponse.data && exportsResponse.data.exports;
  if (!exportsList || !exportsList.length) {
    return { status: 'NO_EXPORT', message: 'Aucun export disponible pour ce formulaire.' };
  }

  var dataResponse = libKizeo.requeteAPIDonnees('GET', '/forms/' + formulaire.id + '/data/all');
  var dataList = dataResponse && dataResponse.data && dataResponse.data.data;
  if (!dataList || !dataList.length) {
    return { status: 'NO_DATA', message: 'Aucune donnée disponible pour ce formulaire.' };
  }

  var firstData = dataList[0];
  var pdfResponse = libKizeo.requeteAPIDonnees('GET', '/forms/' + formulaire.id + '/data/' + firstData.id + '/pdf');
  if (!pdfResponse || !pdfResponse.data) {
    return { status: 'PDF_MISSING', message: 'PDF introuvable pour la réponse testée.' };
  }

  var driveService = libKizeo.DriveMediaService.getDefault();
  if (!driveService || typeof driveService.saveBlobToFolder !== 'function') {
    throw new Error('DriveMediaService indisponible pour zzDescribeScenarioDriveExport.');
  }

  var targetFolderId =
    (MANUAL_TEST_CONFIG && MANUAL_TEST_CONFIG.driveFolderId) || DEFAULT_DRIVE_EXPORT_FOLDER_ID;
  if (!targetFolderId) {
    throw new Error(
      "Impossible de déterminer un dossier Drive cible. Fournissez MANUAL_TEST_CONFIG.driveFolderId ou placez le classeur/la bibliothèque dans un dossier Drive."
    );
  }

  var savedFileId = driveService.saveBlobToFolder(
    pdfResponse.data,
    targetFolderId,
    formulaire.nom + '_' + firstData.id + '_manual.pdf'
  );

  return {
    status: 'DONE',
    exportCount: exportsList.length,
    savedFileId: savedFileId || null
  };
}

function resolveFormulaireDepuisSheet(sheet) {
  if (!sheet) {
    return null;
  }
  var nameParts = sheet.getName().split(' || ');
  if (nameParts.length >= 2) {
    return { nom: nameParts[0], id: nameParts[1] };
  }
  try {
    if (typeof sheetConfig !== 'undefined' && sheetConfig && sheetConfig.readFormConfigFromSheet) {
      var existingConfig = sheetConfig.readFormConfigFromSheet(sheet) || {};
      if (existingConfig.form_id && existingConfig.form_name) {
        return { nom: existingConfig.form_name, id: existingConfig.form_id };
      }
    }
  } catch (configError) {
    Logger.log('resolveFormulaireDepuisSheet -> ' + configError);
  }
  return null;
}

function zzDescribeScenarioSheetInterface() {
  if (typeof libKizeo === 'undefined') {
    throw new Error('zzDescribeScenarioSheetInterface requiert la librairie libKizeo.');
  }
  const library = libKizeo;
  if (typeof library.bqComputeTableName !== 'function') {
    throw new Error('libKizeo.bqComputeTableName est requis pour zzDescribeScenarioSheetInterface.');
  }

  const fakeSheet = {
    getName: () => 'Config',
    getLastRow: () => 2,
    getRange: () => ({
      setValues: () => {},
      clearContent: () => {},
      getValues: () => [['Paramètre', 'Valeur']]
    })
  };

  const config = {
    form_id: 'FORM_SCENARIO',
    form_name: 'Formulaire Scénario',
    action: 'ACTION_SCENARIO',
    bq_table_name: 'form_scenario',
    [CONFIG_BATCH_LIMIT_KEY]: '15',
    [CONFIG_INGEST_BIGQUERY_KEY]: 'true'
  };

  const validation = validateFormConfig(config, fakeSheet);

  if (
    typeof library.SheetInterfaceHelpers === 'undefined' ||
    typeof library.SheetInterfaceHelpers.ensureBigQueryConfigAvailability !== 'function'
  ) {
    throw new Error('libKizeo.SheetInterfaceHelpers.ensureBigQueryConfigAvailability est requis pour zzDescribeScenarioSheetInterface.');
  }

  const availability = library.SheetInterfaceHelpers.ensureBigQueryConfigAvailability(
    validation.config ? validation.config[CONFIG_INGEST_BIGQUERY_KEY] : 'false',
    fakeSheet.getName()
  );

  const summary = {
    isValid: validation.isValid,
    tableName: validation.config ? validation.config.bq_table_name : null,
    batchLimit: validation.config ? validation.config[CONFIG_BATCH_LIMIT_KEY] : null,
    availability
  };

  Logger.log(`zzDescribeScenarioSheetInterface -> ${JSON.stringify(summary)}`);

  return summary;
}
  
