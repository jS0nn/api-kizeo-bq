
/**
 * Harness manuel pour vérifier l’intégration MAJ listes externes.
 * Complétez MANUAL_TEST_CONFIG au besoin. Si aucun dossier n’est fourni, le parent du script
 * (ou du classeur actif) sera utilisé automatiquement.
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
    if (typeof majConfig !== 'undefined' && majConfig && majConfig.readFormConfigFromSheet) {
      var existingConfig = majConfig.readFormConfigFromSheet(sheet) || {};
      if (existingConfig.form_id && existingConfig.form_name) {
        return { nom: existingConfig.form_name, id: existingConfig.form_id };
      }
    }
  } catch (configError) {
    Logger.log('resolveFormulaireDepuisSheet -> ' + configError);
  }
  return null;
}

function zzDescribeScenarioMajListesExternes() {
  const formulaire = { id: 'FORM_SCENARIO', nom: 'Formulaire Scénario' };
  const snapshot = {
    existingHeaders: ['id', 'champ'],
    rowEnCours: ['rec-001', 'Valeur mise à jour']
  };

  const putCalls = [];
  if (typeof libKizeo === 'undefined' || !libKizeo.ExternalListsService) {
    throw new Error('libKizeo.ExternalListsService indisponible');
  }
  if (typeof libKizeo.ExternalListsService.updateFromSnapshot !== 'function') {
    throw new Error('libKizeo.ExternalListsService.updateFromSnapshot indisponible');
  }

  const result = libKizeo.ExternalListsService.updateFromSnapshot(formulaire, snapshot, {
    fetch: function (method, path, payload) {
      if (method === 'GET' && path === '/lists') {
        return {
          data: {
            lists: [
              {
                id: 'LISTE_1',
                name: `${formulaire.nom} || ${formulaire.id}`
              }
            ]
          }
        };
      }
      if (method === 'GET' && path === '/lists/LISTE_1') {
        return {
          data: {
            list: {
              items: ['id:id|champ:champ', 'rec-000:rec-000|champ:champ']
            }
          }
        };
      }
      if (method === 'PUT' && path === '/lists/LISTE_1') {
        putCalls.push(payload);
        return { data: { status: 'ok' }, responseCode: 200 };
      }
      return { data: { status: 'IGNORED' }, responseCode: 200 };
    },
    log: function (message) {
      Logger.log(`ExternalListsService[scenario]: ${message}`);
    },
    handleException: function (name, error, context) {
      Logger.log(`ExternalListsService[scenario] ${name}: ${error}`);
      if (context) {
        try {
          Logger.log(`Context: ${JSON.stringify(context)}`);
        } catch (jsonError) {
          Logger.log(`Context stringify KO: ${jsonError}`);
        }
      }
    }
  });

  const summary = {
    status: result,
    putCalls: putCalls.length,
    samplePayload: putCalls.length ? putCalls[0] : null
  };
  Logger.log(`zzDescribeScenarioMajListesExternes -> ${JSON.stringify(summary)}`);
  return summary;
}

function zzDescribeScenarioSyncExternalLists() {
  if (typeof validateFormConfig !== 'function') {
    throw new Error('validateFormConfig requis pour zzDescribeScenarioSyncExternalLists.');
  }

  const fakeSheet = {
    getName: () => 'Config (scénario)',
    getLastRow: () => 2,
    getRange: () => ({
      setValues: () => {},
      clearContent: () => {},
      getValues: () => [['Paramètre', 'Valeur']]
    })
  };

  const validation = validateFormConfig(
    {
      form_id: 'FORM_SYNC',
      form_name: 'Formulaire Sync',
      action: 'ACTION_SYNC',
      bq_table_name: 'form_sync',
      [CONFIG_BATCH_LIMIT_KEY]: '10',
      [CONFIG_INGEST_BIGQUERY_KEY]: 'true'
    },
    fakeSheet
  );

  const external = zzDescribeScenarioMajListesExternes();
  const summary = {
    configValid: validation.isValid,
    validationErrors: validation.errors,
    external
  };
  Logger.log(`zzDescribeScenarioSyncExternalLists -> ${JSON.stringify(summary)}`);
  return summary;
}
