const CONFIG_KV_HEADER = ['Paramètre', 'Valeur'];
function formSheetName(formulaire) {
  return `${formulaire.nom} || ${formulaire.id}`;
}

function normalizeFormId(formId) {
  return String(formId || '').trim();
}

function getFormSheetById(spreadsheet, formId) {
  const suffix = ` || ${normalizeFormId(formId)}`;
  const sheets = spreadsheet.getSheets();
  for (let i = 0; i < sheets.length; i++) {
    const name = sheets[i].getName();
    if (name.endsWith(suffix)) {
      return sheets[i];
    }
  }
  return null;
}

/**
 * Gère la création et la suppression des feuilles dans le fichier Google Sheets.
 *
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} spreadsheetBdD - Le fichier Google Sheets à gérer.
 * @param {Object} formulaire - L'objet contenant les informations du formulaire.
 * @return {boolean} Retourne 'true' si la feuille n'existe pas dans le fichier (ou vide), 'false' sinon.
 */
function gestionFeuilles(spreadsheetBdD, formulaire) {
  try {
    Logger.log('GestionFeuilles : ' + formulaire.nom);
    const targetName = formSheetName(formulaire);
    let targetSheet = spreadsheetBdD.getSheetByName(targetName);

    if (!targetSheet) {
      const sheets = spreadsheetBdD.getSheets();
      if (sheets.length === 1) {
        targetSheet = sheets[0];
        if (targetSheet.getName() !== targetName) {
          targetSheet.setName(targetName);
        }
      } else if (spreadsheetBdD.getSheetByName('Reinit')) {
        targetSheet = spreadsheetBdD.getSheetByName('Reinit');
        targetSheet.setName(targetName);
      } else {
        targetSheet = spreadsheetBdD.insertSheet(targetName);
        Logger.log('Ajout de la Feuille : ' + targetName);
      }
    }

    const sheets = spreadsheetBdD.getSheets();
    for (let i = 0; i < sheets.length; i++) {
      const sheet = sheets[i];
      if (sheet.getName() !== targetName) {
        spreadsheetBdD.deleteSheet(sheet);
      }
    }

    targetSheet.clear();
    targetSheet.getRange(1, 1, 1, CONFIG_KV_HEADER.length).setValues([CONFIG_KV_HEADER]);
    return targetSheet;
  } catch (e) {
    handleException('gestionFeuilles', e);
    return null;
  }
}

/**
 * Appliquer un format nombre sur toute la feuille
 */
/**
 * Détermine si la valeur est numérique.
 *
 * @param {any} value - La valeur à vérifier.
 * @return {boolean} Retourne true si la valeur est numérique, sinon false.
 */
function isNumeric(value) {
  return !isNaN(parseFloat(value)) && isFinite(value);
}
