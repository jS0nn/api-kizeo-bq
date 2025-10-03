/**
 * Constantes pour les noms de colonnes
 */
const ID_REPONSE = "idReponse";
const FORM_UNIQUE_ID = "form_unique_id";

/**
 * Gère les tableaux dans la feuille de calcul.
 *
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} spreadsheetBdD - La feuille de calcul de la base de données.
 * @param {Object} formulaire - Le formulaire contenant les informations de base.
 * @param {String} idReponse - L'identifiant de la réponse.
 * @param {String} nomTableau - Le nom du tableau.
 * @param {Array<Object>} tableau - Le tableau à gérer, chaque objet représentant une ligne.
 * @return {String|null} - Un lien hypertexte vers la feuille de calcul du tableau ou null en cas d'erreur.
 */
function gestionTableaux(spreadsheetBdD, formulaire, idReponse, nomTableau, tableau) {
  try {
    const nomSheetTab = `${formulaire.nom} || ${formulaire.id} || ${nomTableau}`;
    const sheetTab = getOrCreateSheet(spreadsheetBdD, nomSheetTab);
    const headers = getOrCreateHeaders(sheetTab, tableau[0]);
    
    const newRows = createNewRows(tableau, idReponse, headers, formulaire.id, spreadsheetBdD);
    appendRowsToSheet(sheetTab, newRows);
    
    return createHyperlinkToSheet(spreadsheetBdD, sheetTab);
  } catch (e) {
    console.error('Erreur dans gestionTableaux:', e);
    return null;
  }
}

/**
 * Récupère ou crée une feuille dans le classeur.
 */
function getOrCreateSheet(spreadsheet, sheetName) {
  let sheet = spreadsheet.getSheetByName(sheetName);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(sheetName);
    console.log('Feuille créée:', sheetName);
  }
  return sheet;
}

/**
 * Récupère ou crée les en-têtes de la feuille.
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - La feuille de calcul.
 * @param {Object} firstRow - Le premier objet du tableau de données.
 * @return {Array} Les en-têtes de la feuille.
 */
function getOrCreateHeaders(sheet, firstRow) {
  const lastColumn = sheet.getLastColumn();
  
  if (lastColumn === 0) {
    // La feuille est vide, créons les en-têtes
    const headers = [ID_REPONSE, FORM_UNIQUE_ID, ...Object.keys(firstRow)];
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    return headers;
  } else {
    // La feuille a déjà des données, récupérons les en-têtes existants
    const existingHeaders = sheet.getRange(1, 1, 1, lastColumn).getValues()[0];
    
    // Vérifions s'il y a de nouveaux en-têtes à ajouter
    const newHeaders = getNewHeaders(existingHeaders, firstRow);
    
    if (newHeaders.length > 0) {
      // Ajoutons les nouveaux en-têtes à la fin
      const updatedHeaders = [...existingHeaders, ...newHeaders];
      sheet.getRange(1, 1, 1, updatedHeaders.length).setValues([updatedHeaders]);
      return updatedHeaders;
    } else {
      return existingHeaders;
    }
  }
}

/**
 * Trouve les nouveaux en-têtes qui ne sont pas dans les en-têtes existants.
 * @param {Array} existingHeaders - Les en-têtes existants.
 * @param {Object} firstRow - Le premier objet du tableau de données.
 * @return {Array} Les nouveaux en-têtes à ajouter.
 */
function getNewHeaders(existingHeaders, firstRow) {
  const expectedHeaders = [ID_REPONSE, FORM_UNIQUE_ID, ...Object.keys(firstRow)];
  return expectedHeaders.filter(header => !existingHeaders.includes(header));
}



/**
 * Crée les nouvelles lignes à ajouter.
 */
function createNewRows(tableau, idReponse, headers, formId, spreadsheet) {
  return tableau.map((row, index) => {
    const form_unique_id = `${idReponse}${index}`;
    const values = [idReponse, form_unique_id];
    
    headers.slice(2).forEach(header => {
      if (row[header]) {
        const value = row[header].value;
        const type = row[header].type;
        
        if (type === "photo" || type === "signature") {
          values.push(gestionChampImage(formId, idReponse, header, value, spreadsheet));
        } else {
          values.push(isNumeric(value) ? parseFloat(value) : value);
        }
      } else {
        values.push('');
      }
    });
    
    return values;
  });
}

/**
 * Ajoute les nouvelles lignes à la feuille.
 */
function appendRowsToSheet(sheet, rows) {
  if (rows.length > 0) {
    sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
  }
}

/**
 * Crée un lien hypertexte vers la feuille.
 */
function createHyperlinkToSheet(spreadsheet, sheet) {
  const id = spreadsheet.getId();
  const sheetId = sheet.getSheetId();
  const lastRow = sheet.getLastRow();
  const cellA1 = sheet.getRange(lastRow, 1).getA1Notation();
  return `=HYPERLINK("https://docs.google.com/spreadsheets/d/${id}/edit#gid=${sheetId}&range=${cellA1}"; "${sheet.getName()}")`;
}


