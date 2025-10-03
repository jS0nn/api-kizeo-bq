/**
 * Écrit les données dans la feuille Google.
 *
 * @param {Object} spreadsheetBdD - La feuille Google Sheets sur laquelle écrire.
 * @param {Array} values - Les valeurs à écrire.
 * @param {Array} columnIndices - Les indices de colonnes où écrire les valeurs.
 * @param {Object} formulaire - Les informations de formulaire.
 * @param {Object} dataResponse - Les données de réponse.
 * @param {Object} sheetFormulaire - La feuille Google Sheets sur laquelle écrire.
 * @return {Array|null} - Les valeurs de la ligne écrite ou null en cas d'erreur.
 */
function writeData(spreadsheetBdD, values, columnIndices, formulaire, dataResponse, sheetFormulaire) {
  try {
    // Préparation des données de base et des champs
    const baseResponseData = values.slice(0, 9);
    const tabFields = values.slice(9);

    // Obtenir les valeurs de la ligne à écrire
    const rowValues = getRowValues(spreadsheetBdD, values, columnIndices, baseResponseData, tabFields, formulaire, dataResponse);
    if(rowValues === null) return null;

    // Écriture des données dans la feuille de calcul
    sheetFormulaire.appendRow(rowValues);

    return rowValues;
  } catch (e) {
    handleException('writeData', e);
    return null;
  }
}