/**
 * Marque les réponses comme non lues dans le formulaire correspondant.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheetEnCours - La feuille de calcul en cours d'utilisation.
 */
function marquerReponseNonLues(sheetEnCours,action) {
  const ongletName = sheetEnCours.getName();
  const ongletTabName = ongletName.split(' || ');
  const formulaire = {
    nom: ongletTabName[0],
    id: ongletTabName[1]
  };
  
  const responseID = getValuesResponseId(sheetEnCours);

  if (responseID.length > 0) {
    // Marquer les données comme non lues
    const dataNonLues = { "data_ids": responseID };
    requeteAPIDonnees('POST', `/forms/${formulaire.id}/markasunreadbyaction/${action}`, dataNonLues);
    console.log("Les données ont été marquées non lues")
  }
}

/**
 * Récupère les valeurs de la colonne "id" (sans la première ligne) dans la feuille donnée.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheetEnCours - La feuille de calcul en cours d'utilisation.
 * @return {Array} Un tableau des valeurs de la colonne "id" (sans la première ligne).
 * @throws {Error} Si la colonne "id" n'est pas trouvée.
 */
function getValuesResponseId(sheetEnCours) {
  const data = sheetEnCours.getDataRange().getValues();
  const headerRow = data[0];
  
  // Trouver l'index de la colonne dont le titre est "id"
  const idColumnIndex = headerRow.indexOf("id");
  if (idColumnIndex === -1) {
    console.log('La colonne "id" n\'a pas été trouvée.');
    return [];
  }
  
  // Lire les valeurs de la colonne "id" (sans la première ligne)
  return data.slice(1).map(row => row[idColumnIndex]);
}