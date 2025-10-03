/**
Fonction principale pour charger les données, supprimer les doublons et mettre à jour la feuille.
@param {GoogleAppsScript.Spreadsheet.Sheet} sheet La feuille Google Sheets à traiter.
*/
function gestionDesDonneesMaJ(sheet) {
  console.log("Gestion des doublons")
  // Charger les données de la feuille
  const data = sheet.getDataRange().getValues();

  // Supprimer les doublons dans la colonne "form_unique_id"
  const rowsToRemove = filterDuplicates(data);

  // Mettre à jour la feuille avec les données filtrées
  if(rowsToRemove.length>0){
    removeRows(sheet, rowsToRemove)
  }
}


/**
 * Trouve les indices des lignes où les 'form_unique_id' sont identiques, puis retourne les numéros des lignes à supprimer.
 * @param {Array<Array<any>>} data - Les données de la feuille.
 * @return {Array<number>} - Les numéros des lignes à supprimer.
 */
function filterDuplicates(data) {
  const header = data[0];
  const uniqueIdIndex = header.indexOf('form_unique_id');
  if (uniqueIdIndex === -1) {
    throw new Error("La colonne 'form_unique_id' n'existe pas.");
  }

  const seen = {};
  const rowsToRemove = [];

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const uniqueId = row[uniqueIdIndex];
    if (seen[uniqueId] !== undefined) {
      // Ajouter le numéro de ligne précédent à la liste des suppressions
      rowsToRemove.push(seen[uniqueId] + 1); // +1 pour correspondre au numéro de ligne réel
    }
    // Mettre à jour l'index avec l'index actuel, conservant ainsi la dernière occurrence
    seen[uniqueId] = i;
  }
  if(rowsToRemove.length>0){
    console.log(`Lignes à supprimer : ${rowsToRemove}`)
  }else{
    console.log(`Aucune ligne à supprimer`)
  }
  
  // On a les numéros des lignes à supprimer
  return rowsToRemove;
}

/**
 * Supprime les lignes à partir de leurs numéros.
 * @param {Sheet} sheet - La feuille Google Sheets.
 * @param {Array<number>} rowsToRemove - Les numéros des lignes à supprimer.
 */
function removeRows(sheet, rowsToRemove) {
  rowsToRemove.sort((a, b) => b - a); // Trier les numéros en ordre décroissant pour éviter les problèmes lors de la suppression
  rowsToRemove.forEach(rowNumber => {
    sheet.deleteRow(rowNumber);
  });
}


