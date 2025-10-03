
/**
 * Gère le traitement et le stockage des images associées à un champ spécifique.
 *
 * @param {string} idFormulaire - L'ID du formulaire contenant le champ de image.
 * @param {string} idReponse - L'ID de la réponse contenant le champ de image.
 * @param {string} nomChamp - Le nom du champ de image.
 * @param {string} idImage - L'ID de la image à traiter.
 * @return {string} - Lien vers la image ou le répertoire, ou une chaîne vide si aucune image n'est fournie.
 */
function gestionChampImage(idFormulaire,idReponse,nomChamp,idImage,spreadsheetBdD){
  try {
    let lienVersImage=""
    const folderName = 'Images '+idFormulaire;

    // Obtient l'ID de la feuille de calcul active
    const sheetId = spreadsheetBdD.getId(); 

    // Vérifie si une image est fournie
    if(idImage!=''){
      let tabIdImage=idImage.split(', ')

      for(let i=0;i<tabIdImage.length;i++){
        let nomImage=idReponse+' | '+nomChamp+' | '+tabIdImage[i]
        console.log('Image à traiter : '+tabIdImage[i] )
        // Faire une requête pour obtenir les données de la image
        let image=requeteAPIDonnees('GET',`/forms/${idFormulaire}/data/${idReponse}/medias/${tabIdImage[i] }`) 
        // Vérifie si la image est disponible
        if(image.responseCode!=200){
          lienVersImage ="image non disponible"
          console.log("Image Non Disponible, responseCode : "+ image.responseCode)
        }else{
          // Obtient ou crée le dossier où sauvegarder l'image
          const folderId = getOrCreateFolder(folderName, sheetId); 
          if(folderId===null){
            console.log("Erreur folderId === nulll")
            return null
          } //erreur folderId donc on sort de la fonction
          // Sauvegarde le blob d'image dans le dossier
          let idFichier=saveBlobToFolder(image.data, folderId, nomImage);
          if(idFichier===null){
            console.log("Erreur idFichier === nulll")
            return null
          } //erreur saveBlobToFolder donc on sort de la fonction
          // Crée le lien vers la image ou le dossier
          if(tabIdImage.length===1){
            lienVersImage = '=HYPERLINK(\"https://drive.google.com/uc?export=view&id='+idFichier+'\"; \"Lien vers l image\")'
          }else{
            lienVersImage= '=HYPERLINK(\"https://drive.google.com/drive/folders/'+folderId+'\"; \"Lien vers le repertoire\")'
          }
        }
      }
    }else{
      lienVersImage = ""
    }
    return lienVersImage
  } catch (e) {
    handleException('gestionChampImage', e);
    return null;
  }
}



/**
 * Sauvegarde un blob dans un dossier spécifique et retourne l'ID du fichier créé.
 *
 * @param {GoogleAppsScript.Base.Blob} blob - Le blob à sauvegarder.
 * @param {string} folderId - L'ID du dossier dans lequel sauvegarder le blob.
 * @param {string} fileName - Le nom du fichier à créer.
 * @return {string} L'ID du fichier créé.
 */
function saveBlobToFolder(blob, folderId, fileName) {
  try {
    const folder = DriveApp.getFolderById(folderId);

    // Vérifie si un fichier avec le même nom existe déjà
    const existingFileIterator = folder.getFilesByName(fileName);
  
    if (!existingFileIterator.hasNext()) {
      // Le fichier n'existe pas, crée et sauvegarde le blob en tant que fichier
      const file = folder.createFile(blob);
      file.setName(fileName);
      Logger.log(`File saved as ${fileName}`);
      return file.getId();
    } else {
      Logger.log(`File  withthe same name already exists: ${fileName}`);
      return existingFileIterator.next().getId();
    }
  } catch (e) {
    handleException('saveBlobToFolder', e);
    return null;
  }
}


/**
 * Obtient l'ID d'un dossier avec un nom donné dans le même parent que la feuille de calcul spécifiée,
 * en le créant si nécessaire.
 *
 * @param {string} folderName - Le nom du dossier à obtenir.
 * @param {string} sheetId - L'ID de la feuille de calcul dont le parent doit contenir le dossier.
 * @return {string} - L'ID du dossier.
 */
function getOrCreateFolder(folderName, sheetId) {
  try {
    // Obtient le dossier parent de la feuille de calcul (dossier racine de Drive)
    const parentFolder = DriveApp.getFileById(sheetId).getParents().next();

    // Vérifie si le dossier existe déjà
    const existingFolderIterator = parentFolder.getFoldersByName(folderName);

    if (existingFolderIterator.hasNext()) {
      const existingFolder = existingFolderIterator.next();
      return existingFolder.getId();
    } else {
      // Crée le dossier s'il n'existe pas
      const newFolder = parentFolder.createFolder(folderName);
      return newFolder.getId();
    }
  } catch (e) {
    handleException('getOrCreateFolder', e);
    return null;
  }
}
