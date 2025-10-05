
/**
 * Gère le traitement et le stockage des images associées à un champ spécifique.
 *
 * @param {string} idFormulaire - L'ID du formulaire contenant le champ de image.
 * @param {string} idReponse - L'ID de la réponse contenant le champ de image.
 * @param {string} nomChamp - Le nom du champ de image.
 * @param {string} idImage - L'ID de la image à traiter.
 * @return {{formula:string, files:Array, folderId:string|null, folderUrl:string|null}} Objet décrivant le lien et les fichiers capturés.
*/
function gestionChampImage(idFormulaire, idReponse, nomChamp, idImage, spreadsheetBdD, options) {
  try {
    const fieldType = (options && options.fieldType) || 'media';
    const result = {
      formula: '',
      files: [],
      folderId: null,
      folderUrl: null,
      fieldName: nomChamp,
      fieldType: fieldType
    };

    if (!idImage) {
      return result;
    }

    const mediaIds = String(idImage)
      .split(/,\s*/)
      .map((value) => value.trim())
      .filter((value) => value.length);

    if (!mediaIds.length) {
      return result;
    }

    const folderName = 'Images ' + idFormulaire;
    const sheetId = spreadsheetBdD.getId();
    const folderId = getOrCreateFolder(folderName, sheetId);

    if (folderId === null) {
      console.log('gestionChampImage: folderId null pour le formulaire ' + idFormulaire);
      return null;
    }

    const folderUrl = 'https://drive.google.com/drive/folders/' + folderId;

    mediaIds.forEach((mediaId) => {
      const nomImage = idReponse + ' | ' + nomChamp + ' | ' + mediaId;
      console.log('Image à traiter : ' + mediaId);

      const image = requeteAPIDonnees('GET', `/forms/${idFormulaire}/data/${idReponse}/medias/${mediaId}`);
      if (!image || image.responseCode !== 200) {
        console.log('Image Non Disponible, responseCode : ' + (image ? image.responseCode : 'undefined'));
        return;
      }

      const fileId = saveBlobToFolder(image.data, folderId, nomImage);
      if (fileId === null) {
        console.log('gestionChampImage: échec sauvegarde blob pour ' + nomImage);
        return;
      }

      const driveUrl = 'https://drive.google.com/uc?export=view&id=' + fileId;
      result.files.push({
        fileId: fileId,
        fileName: nomImage,
        driveUrl: driveUrl,
        folderId: folderId,
        folderUrl: folderUrl,
        mediaId: mediaId,
        fieldName: nomChamp,
        fieldType: fieldType
      });
    });

    result.folderId = folderId;
    result.folderUrl = folderUrl;

    if (!result.files.length) {
      result.formula = 'image non disponible';
      return result;
    }

    if (result.files.length === 1) {
      result.formula = '=HYPERLINK("' + result.files[0].driveUrl + '"; "Lien vers l image")';
    } else {
      result.formula = '=HYPERLINK("' + folderUrl + '"; "Lien vers le repertoire")';
    }

    return result;
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
