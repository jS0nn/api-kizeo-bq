
/**
 * Gère le traitement et le stockage des images associées à un champ spécifique.
 *
 * @param {string} idFormulaire - L'ID du formulaire contenant le champ de image.
 * @param {string} idReponse - L'ID de la réponse contenant le champ de image.
 * @param {string} nomChamp - Le nom du champ de image.
 * @param {string} idImage - L'ID de la image à traiter.
 * @return {{formula:string, files:Array, folderId:string|null, folderUrl:string|null}} Objet décrivant le lien et les fichiers capturés.
*/

const DRIVE_FOLDER_CACHE = Object.create(null);
const DRIVE_FOLDER_FILE_CACHE = Object.create(null);

/**
 * Construit le nom du dossier Drive pour stocker les médias.
 *
 * @param {string} formId - Identifiant du formulaire.
 * @param {string} rawFormName - Libellé du formulaire.
 * @return {string} Nom de dossier normalisé.
 */
function buildMediaFolderName(formId, rawFormName) {
  const baseName = 'Medias ' + formId;
  const formName = rawFormName ? String(rawFormName).trim() : '';
  if (!formName) {
    return baseName;
  }
  const sanitized = formName
    .replace(/[\\/:*?"<>|]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!sanitized) {
    return baseName;
  }
  return baseName + ' ' + sanitized;
}

/**
 * Construit les différentes URLs utiles pour un fichier Drive.
 * @param {string} fileId
 * @return {{contentUrl:string, viewUrl:string, publicUrl:string}}
 */
function buildDriveMediaUrls(fileId) {
  const normalized = fileId ? String(fileId).trim() : '';
  if (!normalized) {
    return {
      contentUrl: '',
      viewUrl: '',
      publicUrl: ''
    };
  }
  const sanitized = normalized.replace(/[^A-Za-z0-9_-]/g, '');
  if (!sanitized) {
    return {
      contentUrl: '',
      viewUrl: '',
      publicUrl: ''
    };
  }
  const directUrl = 'https://drive.google.com/uc?export=view&id=' + sanitized;
  const viewUrl = 'https://drive.google.com/file/d/' + sanitized + '/view?usp=drivesdk';
  return {
    contentUrl: directUrl,
    viewUrl: viewUrl,
    publicUrl: normalizeDrivePublicUrl(viewUrl) || directUrl
  };
}

/**
 * Transforme une URL Google Drive "viewer" en lien direct utilisable par Looker Studio.
 * - Conserve les URL déjà directes (uc) ou hébergées sur lh3.
 * - Extrait l'identifiant à partir des motifs "/d/<ID>/" ou "?id=<ID>".
 *
 * @param {string} rawUrl - URL provenant de Drive ou d'une source Kizeo.
 * @return {string} URL normalisée au format https://drive.google.com/uc?export=view&id=<ID>
 */
function normalizeDrivePublicUrl(rawUrl) {
  const urlString = rawUrl ? String(rawUrl).trim() : '';
  if (!urlString) return '';

  if (urlString.indexOf('https://drive.google.com/uc?export=view&id=') === 0) {
    return urlString;
  }

  if (urlString.indexOf('https://lh3.googleusercontent.com/d/') === 0) {
    return urlString;
  }

  const directPattern = /\/d\/([^/?#]+)/;
  const queryPattern = /[?&]id=([^&#]+)/;

  let fileId = '';
  const directMatch = urlString.match(directPattern);
  if (directMatch && directMatch[1]) {
    fileId = directMatch[1];
  } else {
    const queryMatch = urlString.match(queryPattern);
    if (queryMatch && queryMatch[1]) {
      fileId = queryMatch[1];
    }
  }

  if (!fileId) {
    return urlString;
  }

  let decodedId = fileId;
  try {
    decodedId = decodeURIComponent(fileId);
  } catch (e) {
    decodedId = fileId;
  }

  const sanitized = decodedId.replace(/[^A-Za-z0-9_-]/g, '');
  if (!sanitized) {
    return urlString;
  }

  return 'https://drive.google.com/uc?export=view&id=' + sanitized;
}

function gestionChampImage(idFormulaire, idReponse, nomChamp, idImage, spreadsheetBdD, options) {
  try {
    const executionOptions = options || {};
    const fieldType = executionOptions.fieldType || 'media';
    const formName = executionOptions.formName || '';
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

    const folderName = buildMediaFolderName(idFormulaire, formName);
    const sheetId = spreadsheetBdD.getId();
    const folderId = getOrCreateFolder(folderName, sheetId);

    if (folderId === null) {
      console.log('gestionChampImage: folderId null pour le formulaire ' + idFormulaire);
      return null;
    }

    const folderUrl = 'https://drive.google.com/drive/folders/' + folderId;
    const folder = getCachedDriveFolder(folderId);
    if (!folder) {
      console.log('gestionChampImage: dossier introuvable pour le formulaire ' + idFormulaire);
      return null;
    }

    mediaIds.forEach((mediaId) => {
      const nomImage = idReponse + ' | ' + nomChamp + ' | ' + mediaId;
      console.log('Image à traiter : ' + mediaId);

      const existingFileId = lookupFileIdInFolder(folder, folderId, nomImage);
      if (existingFileId) {
        console.log('gestionChampImage: média déjà présent, réutilisation de ' + nomImage);
        const urls = buildDriveMediaUrls(existingFileId);
        result.files.push({
          fileId: existingFileId,
          fileName: nomImage,
          driveUrl: urls.contentUrl,
          driveViewUrl: urls.viewUrl,
          drivePublicUrl: urls.publicUrl,
          folderId: folderId,
          folderUrl: folderUrl,
          mediaId: mediaId,
          fieldName: nomChamp,
          fieldType: fieldType
        });
        return;
      }

      const image = requeteAPIDonnees('GET', `/forms/${idFormulaire}/data/${idReponse}/medias/${mediaId}`);
      if (!image || image.responseCode !== 200) {
        console.log('Image Non Disponible, responseCode : ' + (image ? image.responseCode : 'undefined'));
        return;
      }

      const fileId = saveBlobToFolder(image.data, folderId, nomImage, { folder: folder });
      if (fileId === null) {
        console.log('gestionChampImage: échec sauvegarde blob pour ' + nomImage);
        return;
      }

      const urls = buildDriveMediaUrls(fileId);
      result.files.push({
        fileId: fileId,
        fileName: nomImage,
        driveUrl: urls.contentUrl,
        driveViewUrl: urls.viewUrl,
        drivePublicUrl: urls.publicUrl,
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
 * @param {{folder: GoogleAppsScript.Drive.Folder}=} options - Options d'exécution (dossier déjà chargé).
 * @return {string} L'ID du fichier créé.
 */
function saveBlobToFolder(blob, folderId, fileName, options) {
  try {
    const folder =
      (options && options.folder) ||
      getCachedDriveFolder(folderId);
    if (!folder) {
      console.log('saveBlobToFolder: dossier introuvable pour ' + folderId);
      return null;
    }

    const existingFileId = lookupFileIdInFolder(folder, folder.getId(), fileName);
    if (existingFileId) {
      Logger.log(`File already present: ${fileName}`);
      return existingFileId;
    }

    const file = folder.createFile(blob);
    file.setName(fileName);
    const fileId = file.getId();
    rememberFileInCache(folder.getId(), fileName, fileId);
    Logger.log(`File saved as ${fileName}`);
    return fileId;
  } catch (e) {
    handleException('saveBlobToFolder', e);
    return null;
  }
}


/**
 * Cherche un fichier existant dans un dossier par son nom.
 *
 * @param {GoogleAppsScript.Drive.Folder} folder - Dossier dans lequel chercher.
 * @param {string} fileName - Nom du fichier recherché.
 * @return {GoogleAppsScript.Drive.File|null} - Le fichier trouvé ou null.
 */
function findExistingFileInFolder(folder, fileName) {
  try {
    if (!folder) return null;
    const folderId = folder.getId();
    const fileId = lookupFileIdInFolder(folder, folderId, fileName);
    if (fileId) {
      return DriveApp.getFileById(fileId);
    }
    return null;
  } catch (e) {
    handleException('findExistingFileInFolder', e);
    return null;
  }
}

function getCachedDriveFolder(folderId) {
  if (!folderId) return null;
  if (DRIVE_FOLDER_CACHE[folderId]) {
    return DRIVE_FOLDER_CACHE[folderId];
  }
  try {
    const folder = DriveApp.getFolderById(folderId);
    DRIVE_FOLDER_CACHE[folderId] = folder;
    return folder;
  } catch (e) {
    handleException('getCachedDriveFolder', e, { folderId: folderId });
    return null;
  }
}

function getFolderFileCache(folderId) {
  if (!folderId) return null;
  if (!DRIVE_FOLDER_FILE_CACHE[folderId]) {
    DRIVE_FOLDER_FILE_CACHE[folderId] = Object.create(null);
  }
  return DRIVE_FOLDER_FILE_CACHE[folderId];
}

function rememberFileInCache(folderId, fileName, fileId) {
  if (!folderId || !fileName) return;
  const cache = getFolderFileCache(folderId);
  if (!cache) return;
  cache[fileName] = fileId || '';
}

function lookupFileIdInFolder(folder, folderId, fileName) {
  if (!folder || !folderId || !fileName) return null;
  const cache = getFolderFileCache(folderId);
  if (!cache) return null;
  if (Object.prototype.hasOwnProperty.call(cache, fileName)) {
    const cached = cache[fileName];
    return cached || null;
  }

  const iterator = folder.getFilesByName(fileName);
  if (iterator.hasNext()) {
    const file = iterator.next();
    const fileId = file.getId();
    cache[fileName] = fileId;
    return fileId;
  }

  cache[fileName] = '';
  return null;
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
