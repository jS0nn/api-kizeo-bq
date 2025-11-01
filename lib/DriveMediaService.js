(function (global) {
  if (typeof DriveMediaService !== 'undefined') {
    return;
  }

  function createDriveMediaService(overrides) {
    const driveApp = (overrides && overrides.driveApp) || global.DriveApp;
    const fetchFn =
      (overrides && overrides.fetch) ||
      (typeof global.requeteAPIDonnees === 'function' ? global.requeteAPIDonnees : null);
    const handle =
      (overrides && overrides.handleException) ||
      (typeof global.handleException === 'function' ? global.handleException : function () {});
    const logger =
      (overrides && overrides.logger) ||
      (typeof console !== 'undefined'
        ? {
            log: function (message) {
              try {
                console.log(message);
              } catch (e) {
                // ignore
              }
            }
          }
        : { log: function () {} });

    if (!driveApp || typeof driveApp.getFolderById !== 'function') {
      throw new Error('DriveMediaService: DriveApp indisponible');
    }
    if (fetchFn === null) {
      throw new Error('DriveMediaService: fetch indisponible');
    }

    const folderCache = Object.create(null);
    const folderFileCache = Object.create(null);

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

    function getFolderFileCache(folderId) {
      if (!folderId) return null;
      if (!folderFileCache[folderId]) {
        folderFileCache[folderId] = Object.create(null);
      }
      return folderFileCache[folderId];
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

    function getCachedDriveFolder(folderId) {
      if (!folderId) return null;
      if (folderCache[folderId]) {
        return folderCache[folderId];
      }
      try {
        const folder = driveApp.getFolderById(folderId);
        folderCache[folderId] = folder;
        return folder;
      } catch (e) {
        handle('DriveMediaService.getCachedDriveFolder', e, { folderId: folderId });
        return null;
      }
    }

    function resetCaches() {
      Object.keys(folderCache).forEach(function (key) {
        delete folderCache[key];
      });
      Object.keys(folderFileCache).forEach(function (key) {
        delete folderFileCache[key];
      });
    }

    function getOrCreateFolder(folderName, sheetId) {
      try {
        const file = driveApp.getFileById(sheetId);
        const parents = file.getParents();
        if (!parents.hasNext()) {
          throw new Error('Aucun parent trouvé pour le classeur ' + sheetId);
        }
        const parentFolder = parents.next();
        const existingFolderIterator = parentFolder.getFoldersByName(folderName);

        if (existingFolderIterator.hasNext()) {
          const existingFolder = existingFolderIterator.next();
          return existingFolder.getId();
        }

        const newFolder = parentFolder.createFolder(folderName);
        return newFolder.getId();
      } catch (e) {
        handle('DriveMediaService.getOrCreateFolder', e, { folderName: folderName, sheetId: sheetId });
        return null;
      }
    }

    function saveBlobToFolder(blob, folderId, fileName, options) {
      try {
        const folder =
          (options && options.folder) ||
          getCachedDriveFolder(folderId);
        if (!folder) {
          logger.log('DriveMediaService.saveBlobToFolder: dossier introuvable pour ' + folderId);
          return null;
        }

        const existingFileId = lookupFileIdInFolder(folder, folder.getId(), fileName);
        if (existingFileId) {
          logger.log('DriveMediaService.saveBlobToFolder: fichier déjà présent (' + fileName + ')');
          return existingFileId;
        }

        const file = folder.createFile(blob);
        file.setName(fileName);
        const fileId = file.getId();
        rememberFileInCache(folder.getId(), fileName, fileId);
        return fileId;
      } catch (e) {
        handle('DriveMediaService.saveBlobToFolder', e, { folderId: folderId, fileName: fileName });
        return null;
      }
    }

    function findExistingFileInFolder(folder, fileName) {
      try {
        if (!folder) return null;
        const folderId = folder.getId();
        const fileId = lookupFileIdInFolder(folder, folderId, fileName);
        if (fileId) {
          return driveApp.getFileById(fileId);
        }
        return null;
      } catch (e) {
        handle('DriveMediaService.findExistingFileInFolder', e, { fileName: fileName });
        return null;
      }
    }

    function processField(formId, dataId, fieldName, rawMediaIds, spreadsheet, options) {
      try {
        const executionOptions = options || {};
        const fieldType = executionOptions.fieldType || 'media';
        const formName = executionOptions.formName || '';
        const result = {
          formula: '',
          files: [],
          folderId: null,
          folderUrl: null,
          fieldName: fieldName,
          fieldType: fieldType
        };

        if (!rawMediaIds) {
          return result;
        }

        const mediaIds = String(rawMediaIds)
          .split(/,\s*/)
          .map(function (value) {
            return value.trim();
          })
          .filter(function (value) {
            return value.length;
          });

        if (!mediaIds.length) {
          return result;
        }

        if (!spreadsheet || typeof spreadsheet.getId !== 'function') {
          logger.log('DriveMediaService.processField: spreadsheet invalide');
          return null;
        }

        const folderName = buildMediaFolderName(formId, formName);
        const sheetId = spreadsheet.getId();
        const folderId = getOrCreateFolder(folderName, sheetId);

        if (folderId === null) {
          logger.log('DriveMediaService.processField: folderId null pour le formulaire ' + formId);
          return null;
        }

        const folderUrl = 'https://drive.google.com/drive/folders/' + folderId;
        const folder = getCachedDriveFolder(folderId);
        if (!folder) {
          logger.log('DriveMediaService.processField: dossier introuvable pour le formulaire ' + formId);
          return null;
        }

        mediaIds.forEach(function (mediaId) {
          const fileLabel = dataId + ' | ' + fieldName + ' | ' + mediaId;

          const existingFileId = lookupFileIdInFolder(folder, folderId, fileLabel);
          if (existingFileId) {
            logger.log('DriveMediaService.processField: média déjà présent, réutilisation de ' + fileLabel);
            const urlsExisting = buildDriveMediaUrls(existingFileId);
            result.files.push({
              fileId: existingFileId,
              fileName: fileLabel,
              driveUrl: urlsExisting.contentUrl,
              driveViewUrl: urlsExisting.viewUrl,
              drivePublicUrl: urlsExisting.publicUrl,
              folderId: folderId,
              folderUrl: folderUrl,
              mediaId: mediaId,
              fieldName: fieldName,
              fieldType: fieldType
            });
            return;
          }

          const image = fetchFn('GET', '/forms/' + formId + '/data/' + dataId + '/medias/' + mediaId);
          if (!image || image.responseCode !== 200) {
            logger.log(
              'DriveMediaService.processField: média non disponible (' +
                mediaId +
                '), code=' +
                (image ? image.responseCode : 'undefined')
            );
            return;
          }

          const fileId = saveBlobToFolder(image.data, folderId, fileLabel, { folder: folder });
          if (fileId === null) {
            logger.log('DriveMediaService.processField: échec sauvegarde blob pour ' + fileLabel);
            return;
          }

          const urls = buildDriveMediaUrls(fileId);
          result.files.push({
            fileId: fileId,
            fileName: fileLabel,
            driveUrl: urls.contentUrl,
            driveViewUrl: urls.viewUrl,
            drivePublicUrl: urls.publicUrl,
            folderId: folderId,
            folderUrl: folderUrl,
            mediaId: mediaId,
            fieldName: fieldName,
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
        handle('DriveMediaService.processField', e, {
          formId: formId,
          fieldName: fieldName
        });
        return null;
      }
    }

    return {
      buildMediaFolderName: buildMediaFolderName,
      normalizeDrivePublicUrl: normalizeDrivePublicUrl,
      buildDriveMediaUrls: buildDriveMediaUrls,
      getOrCreateFolder: getOrCreateFolder,
      getCachedDriveFolder: getCachedDriveFolder,
      getFolderFileCache: getFolderFileCache,
      rememberFileInCache: rememberFileInCache,
      lookupFileIdInFolder: lookupFileIdInFolder,
      findExistingFileInFolder: findExistingFileInFolder,
      saveBlobToFolder: saveBlobToFolder,
      processField: processField,
      resetCaches: resetCaches
    };
  }

  const defaultService = createDriveMediaService();

  global.DriveMediaService = {
    create: createDriveMediaService,
    getDefault: function () {
      return defaultService;
    }
  };
})(this);
