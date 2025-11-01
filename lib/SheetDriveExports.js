// SheetDriveExports Version 0.1.0

function sheetDriveGetOrCreateSubFolder(parentFolderId, subFolderName) {
  var parent = DriveApp.getFolderById(parentFolderId);
  var iterator = parent.getFoldersByName(subFolderName);
  if (iterator.hasNext()) {
    return iterator.next().getId();
  }
  return parent.createFolder(subFolderName).getId();
}

function sheetDriveBuildMediaDisplayName(media) {
  var baseName = media.name || media.fileName || 'media_' + (media.dataId || 'unknown');
  var driveId = media.driveFileId || '';
  if (!driveId) {
    return baseName;
  }
  var sanitizedId = driveId.replace(/[^A-Za-z0-9_-]/g, '');
  if (!sanitizedId || baseName.indexOf(sanitizedId) !== -1) {
    return baseName;
  }
  return baseName + '__' + sanitizedId;
}

function sheetDriveExportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  var fileName =
    formulaireNom +
    '_' +
    dataId +
    '_' +
    new Date()
      .toISOString()
      .replace(/[:.]/g, '-');
  try {
    DriveMediaService.getDefault().saveBlobToFolder(pdfBlob, targetFolderId, fileName);
  } catch (driveError) {
    if (typeof handleException === 'function') {
      handleException('SheetDriveExports.exportPdfBlob', driveError, { targetFolderId: targetFolderId, fileName: fileName });
    }
  }
}

function sheetDriveExportMedias(mediaList, targetFolderId) {
  if (!mediaList || !mediaList.length) {
    return;
  }

  var mediaFolderId = sheetDriveGetOrCreateSubFolder(targetFolderId, 'media');
  var mediaFolder = DriveApp.getFolderById(mediaFolderId);

  mediaList.forEach(function (media) {
    try {
      var displayName = sheetDriveBuildMediaDisplayName(media);
      var fileId = media.driveFileId || '';

      if (!fileId && typeof media.id === 'string' && media.id.indexOf('id=') !== -1) {
        fileId = media.id.split('id=')[1].split('"')[0];
      }

      if (!fileId && typeof media.driveUrl === 'string' && media.driveUrl.indexOf('id=') !== -1) {
        fileId = media.driveUrl.split('id=')[1].split('&')[0];
      }

      if (!fileId) {
        console.log('SheetDriveExports: ID Drive introuvable pour ' + displayName);
        return;
      }

      if (mediaFolder.getFilesByName(displayName).hasNext()) {
        return;
      }

      var file = DriveApp.getFileById(fileId);
      file.makeCopy(displayName, mediaFolder);
    } catch (error) {
      console.log(
        'SheetDriveExports: copie média échouée (' +
          (media.name || media.fileName || 'unknown') +
          ') -> ' +
          error +
          '\\nID original: ' +
          (media.driveFileId || media.id || '')
      );
    }
  });
}

var SheetDriveExports = {
  getOrCreateSubFolder: sheetDriveGetOrCreateSubFolder,
  buildMediaDisplayName: sheetDriveBuildMediaDisplayName,
  exportPdfBlob: sheetDriveExportPdfBlob,
  exportMedias: sheetDriveExportMedias
};

this.SheetDriveExports = SheetDriveExports;

