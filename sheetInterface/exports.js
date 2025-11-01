// exports Version 0.1.0

var sheetExports =
  typeof sheetExports !== 'undefined'
    ? sheetExports
    : (function () {
        if (typeof libKizeo === 'undefined' || libKizeo === null) {
          throw new Error('libKizeo indisponible (résolution SheetDriveExports)');
        }

        var sheetDriveExports = libKizeo.SheetDriveExports;
        if (!sheetDriveExports) {
          throw new Error('SheetDriveExports indisponible via libKizeo');
        }

        function getOrCreateSubFolder(parentFolderId, subFolderName) {
          return sheetDriveExports.getOrCreateSubFolder(parentFolderId, subFolderName);
        }

        function buildMediaDisplayName(media) {
          return sheetDriveExports.buildMediaDisplayName(media);
        }

        function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
          sheetDriveExports.exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId);
        }

        function exportMedias(mediaList, targetFolderId) {
          sheetDriveExports.exportMedias(mediaList, targetFolderId);
        }

        return {
          getOrCreateSubFolder: getOrCreateSubFolder,
          buildMediaDisplayName: buildMediaDisplayName,
          exportPdfBlob: exportPdfBlob,
          exportMedias: exportMedias
        };
      })();
