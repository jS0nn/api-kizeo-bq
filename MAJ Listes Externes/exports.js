// exports Version 0.1.0

var majExports =
  typeof majExports !== 'undefined'
    ? majExports
    : (function () {
        function resolveSymbol(symbolName) {
          if (typeof requireMajSymbol === 'function') {
            return requireMajSymbol(symbolName);
          }
          if (typeof majBootstrap !== 'undefined' && majBootstrap) {
            if (typeof majBootstrap.require === 'function') {
              return majBootstrap.require(symbolName);
            }
            if (typeof majBootstrap.requireMany === 'function') {
              var resolved = majBootstrap.requireMany([symbolName]);
              if (resolved && Object.prototype.hasOwnProperty.call(resolved, symbolName)) {
                return resolved[symbolName];
              }
            }
          }
          if (typeof libKizeo === 'undefined' || libKizeo === null) {
            throw new Error('libKizeo indisponible (accès ' + symbolName + ')');
          }
          var value = libKizeo[symbolName];
          if (value === undefined || value === null) {
            throw new Error('libKizeo.' + symbolName + ' indisponible');
          }
          return value;
        }

        var sheetDriveExports = resolveSymbol('SheetDriveExports');

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
