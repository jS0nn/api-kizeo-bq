// bootstrap Version 0.1.0

/**
 * Résout et met en cache l'accès à la librairie Apps Script `libKizeo`.
 * Fournit également un utilitaire `require` pour récupérer un symbole en
 * vérifiant immédiatement sa disponibilité.
 */
var sheetBootstrap =
  typeof sheetBootstrap !== 'undefined'
    ? sheetBootstrap
    : (function () {
        var cachedLib = null;
        var symbolCache = {};

        function resolveLib() {
          if (cachedLib) {
            return cachedLib;
          }
          if (typeof libKizeo === 'undefined' || libKizeo === null) {
            throw new Error('libKizeo indisponible');
          }
          cachedLib = libKizeo;
          return cachedLib;
        }

        function requireSymbol(symbolName) {
          if (!symbolName) {
            throw new Error('Nom de symbole libKizeo requis');
          }
          if (Object.prototype.hasOwnProperty.call(symbolCache, symbolName)) {
            return symbolCache[symbolName];
          }
          var lib = resolveLib();
          var value = lib[symbolName];
          if (value === undefined || value === null) {
            throw new Error('libKizeo.' + symbolName + ' indisponible');
          }
          symbolCache[symbolName] = value;
          return value;
        }

        function requireMany(symbolNames) {
          if (!symbolNames || !symbolNames.length) {
            throw new Error('Liste de symboles libKizeo requise');
          }
          var result = {};
          for (var i = 0; i < symbolNames.length; i++) {
            var name = symbolNames[i];
            result[name] = requireSymbol(name);
          }
          return result;
        }

        return {
          getLib: resolveLib,
          require: requireSymbol,
          requireMany: requireMany
        };
      })();
