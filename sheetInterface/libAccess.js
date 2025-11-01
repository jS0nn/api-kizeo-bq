// libAccess Version 0.1.0

var requireSheetSymbol =
  typeof requireSheetSymbol === 'function'
    ? requireSheetSymbol
    : function (symbolName) {
        if (typeof sheetBootstrap !== 'undefined' && sheetBootstrap) {
          if (typeof sheetBootstrap.require === 'function') {
            return sheetBootstrap.require(symbolName);
          }
          if (typeof sheetBootstrap.requireMany === 'function') {
            var resolved = sheetBootstrap.requireMany([symbolName]);
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
      };
