// libAccess Version 0.1.0

var requireMajSymbol =
  typeof requireMajSymbol !== 'undefined' && typeof requireMajSymbol === 'function'
    ? requireMajSymbol
    : function (symbolName) {
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
      };
