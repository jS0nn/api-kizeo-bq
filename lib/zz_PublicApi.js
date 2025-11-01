// PublicApi Version 0.1.0

/**
 * Construit et fige l'API publique de la librairie.
 * L’objectif est de centraliser l’exposition des symboles
 * et d’éviter les assignations éparses via `this.*`.
 */
(function bootstrapPublicApi(global) {
  var apiCache = null;

  function buildApi() {
    if (apiCache) {
      return apiCache;
    }

    var symbolResolver =
      typeof getLibPublicSymbols === 'function'
        ? getLibPublicSymbols()
        : [];

    if (!Array.isArray(symbolResolver) || !symbolResolver.length) {
      throw new Error('lib:PublicApi -> getLibPublicSymbols a retourné un tableau vide ou invalide.');
    }

    var publicApi = {};

    for (var i = 0; i < symbolResolver.length; i++) {
      var symbol = symbolResolver[i];
      var value = global[symbol];
      if (typeof value === 'undefined') {
        throw new Error('lib:PublicApi -> symbole manquant: ' + symbol);
      }
      publicApi[symbol] = value;
    }

    apiCache = Object.freeze(publicApi);
    return apiCache;
  }

  /**
   * Retourne l’objet gelé représentant l’API publique.
   * Peut être invoqué explicitement par les consommateurs
   * pour récupérer la liste des fonctions supportées.
   */
  global.getLibPublicApi = function getLibPublicApi() {
    return buildApi();
  };

  try {
    buildApi();
  } catch (error) {
    if (global && global.console && typeof global.console.log === 'function') {
      global.console.log('lib:PublicApi -> initialisation différée: ' + error);
    }
  }
})(this);
