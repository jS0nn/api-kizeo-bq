
function majListeExterne(formulaire, dataEnCours) {
  const service =
    typeof ExternalListsService !== 'undefined' && ExternalListsService && ExternalListsService.updateFromSnapshot
      ? ExternalListsService
      : null;
  if (!service) {
    throw new Error('ExternalListsService indisponible pour majListeExterne');
  }
  return service.updateFromSnapshot(formulaire, dataEnCours, {
    fetch: typeof requeteAPIDonnees === 'function' ? requeteAPIDonnees : undefined,
    handleException: typeof handleException === 'function' ? handleException : undefined,
    log: console.log.bind(console)
  });
}

/**
 * Remplace une valeur dans un tableau en utilisant la syntaxe Kizeo.
 *
 * @param {Array} array - Le tableau dans lequel rechercher et remplacer une valeur.
 * @param {string} searchValue - La valeur à rechercher.
 * @param {string} replaceValue - La valeur par laquelle remplacer.
 * @return {Array} - Le tableau avec les valeurs remplacées.
 */
function replaceKizeoData(array, searchValue, replaceValue) {
  if (
    typeof ExternalListsService !== 'undefined' &&
    ExternalListsService &&
    typeof ExternalListsService.replaceItems === 'function'
  ) {
    return ExternalListsService.replaceItems(array, searchValue, replaceValue);
  }
  throw new Error('ExternalListsService.replaceItems indisponible');
}

(function (global) {
  global.majListeExterne = majListeExterne;
  if (typeof global.replaceKizeoData !== 'function') {
    global.replaceKizeoData = replaceKizeoData;
  }
})(this);
