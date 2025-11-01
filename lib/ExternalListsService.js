// ExternalListsService Version 0.3.0

function externalListsCreateDependencies(overrides) {
  var opts = overrides || {};
  var log =
    typeof opts.log === 'function'
      ? opts.log
      : function (message) {
          try {
            console.log(message);
          } catch (error) {
            // ignore logging failure
          }
        };

  var fetchFn =
    typeof opts.fetch === 'function'
      ? opts.fetch
      : function (method, path, payload) {
          if (typeof requeteAPIDonnees === 'function') {
            return requeteAPIDonnees(method, path, payload);
          }
          throw new Error('ExternalListsService: fetch indisponible');
        };

  var handle =
    typeof opts.handleException === 'function'
      ? opts.handleException
      : function (name, error, context) {
          if (typeof handleException === 'function') {
            handleException(name, error, context);
          } else {
            log(name + ': ' + error);
          }
        };

  return { fetch: fetchFn, handleException: handle, log: log };
}

function externalListsReplaceKizeoData(array, searchValue, replaceValue) {
  try {
    var separator = '|';
    var searchValueKizeo = searchValue + ':' + searchValue;
    var replaceValueKizeo = replaceValue + ':' + replaceValue;

    if (!Array.isArray(array) || !array.length) {
      return array;
    }

    var headers = array[0].split(separator);
    var index = headers.findIndex(function (value) {
      return value.trim() === searchValueKizeo;
    });
    if (index === -1) {
      return array;
    }

    for (var i = 1; i < array.length; i++) {
      var current = array[i].split(separator);
      current[index] = replaceValueKizeo;
      array[i] = current.join(separator);
    }

    return array;
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('replaceKizeoData', error);
    }
    return null;
  }
}

function externalListsUpdateFromSnapshot(formulaire, snapshot, overrides) {
  var deps = externalListsCreateDependencies(overrides);

  if (!formulaire || !formulaire.id) {
    deps.log('ExternalListsService: formulaire invalide, mise à jour ignorée.');
    return 'IGNORED';
  }

  if (!snapshot || !Array.isArray(snapshot.existingHeaders) || !Array.isArray(snapshot.rowEnCours)) {
    deps.log('ExternalListsService: snapshot incomplet, mise à jour ignorée.');
    return 'IGNORED';
  }

  try {
    var listsResponse = deps.fetch('GET', '/lists');
    var lists =
      listsResponse && listsResponse.data && Array.isArray(listsResponse.data.lists)
        ? listsResponse.data.lists
        : [];

    if (!lists.length) {
      deps.log('ExternalListsService: aucune liste disponible.');
      return 'IGNORED';
    }

    var headers = snapshot.existingHeaders;
    var values = snapshot.rowEnCours;
    var updatePerformed = false;

    lists.forEach(function (liste) {
      var nameParts = (liste.name || '').split(' || ');
      var listeFormId = nameParts.length > 1 ? nameParts[1] : '';
      if (!listeFormId || String(listeFormId) !== String(formulaire.id)) {
        return;
      }

      var detailResponse = deps.fetch('GET', '/lists/' + liste.id);
      var detailList = detailResponse && detailResponse.data && detailResponse.data.list;
      if (!detailList || !Array.isArray(detailList.items) || !detailList.items.length) {
        deps.log('ExternalListsService: détail liste ' + liste.id + ' vide ou invalide.');
        return;
      }

      var items = detailList.items.slice();
      var headerLine = items[0] ? items[0].split('|') : [];
      if (!headerLine.length) {
        deps.log('ExternalListsService: entêtes liste ' + liste.id + ' introuvables.');
        return;
      }

      var shouldUpdate = false;
      for (var i = 1; i < headerLine.length; i++) {
        var variableParts = headerLine[i].split(':');
        var variable = variableParts[0];
        if (!variable) {
          continue;
        }
        var headerIndex = headers.indexOf(variable);
        if (headerIndex === -1) {
          deps.log('ExternalListsService: variable ' + variable + ' introuvable pour form ' + formulaire.id);
          continue;
        }
        var rawValue = values[headerIndex];
        var safeValue = rawValue === undefined || rawValue === null ? '' : rawValue;
        externalListsReplaceKizeoData(items, variable, safeValue);
        shouldUpdate = true;
      }

      if (shouldUpdate) {
        deps.fetch('PUT', '/lists/' + liste.id, { items: items });
        updatePerformed = true;
      }
    });

    return updatePerformed ? 'Mise A Jour OK' : 'IGNORED';
  } catch (error) {
    deps.handleException('ExternalListsService.updateFromSnapshot', error, {
      formId: (formulaire && formulaire.id) || 'unknown'
    });
    return null;
  }
}

var ExternalListsService = {
  updateFromSnapshot: externalListsUpdateFromSnapshot,
  replaceItems: externalListsReplaceKizeoData
};

if (typeof replaceKizeoData !== 'function') {
  var replaceKizeoData = externalListsReplaceKizeoData;
}

this.ExternalListsService = ExternalListsService;
this.replaceKizeoData = externalListsReplaceKizeoData;
