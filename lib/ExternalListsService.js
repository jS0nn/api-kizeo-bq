(function (global) {
  function createDependencies(overrides) {
    const opts = overrides || {};
    const log =
      typeof opts.log === 'function'
        ? opts.log
        : function (message) {
            try {
              console.log(message);
            } catch (e) {
              // ignore logging failure
            }
          };

    const fetchFn =
      typeof opts.fetch === 'function'
        ? opts.fetch
        : function (method, path, payload) {
            if (typeof requeteAPIDonnees === 'function') {
              return requeteAPIDonnees(method, path, payload);
            }
            throw new Error('ExternalListsService: fetch indisponible');
          };

    const handle =
      typeof opts.handleException === 'function'
        ? opts.handleException
        : function (name, error, context) {
            if (typeof handleException === 'function') {
              handleException(name, error, context);
            } else {
              log(`${name}: ${error}`);
            }
          };

    return { fetch: fetchFn, handleException: handle, log };
  }

  function replaceKizeoData(array, searchValue, replaceValue) {
    try {
      const separator = '|';
      const searchValueKizeo = `${searchValue}:${searchValue}`;
      const replaceValueKizeo = `${replaceValue}:${replaceValue}`;

      if (!Array.isArray(array) || !array.length) {
        return array;
      }

      const headers = array[0].split(separator);
      const index = headers.findIndex((value) => value.trim() === searchValueKizeo);
      if (index === -1) {
        return array;
      }

      for (let i = 1; i < array.length; i++) {
        const current = array[i].split(separator);
        current[index] = replaceValueKizeo;
        array[i] = current.join(separator);
      }

      return array;
    } catch (e) {
      if (typeof handleException === 'function') {
        handleException('replaceKizeoData', e);
      }
      return null;
    }
  }

  function updateExternalListFromSnapshot(formulaire, snapshot, overrides) {
    const deps = createDependencies(overrides);

    if (!formulaire || !formulaire.id) {
      deps.log('ExternalListsService: formulaire invalide, mise à jour ignorée.');
      return 'IGNORED';
    }

    if (!snapshot || !Array.isArray(snapshot.existingHeaders) || !Array.isArray(snapshot.rowEnCours)) {
      deps.log('ExternalListsService: snapshot incomplet, mise à jour ignorée.');
      return 'IGNORED';
    }

    try {
      const listsResponse = deps.fetch('GET', '/lists');
      const lists =
        listsResponse && listsResponse.data && Array.isArray(listsResponse.data.lists)
          ? listsResponse.data.lists
          : [];

      if (!lists.length) {
        deps.log('ExternalListsService: aucune liste disponible.');
        return 'IGNORED';
      }

      const headers = snapshot.existingHeaders;
      const values = snapshot.rowEnCours;
      let updatePerformed = false;

      lists.forEach((liste) => {
        const nameParts = (liste.name || '').split(' || ');
        const listeFormId = nameParts.length > 1 ? nameParts[1] : '';
        if (!listeFormId || String(listeFormId) !== String(formulaire.id)) {
          return;
        }

        const detailResponse = deps.fetch('GET', `/lists/${liste.id}`);
        const detailList = detailResponse && detailResponse.data && detailResponse.data.list;
        if (!detailList || !Array.isArray(detailList.items) || !detailList.items.length) {
          deps.log(`ExternalListsService: détail liste ${liste.id} vide ou invalide.`);
          return;
        }

        const items = detailList.items.slice();
        const headerLine = items[0] ? items[0].split('|') : [];
        if (!headerLine.length) {
          deps.log(`ExternalListsService: entêtes liste ${liste.id} introuvables.`);
          return;
        }

        let shouldUpdate = false;
        for (let i = 1; i < headerLine.length; i++) {
          const variableParts = headerLine[i].split(':');
          const variable = variableParts[0];
          if (!variable) {
            continue;
          }
          const headerIndex = headers.indexOf(variable);
          if (headerIndex === -1) {
            deps.log(`ExternalListsService: variable ${variable} introuvable pour form ${formulaire.id}`);
            continue;
          }
          const rawValue = values[headerIndex];
          const safeValue = rawValue === undefined || rawValue === null ? '' : rawValue;
          replaceKizeoData(items, variable, safeValue);
          shouldUpdate = true;
        }

        if (shouldUpdate) {
          deps.fetch('PUT', `/lists/${liste.id}`, { items });
          updatePerformed = true;
        }
      });

      return updatePerformed ? 'Mise A Jour OK' : 'IGNORED';
    } catch (error) {
      deps.handleException('ExternalListsService.updateFromSnapshot', error, {
        formId: formulaire?.id || 'unknown'
      });
      return null;
    }
  }

  const service = {
    updateFromSnapshot: updateExternalListFromSnapshot,
    replaceItems: replaceKizeoData
  };

  global.ExternalListsService = service;
  if (typeof global.replaceKizeoData !== 'function') {
    global.replaceKizeoData = replaceKizeoData;
  }
})(this);
