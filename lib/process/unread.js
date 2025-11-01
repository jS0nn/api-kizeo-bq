function resolveUnreadDataset(fetchFn, formulaire, apiPath, hasPreviousRun, prefetchedPayload, log) {
  var describePayload = function (payload) {
    if (!payload) {
      return { hasData: false, type: 'undefined', keys: [], isArray: false, status: 'n/a', length: 'n/a' };
    }
    var dataProp = payload.data;
    return {
      hasData: !!payload,
      type: typeof payload,
      keys: typeof payload === 'object' ? Object.keys(payload).slice(0, 10) : [],
      isArray: Array.isArray(payload),
      status: payload.status || 'n/a',
      length: Array.isArray(dataProp) ? dataProp.length : 'n/a'
    };
  };

  var getArrayFromPayload = function (payload) {
    if (!payload) return null;
    if (Array.isArray(payload.data)) return payload.data;
    if (Array.isArray(payload)) return payload;
    return null;
  };

  var unreadPayload = prefetchedPayload || null;
  var unreadResponseCode = null;
  var requester = typeof fetchFn === 'function' ? fetchFn : requeteAPIDonnees;
  var prefix = processGetLogPrefix();

  if (!unreadPayload) {
    var unreadResponse = requester('GET', apiPath);
    unreadPayload = unreadResponse ? unreadResponse.data : null;
    unreadResponseCode = unreadResponse ? unreadResponse.responseCode : null;
  }

  var unreadInfo = describePayload(unreadPayload);
  log(
    prefix +
      ': analyse réponse unread -> hasData=' +
      unreadInfo.hasData +
      ', type=' +
      unreadInfo.type +
      ', keys=' +
      unreadInfo.keys +
      ', isArray=' +
      unreadInfo.isArray +
      ', status=' +
      unreadInfo.status +
      ', length=' +
      unreadInfo.length
  );

  var unreadArray = getArrayFromPayload(unreadPayload);
  if (!unreadArray) {
    log(
      prefix +
        ': réponse \"unread\" inattendue (status=' +
        unreadInfo.status +
        ', code=' +
        (unreadResponseCode || 'n/a') +
        ')'
    );
    return {
      type: 'INVALID',
      payload: unreadPayload
    };
  }

  if (unreadArray.length) {
    return {
      type: 'OK',
      payload: unreadPayload
    };
  }

  if (hasPreviousRun) {
    log(prefix + ': aucune donnée non lue pour form=' + formulaire.id + '.');
    return {
      type: 'NO_UNREAD',
      payload: unreadPayload
    };
  }

  log(
    prefix + ': aucune donnée non lue pour form=' + formulaire.id + '. Tentative de chargement complet via data/all.'
  );
  var fullResponse = requester('GET', '/forms/' + formulaire.id + '/data/all');
  var fallbackPayload = fullResponse ? fullResponse.data : null;
  var fallbackArray = getArrayFromPayload(fallbackPayload) || [];

  if (!fallbackArray.length) {
    var fallbackInfo = describePayload(fallbackPayload);
    log(
      prefix +
        ': fallback data/all sans enregistrements (status=' +
        fallbackInfo.status +
        '). keys=' +
        fallbackInfo.keys
    );
    return {
      type: 'FALLBACK_EMPTY',
      payload: fallbackPayload
    };
  }

  var fallbackInfo = describePayload(fallbackPayload);
  log(
    prefix +
      ': récupération fallback réussie (' +
      fallbackArray.length +
      ' enregistrements via data/all). Status=' +
      fallbackInfo.status +
      ', type=' +
      fallbackInfo.type +
      ', keys=' +
      fallbackInfo.keys
  );
  if (fallbackArray[0] && typeof fallbackArray[0] === 'object') {
    var firstKeys = Object.keys(fallbackArray[0]).slice(0, 15);
    log(prefix + ': aperçu fallback[0] keys=' + firstKeys);
  }

  return {
    type: 'FALLBACK_OK',
    payload: {
      status: fallbackPayload && fallbackPayload.status ? fallbackPayload.status : 'fallback_all',
      data: fallbackArray
    }
  };
}
