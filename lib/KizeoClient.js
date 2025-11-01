// KizeoClient Version 0.3.0
var KIZEO_LOG_PREFIX = 'lib:APIHandler:requeteAPIDonnees';
var KIZEO_TOKEN_PROPERTY_KEY = 'KIZEO_API_TOKEN';
var KIZEO_TOKEN_SPREADSHEET_ID = '1CtkKyPck3rZ97AbRevzNGiRojranofxMz28zmtSP4LI';
var KIZEO_TOKEN_SHEET_NAME = 'token';
var KIZEO_API_BASE_URL = 'https://www.kizeoforms.com/rest/v3';
var KIZEO_MEMORY_CACHE_TTL_MS = 10 * 60 * 1000;

var kizeoMemoryTokenCache = {
  value: null,
  expiresAt: 0
};

function createTokenStore() {
  try {
    return PropertiesService.getDocumentProperties();
  } catch (error) {
    console.log(KIZEO_LOG_PREFIX + ': DocumentProperties indisponibles, fallback ScriptProperties (' + error + ')');
    return PropertiesService.getScriptProperties();
  }
}

function readTokenFromSheet() {
  try {
    var spreadsheet = SpreadsheetApp.openById(KIZEO_TOKEN_SPREADSHEET_ID);
    var sheet = spreadsheet.getSheetByName(KIZEO_TOKEN_SHEET_NAME);
    if (!sheet) {
      throw new Error('Onglet ' + KIZEO_TOKEN_SHEET_NAME + ' introuvable dans le classeur token');
    }
    var token = String(sheet.getRange(1, 1).getValue() || '').trim();
    if (!token) {
      throw new Error('Token Kizeo vide dans le classeur');
    }
    return token;
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('requeteApiDonnees.fetchTokenSheet', error);
    }
    return null;
  }
}

function getTokenFromMemory() {
  if (!kizeoMemoryTokenCache.value) {
    return null;
  }
  if (kizeoMemoryTokenCache.expiresAt && Date.now() > kizeoMemoryTokenCache.expiresAt) {
    kizeoMemoryTokenCache.value = null;
    kizeoMemoryTokenCache.expiresAt = 0;
    return null;
  }
  return kizeoMemoryTokenCache.value;
}

function setTokenInMemory(token) {
  if (!token) {
    kizeoMemoryTokenCache.value = null;
    kizeoMemoryTokenCache.expiresAt = 0;
    return;
  }
  kizeoMemoryTokenCache.value = token;
  kizeoMemoryTokenCache.expiresAt = Date.now() + KIZEO_MEMORY_CACHE_TTL_MS;
}

function resetTokenCache() {
  kizeoMemoryTokenCache.value = null;
  kizeoMemoryTokenCache.expiresAt = 0;
}

function invalidateTokenCache(store, reason, options) {
  resetTokenCache();
  var opts = options || {};
  if (opts.preserveProperty) {
    return;
  }
  if (!store || typeof store.deleteProperty !== 'function') {
    return;
  }
  try {
    store.deleteProperty(KIZEO_TOKEN_PROPERTY_KEY);
    if (reason) {
      console.log(KIZEO_LOG_PREFIX + ': cache token invalidé (' + reason + ').');
    }
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('requeteAPIDonnees.invalidateTokenCache', error, { reason: reason || 'unknown' });
    }
  }
}

function refreshToken(store) {
  var newToken = readTokenFromSheet();
  if (newToken) {
    try {
      store.setProperty(KIZEO_TOKEN_PROPERTY_KEY, newToken);
      console.log(KIZEO_LOG_PREFIX + ': token rafraîchi depuis le classeur.');
      setTokenInMemory(newToken);
    } catch (error) {
      if (typeof handleException === 'function') {
        handleException('requeteApiDonnees.persistToken', error);
      }
    }
  } else {
    resetTokenCache();
  }
  return newToken;
}

function buildRequestSettings(method, payload, token) {
  var normalizedMethod = (method || 'GET').toUpperCase();
  var bodyCandidate = payload === undefined ? null : payload;
  var hasBody = normalizedMethod !== 'GET' && bodyCandidate !== null && bodyCandidate !== undefined;
  var settings = {
    async: true,
    crossDomain: true,
    method: normalizedMethod,
    headers: {
      'content-type': 'application/json',
      authorization: token,
      'cache-control': 'no-cache'
    },
    muteHttpExceptions: true
  };
  if (hasBody) {
    settings.payload = typeof bodyCandidate === 'string' ? bodyCandidate : JSON.stringify(bodyCandidate || {});
  }
  return settings;
}

function parseResponse(response) {
  var responseCode = response.getResponseCode();
  var headers = response.getHeaders() || {};
  var contentTypeRaw = headers['Content-Type'] || headers['content-type'] || '';
  var contentType = contentTypeRaw.split(';')[0].trim().toLowerCase();
  var data = null;

  if (contentType === 'image/jpeg' || contentType === 'image/png' || contentType === 'application/pdf') {
    data = response.getBlob();
  } else if (contentType.indexOf('application/json') === 0 || contentType.indexOf('text/json') === 0) {
    try {
      data = JSON.parse(response.getContentText());
    } catch (error) {
      if (typeof handleException === 'function') {
        handleException('requeteApiDonnees.parseJson', error, { contentType: contentTypeRaw });
      }
      data = null;
    }
  } else {
    data = response.getContentText();
  }

  return { responseCode: responseCode, data: data, headers: headers, contentType: contentTypeRaw, rawResponse: response };
}

function shouldRefresh(code) {
  return code === 401 || code === 403;
}

function requeteAPIDonnees(method, path, payload) {
  var store = createTokenStore();
  var token = getTokenFromMemory();
  if (!token) {
    try {
      var storedToken = String(store.getProperty(KIZEO_TOKEN_PROPERTY_KEY) || '').trim();
      if (storedToken) {
        token = storedToken;
        setTokenInMemory(token);
      }
    } catch (error) {
      if (typeof handleException === 'function') {
        handleException('requeteAPIDonnees.readTokenProperty', error);
      }
    }
  }
  if (!token) {
    token = refreshToken(store) || '';
  }

  var serializedPayload = payload === undefined ? null : payload;
  var refreshPerformed = false;
  var lastError = null;
  var responseDetails = null;
  var networkFailure = false;
  var refreshedAfterFailure = false;

  var execute = function (currentToken) {
    var requestSettings = buildRequestSettings(method, serializedPayload, currentToken);
    try {
      var httpResponse = UrlFetchApp.fetch(KIZEO_API_BASE_URL + path, requestSettings);
      return parseResponse(httpResponse);
    } catch (error) {
      lastError = error;
      networkFailure = true;
      return null;
    }
  };

  if (!token) {
    lastError = new Error('Token Kizeo inexistant');
  } else {
    responseDetails = execute(token);
  }

  if (responseDetails && shouldRefresh(responseDetails.responseCode) && !refreshPerformed) {
    var refreshedToken = refreshToken(store);
    refreshPerformed = true;
    if (refreshedToken) {
      token = refreshedToken;
      responseDetails = execute(token);
    }
  } else if (!responseDetails && networkFailure && !refreshPerformed) {
    var refreshedAfterError = refreshToken(store);
    refreshPerformed = true;
    if (refreshedAfterError) {
      refreshedAfterFailure = true;
    }
  }

  if (!responseDetails) {
    invalidateTokenCache(store, 'fetch-error', { preserveProperty: refreshedAfterFailure });
    if (typeof handleException === 'function') {
      handleException('requeteAPIDonnees', lastError || new Error('Réponse Kizeo vide'), {
        methode: method,
        type: path
      });
    }
    return {
      data: null,
      responseCode: null,
      headers: undefined,
      error: {
        message: lastError && lastError.message ? lastError.message : 'Erreur requête Kizeo',
        cause: lastError ? String(lastError) : 'UNKNOWN'
      }
    };
  }

  var responseCode = responseDetails.responseCode;
  var data = responseDetails.data;
  var headers = responseDetails.headers;
  var rawResponse = responseDetails.rawResponse;

  if (responseCode >= 200 && responseCode < 300) {
    return {
      data: data,
      responseCode: responseCode,
      headers: headers,
      error: null
    };
  }

  var errorBody = rawResponse ? rawResponse.getContentText() : null;
  var errorMessage = 'Erreur HTTP: ' + responseCode;
  if (!shouldRefresh(responseCode)) {
    invalidateTokenCache(store, 'http-' + responseCode);
  }
  if (typeof handleException === 'function') {
    handleException('requeteAPIDonnees', new Error(errorMessage), {
      methode: method,
      type: path,
      responseCode: responseCode,
      body: errorBody ? errorBody.substring(0, 500) : ''
    });
  }

  return {
    data: null,
    responseCode: responseCode,
    headers: headers,
    error: {
      message: errorMessage,
      body: errorBody
    }
  };
}

var KizeoClient = {
  requeteAPIDonnees: requeteAPIDonnees,
  __resetTokenCacheForTests: resetTokenCache
};

this.requeteAPIDonnees = requeteAPIDonnees;
this.KizeoClient = KizeoClient;
