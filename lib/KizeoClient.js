(function (global) {
  const LOG_PREFIX = 'lib:APIHandler:requeteAPIDonnees';
  const TOKEN_PROPERTY_KEY = 'KIZEO_API_TOKEN';
  const TOKEN_SPREADSHEET_ID = '1CtkKyPck3rZ97AbRevzNGiRojranofxMz28zmtSP4LI';
  const TOKEN_SHEET_NAME = 'token';
  const API_BASE_URL = 'https://www.kizeoforms.com/rest/v3';
  const MEMORY_CACHE_TTL_MS = 10 * 60 * 1000;

  const memoryTokenCache = {
    value: null,
    expiresAt: 0
  };

  function createTokenStore() {
    try {
      return PropertiesService.getDocumentProperties();
    } catch (e) {
      console.log(`${LOG_PREFIX}: DocumentProperties indisponibles, fallback ScriptProperties (${e})`);
      return PropertiesService.getScriptProperties();
    }
  }

  function readTokenFromSheet() {
    try {
      const spreadsheet = SpreadsheetApp.openById(TOKEN_SPREADSHEET_ID);
      const sheet = spreadsheet.getSheetByName(TOKEN_SHEET_NAME);
      if (!sheet) {
        throw new Error(`Onglet ${TOKEN_SHEET_NAME} introuvable dans le classeur token`);
      }
      const token = String(sheet.getRange(1, 1).getValue() || '').trim();
      if (!token) {
        throw new Error('Token Kizeo vide dans le classeur');
      }
      return token;
    } catch (e) {
      handleException('requeteApiDonnees.fetchTokenSheet', e);
      return null;
    }
  }

  function getTokenFromMemory() {
    if (!memoryTokenCache.value) {
      return null;
    }
    if (memoryTokenCache.expiresAt && Date.now() > memoryTokenCache.expiresAt) {
      memoryTokenCache.value = null;
      memoryTokenCache.expiresAt = 0;
      return null;
    }
    return memoryTokenCache.value;
  }

  function setTokenInMemory(token) {
    if (!token) {
      memoryTokenCache.value = null;
      memoryTokenCache.expiresAt = 0;
      return;
    }
    memoryTokenCache.value = token;
    memoryTokenCache.expiresAt = Date.now() + MEMORY_CACHE_TTL_MS;
  }

  function resetTokenCache() {
    memoryTokenCache.value = null;
    memoryTokenCache.expiresAt = 0;
  }

  function invalidateTokenCache(store, reason, options) {
    resetTokenCache();
    const opts = options || {};
    if (opts.preserveProperty) {
      return;
    }
    if (!store || typeof store.deleteProperty !== 'function') {
      return;
    }
    try {
      store.deleteProperty(TOKEN_PROPERTY_KEY);
      if (reason) {
        console.log(`${LOG_PREFIX}: cache token invalidé (${reason}).`);
      }
    } catch (e) {
      handleException('requeteAPIDonnees.invalidateTokenCache', e, { reason: reason || 'unknown' });
    }
  }

  function refreshToken(store) {
    const newToken = readTokenFromSheet();
    if (newToken) {
      try {
        store.setProperty(TOKEN_PROPERTY_KEY, newToken);
        console.log(`${LOG_PREFIX}: token rafraîchi depuis le classeur.`);
        setTokenInMemory(newToken);
      } catch (e) {
        handleException('requeteApiDonnees.persistToken', e);
      }
    } else {
      resetTokenCache();
    }
    return newToken;
  }

  function buildRequestSettings(method, payload, token) {
    const normalizedMethod = (method || 'GET').toUpperCase();
    const bodyCandidate = payload === undefined ? null : payload;
    const hasBody = normalizedMethod !== 'GET' && bodyCandidate !== null && bodyCandidate !== undefined;
    const settings = {
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
    const responseCode = response.getResponseCode();
    const headers = response.getHeaders() || {};
    const contentTypeRaw = headers['Content-Type'] || headers['content-type'] || '';
    const contentType = contentTypeRaw.split(';')[0].trim().toLowerCase();
    let data = null;

    if (contentType === 'image/jpeg' || contentType === 'image/png' || contentType === 'application/pdf') {
      data = response.getBlob();
    } else if (contentType.indexOf('application/json') === 0 || contentType.indexOf('text/json') === 0) {
      try {
        data = JSON.parse(response.getContentText());
      } catch (e) {
        handleException('requeteApiDonnees.parseJson', e, { contentType: contentTypeRaw });
        data = null;
      }
    } else {
      data = response.getContentText();
    }

    return { responseCode, data, headers, contentType: contentTypeRaw, rawResponse: response };
  }

  function shouldRefresh(code) {
    return code === 401 || code === 403;
  }

  function requeteAPIDonnees(method, path, payload) {
    const store = createTokenStore();
    let token = getTokenFromMemory();
    if (!token) {
      try {
        const storedToken = String(store.getProperty(TOKEN_PROPERTY_KEY) || '').trim();
        if (storedToken) {
          token = storedToken;
          setTokenInMemory(token);
        }
      } catch (e) {
        handleException('requeteAPIDonnees.readTokenProperty', e);
      }
    }
    if (!token) {
      token = refreshToken(store) || '';
    }

    const serializedPayload = payload === undefined ? null : payload;
    let refreshPerformed = false;
    let lastError = null;
    let responseDetails = null;
    let networkFailure = false;
    let refreshedAfterFailure = false;

    const execute = (currentToken) => {
      const requestSettings = buildRequestSettings(method, serializedPayload, currentToken);
      try {
        const httpResponse = UrlFetchApp.fetch(`${API_BASE_URL}${path}`, requestSettings);
        return parseResponse(httpResponse);
      } catch (e) {
        lastError = e;
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
      const refreshedToken = refreshToken(store);
      refreshPerformed = true;
      if (refreshedToken) {
        token = refreshedToken;
        responseDetails = execute(token);
      }
    } else if (!responseDetails && networkFailure && !refreshPerformed) {
      const refreshedToken = refreshToken(store);
      refreshPerformed = true;
      if (refreshedToken) {
        refreshedAfterFailure = true;
      }
    }

    if (!responseDetails) {
      invalidateTokenCache(store, 'fetch-error', { preserveProperty: refreshedAfterFailure });
      handleException('requeteAPIDonnees', lastError || new Error('Réponse Kizeo vide'), {
        methode: method,
        type: path
      });
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

    const { responseCode, data, headers, rawResponse } = responseDetails;
    if (responseCode >= 200 && responseCode < 300) {
      return {
        data,
        responseCode,
        headers,
        error: null
      };
    }

    const errorBody = rawResponse ? rawResponse.getContentText() : null;
    const errorMessage = `Erreur HTTP: ${responseCode}`;
    if (!shouldRefresh(responseCode)) {
      invalidateTokenCache(store, `http-${responseCode}`);
    }
    handleException('requeteAPIDonnees', new Error(errorMessage), {
      methode: method,
      type: path,
      responseCode,
      body: errorBody ? errorBody.substring(0, 500) : ''
    });

    return {
      data: null,
      responseCode,
      headers,
      error: {
        message: errorMessage,
        body: errorBody
      }
    };
  }

  const client = {
    requeteAPIDonnees,
    __resetTokenCacheForTests: resetTokenCache
  };

  global.KizeoClient = client;
  if (typeof global.requeteAPIDonnees !== 'function') {
    global.requeteAPIDonnees = client.requeteAPIDonnees;
  }
})(this);
