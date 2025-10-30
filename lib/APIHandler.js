
const KIZEO_TOKEN_PROPERTY_KEY = 'KIZEO_API_TOKEN';
const KIZEO_TOKEN_SPREADSHEET_ID = '1CtkKyPck3rZ97AbRevzNGiRojranofxMz28zmtSP4LI';
const KIZEO_TOKEN_SHEET_NAME = 'token';
const KIZEO_API_BASE_URL = 'https://www.kizeoforms.com/rest/v3';
const API_HANDLER_LOG_PREFIX = 'lib:APIHandler:requeteAPIDonnees';

function getKizeoTokenStore() {
  try {
    return PropertiesService.getDocumentProperties();
  } catch (e) {
    console.log(`${API_HANDLER_LOG_PREFIX}: DocumentProperties indisponibles, fallback ScriptProperties (${e})`);
    return PropertiesService.getScriptProperties();
  }
}

function readTokenFromSheet() {
  try {
    const spreadsheet = SpreadsheetApp.openById(KIZEO_TOKEN_SPREADSHEET_ID);
    const sheet = spreadsheet.getSheetByName(KIZEO_TOKEN_SHEET_NAME);
    if (!sheet) {
      throw new Error(`Onglet ${KIZEO_TOKEN_SHEET_NAME} introuvable dans le classeur token`);
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

function refreshTokenFromSheet(store) {
  const newToken = readTokenFromSheet();
  if (newToken) {
    try {
      store.setProperty(KIZEO_TOKEN_PROPERTY_KEY, newToken);
      console.log(`${API_HANDLER_LOG_PREFIX}: token rafraîchi depuis le classeur.`);
    } catch (e) {
      handleException('requeteApiDonnees.persistToken', e);
    }
  }
  return newToken;
}

function buildRequestSettings(methode, payload, token) {
  const normalizedMethod = (methode || 'GET').toUpperCase();
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

function parseKizeoResponse(response) {
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

function shouldRefreshToken(code) {
  return code === 401 || code === 403;
}

/**
 * Envoie une requête HTTP à l'API Kizeo et renvoie la réponse.
 *
 * @param {string} methode - La méthode HTTP à utiliser pour la requête.
 * @param {string} type - Le chemin d'accès à l'API.
 * @param {Object} donnees - Les données à inclure dans le corps de la requête (optionnel).
 * @return {{data: *, responseCode: number|null, headers:Object|undefined, error:Object|null}}
 */
function requeteAPIDonnees(methode, type, donnees) {
  const store = getKizeoTokenStore();
  let token = String(store.getProperty(KIZEO_TOKEN_PROPERTY_KEY) || '').trim();
  if (!token) {
    token = refreshTokenFromSheet(store) || '';
  }

  const serializedPayload = donnees === undefined ? null : donnees;
  let refreshPerformed = false;
  let lastError = null;
  let responseDetails = null;

  const execute = (currentToken) => {
    const requestSettings = buildRequestSettings(methode, serializedPayload, currentToken);
    try {
      const httpResponse = UrlFetchApp.fetch(`${KIZEO_API_BASE_URL}${type}`, requestSettings);
      return parseKizeoResponse(httpResponse);
    } catch (e) {
      lastError = e;
      return null;
    }
  };

  if (!token) {
    lastError = new Error('Token Kizeo inexistant');
  } else {
    responseDetails = execute(token);
  }

  if (
    (!responseDetails && !refreshPerformed) ||
    (responseDetails && shouldRefreshToken(responseDetails.responseCode) && !refreshPerformed)
  ) {
    const refreshedToken = refreshTokenFromSheet(store);
    refreshPerformed = true;
    if (refreshedToken) {
      token = refreshedToken;
      responseDetails = execute(token);
    }
  }

  if (!responseDetails) {
    handleException('requeteApiDonnees', lastError || new Error('Réponse Kizeo vide'), {
      methode,
      type
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
  handleException('requeteApiDonnees', new Error(errorMessage), {
    methode,
    type,
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
