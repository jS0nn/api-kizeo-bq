
const CONFIG_HEADERS = [
  'form_id',
  'form_name',
  'bq_table_name',
  'action',
  'last_data_id',
  'last_update_time',
  'last_answer_time',
  'last_run_at',
  'last_row_count',
  'trigger_frequency'
];
const CONFIG_KV_HEADER = ['key', 'value'];
const ACTION_CODE_PREFIX = 'act_';
const SUBFORM_FIELD_TYPES = ['subform', 'table', 'tableau', 'table_subform', 'table subform'];

function generateActionCode() {
  const timestamp = new Date().getTime().toString(36);
  const randomSegment = Math.random().toString(36).substring(2, 10);
  return `${ACTION_CODE_PREFIX}${timestamp}${randomSegment}`.substring(0, 30);
}

function normalizeSubformRows(rawValue) {
  if (!rawValue) return [];

  if (Array.isArray(rawValue)) {
    return rawValue
      .map((row) => normalizeSubformRow(row))
      .filter((row) => row && Object.keys(row).length);
  }

  if (typeof rawValue === 'object') {
    if (Array.isArray(rawValue.rows)) {
      return rawValue.rows
        .map((row) => normalizeSubformRow(row))
        .filter((row) => row && Object.keys(row).length);
    }
    if (Array.isArray(rawValue.data)) {
      return rawValue.data
        .map((row) => normalizeSubformRow(row))
        .filter((row) => row && Object.keys(row).length);
    }
    if (isLikelySubformRow(rawValue)) {
      const normalizedRow = normalizeSubformRow(rawValue);
      return Object.keys(normalizedRow).length ? [normalizedRow] : [];
    }
    return normalizeSubformRows(Object.values(rawValue));
  }

  if (typeof rawValue === 'string') {
    const trimmed = rawValue.trim();
    if (!trimmed) return [];
    try {
      const parsed = JSON.parse(trimmed);
      return normalizeSubformRows(parsed);
    } catch (e) {
      return [];
    }
  }

  return [];
}

function isSubformField(fieldType, fieldValue) {
  const normalizedType = (fieldType || '').toString().toLowerCase();
  if (SUBFORM_FIELD_TYPES.some((type) => normalizedType === type || normalizedType.indexOf(type) !== -1)) {
    return true;
  }

  const rows = normalizeSubformRows(fieldValue);
  return rows.length > 0;
}

function normalizeSubformRow(row) {
  if (!row || typeof row !== 'object') {
    return {};
  }

  const source = row.fields && typeof row.fields === 'object' ? row.fields : row;
  const normalized = {};

  Object.keys(source).forEach((key) => {
    const cell = source[key];
    if (cell && typeof cell === 'object' && cell.hasOwnProperty('value')) {
      normalized[key] = cell.value;
    } else {
      normalized[key] = cell;
    }
  });

  return normalized;
}

function isLikelySubformRow(obj) {
  if (!obj || typeof obj !== 'object') return false;
  if (obj.fields && typeof obj.fields === 'object') return true;
  return Object.keys(obj).some((key) => {
    const cell = obj[key];
    return cell && typeof cell === 'object' && cell.hasOwnProperty('value');
  });
}

function formSheetName(formulaire) {
  return `${formulaire.nom} || ${formulaire.id}`;
}

function normalizeFormId(formId) {
  return String(formId || '').trim();
}

function getFormSheetById(spreadsheet, formId) {
  const suffix = ` || ${normalizeFormId(formId)}`;
  const sheets = spreadsheet.getSheets();
  for (let i = 0; i < sheets.length; i++) {
    const name = sheets[i].getName();
    if (name.endsWith(suffix)) {
      return sheets[i];
    }
  }
  return null;
}

/**
 * Gère la création et la suppression des feuilles dans le fichier Google Sheets.
 *
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} spreadsheetBdD - Le fichier Google Sheets à gérer.
 * @param {Object} formulaire - L'objet contenant les informations du formulaire.
 * @return {boolean} Retourne 'true' si la feuille n'existe pas dans le fichier (ou vide), 'false' sinon.
 */
function gestionFeuilles(spreadsheetBdD, formulaire) {
  try {
    Logger.log('GestionFeuilles : ' + formulaire.nom);
    const targetName = formSheetName(formulaire);
    let targetSheet = spreadsheetBdD.getSheetByName(targetName);

    if (!targetSheet) {
      const sheets = spreadsheetBdD.getSheets();
      if (sheets.length === 1) {
        targetSheet = sheets[0];
        if (targetSheet.getName() !== targetName) {
          targetSheet.setName(targetName);
        }
      } else if (spreadsheetBdD.getSheetByName('Reinit')) {
        targetSheet = spreadsheetBdD.getSheetByName('Reinit');
        targetSheet.setName(targetName);
      } else {
        targetSheet = spreadsheetBdD.insertSheet(targetName);
        Logger.log('Ajout de la Feuille : ' + targetName);
      }
    }

    const sheets = spreadsheetBdD.getSheets();
    for (let i = 0; i < sheets.length; i++) {
      const sheet = sheets[i];
      if (sheet.getName() !== targetName) {
        spreadsheetBdD.deleteSheet(sheet);
      }
    }

    targetSheet.clear();
    targetSheet.getRange(1, 1, 1, CONFIG_KV_HEADER.length).setValues([CONFIG_KV_HEADER]);
    return targetSheet;
  } catch (e) {
    handleException('gestionFeuilles', e);
    return null;
  }
}

function readConfigFromSheet(sheet) {
  const config = {};
  if (!sheet) return config;
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return config;
  }
  const values = sheet.getRange(2, 1, lastRow - 1, CONFIG_KV_HEADER.length).getValues();
  for (let i = 0; i < values.length; i++) {
    const key = String(values[i][0] || '').trim();
    if (!key) continue;
    config[key] = values[i][1];
  }
  return config;
}

function writeConfigToSheet(sheet, config) {
  if (!sheet) return;
  const entries = new Map();
  Object.keys(config || {}).forEach((key) => {
    entries.set(key, config[key]);
  });

  entries.delete('bq_alias');

  const rows = [];
  CONFIG_HEADERS.forEach((header) => {
    rows.push([header, entries.has(header) ? entries.get(header) : '']);
    entries.delete(header);
  });
  entries.forEach((value, key) => {
    rows.push([key, value]);
  });

  sheet.clear();
  sheet.getRange(1, 1, 1, CONFIG_KV_HEADER.length).setValues([CONFIG_KV_HEADER]);
  if (rows.length) {
    sheet.getRange(2, 1, rows.length, CONFIG_KV_HEADER.length).setValues(rows);
  }
}

function getFormConfig(spreadsheet, formId) {
  const id = normalizeFormId(formId);
  if (!id) return null;
  const sheet = getFormSheetById(spreadsheet, id);
  if (!sheet) return null;
  const config = readConfigFromSheet(sheet);
  if (!Object.keys(config).length) return null;
  if (!config.form_id) {
    config.form_id = id;
  }
  if (!config.bq_table_name && config.bq_alias) {
    try {
      config.bq_table_name = bqComputeTableName(config.form_id, config.form_name || '', config.bq_alias);
    } catch (e) {
      console.log(`getFormConfig: conversion bq_alias vers bq_table_name échouée -> ${e}`);
    }
  }
  if (config.bq_alias) {
    delete config.bq_alias;
  }
  return config;
}

function ensureFormActionCode(spreadsheet, formId) {
  const id = normalizeFormId(formId);
  if (!id) {
    throw new Error('ensureFormActionCode: formId requis');
  }

  const existingConfig = getFormConfig(spreadsheet, id);
  if (existingConfig && existingConfig.action) {
    return existingConfig.action.toString().trim();
  }
  throw new Error(`ensureFormActionCode: aucune action disponible pour le formulaire ${id}`);
}

function upsertFormConfig(spreadsheet, config) {
  const formId = normalizeFormId(config.form_id || config.formId);
  if (!formId) {
    throw new Error('upsertFormConfig: form_id requis');
  }

  let sheet = getFormSheetById(spreadsheet, formId);
  if (!sheet) {
    const formName = config.form_name || config.formName || `Form ${formId}`;
    sheet = gestionFeuilles(spreadsheet, { nom: formName, id: formId });
  }

  const existing = readConfigFromSheet(sheet);
  const merged = Object.assign({}, existing);
  merged.form_id = formId;
  delete merged.max_unread_limit;
  CONFIG_HEADERS.forEach((header) => {
    if (config[header] !== undefined) {
      merged[header] = config[header];
    }
  });
  Object.keys(config).forEach((key) => {
    if (CONFIG_HEADERS.indexOf(key) === -1) {
      merged[key] = config[key];
    }
  });

  writeConfigToSheet(sheet, merged);
}

/**
 * Formats an object into a JSON-like string and sends it via email.
 * 
 * @param {Object} javascriptObject - The object to be formatted.
 * @param {string} functionName - The name of the calling function.
 * @param {string} context - Additionnal info to send.
 */
function emailLogger(javascriptObject, functionName = '', context = {}, fileName = 'data.json') {
  // Convert the javascriptObject to a JSON string with indentation for readability
  const jsonString = JSON.stringify(javascriptObject, null, 2);
  // Create a Blob from the JSON string
  const jsonBlob = Utilities.newBlob(jsonString, 'application/json', fileName);
  // Truncate the JSON string for the email body
  const truncatedJsonString = jsonString.substring(0, 500) + "...(truncated)";
  
  // Get the ID of the current script
  const scriptId = ScriptApp.getScriptId();
  // Build the script URL
  const scriptUrl = `https://script.google.com/d/${scriptId}/edit`;

  // Build the Debug email
  let subject= `Debug Json ${functionName}`
  let bodyMessage = `Debug in function ${functionName}, Please find the attached JSON file.\n\n`;
  bodyMessage += `Script URL : ${scriptUrl}\n\n\n`;
  bodyMessage += `500 premiers caracteres du Json : \n\n ${truncatedJsonString}\n\n\n`; 

  // Add context information, if available
  for (const [key, value] of Object.entries(context)) {
    bodyMessage += `${key} : ${value}\n`;
  }

  // Send the formatted JSON string by email
  MailApp.sendEmail({
    to: "jsonnier@sarpindustries.fr",
    subject: subject,
    body: bodyMessage,
    attachments: [jsonBlob]
  });
}

/**
 * Appliquer un format nombre sur toute la feuille
 */
function formatNumberAllSheets(spreadsheetBdD) {

  // Récupérer toutes les feuilles du classeur
  let sheets = spreadsheetBdD.getSheets();
  
  // Appliquer le format numérique à toutes les cellules de toutes les feuilles
  for (let i = 0; i < sheets.length; i++) {
    let sheet = sheets[i];
    let range = sheet.getDataRange();
    range.setNumberFormat("#,###.##########"); // Permet de conserver jusqu'à 10 décimales significatives
  }

  Logger.log('Le format a été appliqué à toutes les feuilles.');
}

/**
 * Fonction pour réduire la taille d'un JSON en limitant le nombre d'éléments des tableaux
 * @param {Object} jsonObj - Le JSON à réduire
 * @param {number} nbMaxTab - Nombre maximum d'éléments par tableau
 * @return {Object} - Le JSON réduit
 */
function reduireJSON2(jsonObj, nbMaxTab) {
  // Fonction récursive pour parcourir et réduire le JSON
  function reduire(obj) {
    if (Array.isArray(obj)) {
      // Si c'est un tableau, on le tronque et on applique la réduction à ses éléments
      return obj.slice(0, nbMaxTab).map(reduire);
    } else if (typeof obj === 'object' && obj !== null) {
      // Si c'est un objet, on parcourt ses propriétés
      var newObj = {};
      for (var key in obj) {
        if (obj.hasOwnProperty(key)) {
          newObj[key] = reduire(obj[key]);
        }
      }
      return newObj;
    } else {
      // Pour les types primitifs, on retourne la valeur telle quelle
      return obj;
    }
  }
  // Appel initial de la fonction récursive
  return reduire(jsonObj);
}

/**
 * Fonction pour réduire la taille d'un JSON en limitant le nombre d'éléments des tableaux et des objets spécifiques
 * @param {Object} jsonObj - Le JSON à réduire
 * @param {Object} limites - Objet contenant nbMaxTab et listeObjAReduire
 * @return {Object} - Le JSON réduit
 */
function reduireJSON(jsonObj, limites) {
  // Fonction récursive pour parcourir et réduire le JSON
  function reduire(obj, key) {
    if (Array.isArray(obj)) {
      // Si c'est un tableau, on le tronque et on applique la réduction à ses éléments
      return obj.slice(0, limites.nbMaxTab).map(function(item) {
        return reduire(item, null); // Pas de clé pour les éléments du tableau
      });
    } else if (typeof obj === 'object' && obj !== null) {
      // Si c'est un objet
      var newObj = {};
      var keys = Object.keys(obj);
      
      if (key && limites.listeObjAReduire && limites.listeObjAReduire.hasOwnProperty(key)) {
        // Si l'objet est dans listeObjAReduire, on limite le nombre de propriétés
        var maxProps = limites.listeObjAReduire[key];
        keys = keys.slice(0, maxProps);
      }
      
      keys.forEach(function(k) {
        newObj[k] = reduire(obj[k], k);
      });
      return newObj;
    } else {
      // Pour les types primitifs, on retourne la valeur telle quelle
      return obj;
    }
  }
  // Appel initial de la fonction récursive
  return reduire(jsonObj, null);
}


/**
 * Détermine si la valeur est numérique.
 *
 * @param {any} value - La valeur à vérifier.
 * @return {boolean} Retourne true si la valeur est numérique, sinon false.
 */
function isNumeric(value) {
  return !isNaN(parseFloat(value)) && isFinite(value);
}
