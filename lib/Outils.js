
const CONFIG_BATCH_LIMIT_KEY = 'batch_limit';
const CONFIG_INGEST_BIGQUERY_KEY = 'ingest_bigquery';

const CONFIG_HEADERS = [
  'form_id',
  'form_name',
  'bq_table_name',
  'action',
  CONFIG_BATCH_LIMIT_KEY,
  CONFIG_INGEST_BIGQUERY_KEY,
  'last_data_id',
  'last_update_time',
  'last_answer_time',
  'last_run_at',
  'last_saved_row_count',
  'last_run_duration_s',
  'trigger_frequency'
];
const CONFIG_KV_HEADER = ['Paramètre', 'Valeur'];
const SUBFORM_FIELD_TYPES = ['subform', 'table', 'tableau', 'table_subform', 'table subform'];

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

/**
 * @deprecated Utiliser les nouvelles APIs d'orchestration (`ProcessManager`) pour récupérer l'action.
 */
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
/**
 * Détermine si la valeur est numérique.
 *
 * @param {any} value - La valeur à vérifier.
 * @return {boolean} Retourne true si la valeur est numérique, sinon false.
 */
function isNumeric(value) {
  return !isNaN(parseFloat(value)) && isFinite(value);
}
