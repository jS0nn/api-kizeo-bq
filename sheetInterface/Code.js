//Version 4.0

/**    DOC :
  https://www.kizeoforms.com/doc/swagger/v3/#/
    types GET: 
      /users : get all users
      /forms : list all forms
      /forms/{formId} : Get form definition
      /forms/{formId}/data : Get the list of all data of a form (not read)
      /forms/{formId}/data/all :Get the list of all data of a form
      /forms/{formId}/data/readnew : Get content of unread data
      /forms/{formId}/data/{dataId} : Get data of a form
      /forms/push/inbox : Receive new pushed data
      /forms/{formId}/data/{dataId}/pdf : Get PDF data of a form
      /forms/{formId}/exports : Get list of Word and Excel exports
      /forms/{formId}/data/{dataId}/exports/{exportId} : Export data
      /forms/{formId}/data/{dataId}/exports/{exportId}/pdf : Export data (PDF)
      /lists : Get External Lists
      /lists/{listId} : Get External List Definition
      /lists/{listId}/complete : Get External List Definition (Without taking in account filters)
      groups...
*/


  /**
  TODO : 
  - gestion des codes dans les formulaires kizeo
  - gestion des champs cases à choix multiples


  ⚠⚠⚠ Attention aux champs cases à choix multiples  (doit on concatainer les réponses ou créer un nouvel onglet ?) ⚠⚠⚠

  */

// Paramétrage de la limite d'ingestion Kizeo
const DEFAULT_KIZEO_BATCH_LIMIT = 30;
const CONFIG_BATCH_LIMIT_KEY = 'batch_limit';

function sanitizeBatchLimitValue(raw) {
  if (raw === null || raw === undefined) return null;
  const numeric = Number(raw);
  if (!Number.isFinite(numeric)) return null;
  const floored = Math.floor(numeric);
  if (floored <= 0) return null;
  return floored;
}

function getConfiguredBatchLimit(config) {
  const raw =
    config && typeof config === 'object' && CONFIG_BATCH_LIMIT_KEY in config ? config[CONFIG_BATCH_LIMIT_KEY] : null;
  const sanitized = sanitizeBatchLimitValue(raw);
  if (sanitized !== null) return sanitized;
  return DEFAULT_KIZEO_BATCH_LIMIT;
}

// Configuration des déclencheurs temporels
const TRIGGER_DISABLED_KEY = 'none';
const DEFAULT_TRIGGER_FREQUENCY = 'H24';
const TRIGGER_FREQUENCY_PROPERTY = 'TRIGGER_FREQUENCY';
const TRIGGER_OPTIONS = {
  M1: { type: 'M', value: 1, label: 'Toutes les minutes' },
  M10: { type: 'M', value: 10, label: 'Toutes les 10 minutes' },
  M30: { type: 'M', value: 30, label: 'Toutes les 30 minutes' },
  H1: { type: 'H', value: 1, label: 'Toutes les heures' },
  H3: { type: 'H', value: 3, label: 'Toutes les 3 heures' },
  H6: { type: 'H', value: 6, label: 'Toutes les 6 heures' },
  H24: { type: 'H', value: 24, label: 'Une fois par jour' },
  WD1: { type: 'D', value: 7, label: 'Une fois par semaine' }
};
const DAILY_CUSTOM_TRIGGER_PATTERN = /^H24@([01]\d|2[0-3])$/;
const WEEKLY_CUSTOM_TRIGGER_PATTERN = /^WD1@([A-Z]{3})@([01]\d|2[0-3])$/;
const WEEKDAY_CODE_MAP = {
  MON: { scriptEnum: ScriptApp.WeekDay.MONDAY, label: 'lundi' },
  TUE: { scriptEnum: ScriptApp.WeekDay.TUESDAY, label: 'mardi' },
  WED: { scriptEnum: ScriptApp.WeekDay.WEDNESDAY, label: 'mercredi' },
  THU: { scriptEnum: ScriptApp.WeekDay.THURSDAY, label: 'jeudi' },
  FRI: { scriptEnum: ScriptApp.WeekDay.FRIDAY, label: 'vendredi' },
  SAT: { scriptEnum: ScriptApp.WeekDay.SATURDAY, label: 'samedi' },
  SUN: { scriptEnum: ScriptApp.WeekDay.SUNDAY, label: 'dimanche' }
};
const CONFIG_HEADERS = [
  'form_id',
  'form_name',
  'bq_table_name',
  'action',
  CONFIG_BATCH_LIMIT_KEY,
  'last_data_id',
  'last_update_time',
  'last_answer_time',
  'last_run_at',
  'last_saved_row_count',
  'last_run_duration_s',
  'trigger_frequency'
];
const REQUIRED_CONFIG_KEYS = ['form_id', 'form_name', 'action'];
const MAX_BQ_TABLE_NAME_LENGTH = 128;

function sanitizeTriggerFrequency(raw) {
  if (raw === null || raw === undefined) return DEFAULT_TRIGGER_FREQUENCY;
  const stringValue = raw.toString().trim();
  if (!stringValue) return DEFAULT_TRIGGER_FREQUENCY;
  const lower = stringValue.toLowerCase();
  if (lower === TRIGGER_DISABLED_KEY) return TRIGGER_DISABLED_KEY;
  const upper = stringValue.toUpperCase();
  if (TRIGGER_OPTIONS[upper]) return upper;
  if (DAILY_CUSTOM_TRIGGER_PATTERN.test(upper)) return upper;
  if (WEEKLY_CUSTOM_TRIGGER_PATTERN.test(upper)) return upper;
  return DEFAULT_TRIGGER_FREQUENCY;
}

function getTriggerOption(key) {
  if (!key) return null;
  if (TRIGGER_OPTIONS[key]) {
    return TRIGGER_OPTIONS[key];
  }
  const customHour = parseCustomDailyHour(key);
  if (customHour !== null) {
    return {
      type: 'CUSTOM_DAILY',
      value: 24,
      label: `Chaque jour à ${formatHourLabel(customHour)}`,
      hour: customHour
    };
  }
  const customWeekly = parseCustomWeekly(key);
  if (customWeekly) {
    return {
      type: 'CUSTOM_WEEKLY',
      value: 7,
      label: `Chaque semaine le ${formatWeekdayLabel(customWeekly.dayCode)} à ${formatHourLabel(customWeekly.hour)}`,
      dayCode: customWeekly.dayCode,
      hour: customWeekly.hour
    };
  }
  return null;
}

function describeTriggerOption(key) {
  if (!key) return 'inconnue';
  if (key === TRIGGER_DISABLED_KEY) return 'désactivée';
  const option = getTriggerOption(key);
  if (!option) return key;
  if (option.type === 'CUSTOM_DAILY' && typeof option.hour === 'number') {
    return option.label;
  }
  if (option.type === 'CUSTOM_WEEKLY' && typeof option.hour === 'number') {
    return option.label;
  }
  const unit = option.type === 'M' ? 'minute' : 'heure';
  const plural = option.value > 1 ? 's' : '';
  return `${option.value} ${unit}${plural}`;
}

function configureTriggerFromKey(key) {
  if (key === TRIGGER_DISABLED_KEY) {
    const mainHandler = typeof MAIN_TRIGGER_FUNCTION === 'undefined' ? 'main' : MAIN_TRIGGER_FUNCTION;
    const dedupHandler =
      typeof DEDUP_TRIGGER_FUNCTION === 'undefined' ? 'runBigQueryDeduplication' : DEDUP_TRIGGER_FUNCTION;
    deleteTriggersByFunction(mainHandler);
    deleteTriggersByFunction(dedupHandler);
    console.log('Déclencheurs automatiques désactivés.');
    return null;
  }
  const option = getTriggerOption(key);
  if (!option) {
    throw new Error(`Fréquence de déclencheur inconnue: ${key}`);
  }
  if (option.type === 'CUSTOM_DAILY') {
    configurerDeclencheurQuotidienAvecHeure(option.hour);
  } else if (option.type === 'CUSTOM_WEEKLY') {
    configurerDeclencheurHebdomadaire(option.dayCode, option.hour);
  } else {
    configurerDeclencheurHoraire(option.value, option.type);
  }
  ensureDeduplicationTrigger();
  return option;
}

function parseCustomDailyHour(key) {
  if (!key) return null;
  const match = DAILY_CUSTOM_TRIGGER_PATTERN.exec(key.toUpperCase());
  if (!match) return null;
  const hour = Number(match[1]);
  if (!Number.isFinite(hour) || hour < 0 || hour > 23) {
    return null;
  }
  return hour;
}

function formatHourLabel(hour) {
  const normalized = Math.min(23, Math.max(0, Math.floor(hour)));
  return `${normalized.toString().padStart(2, '0')}h00`;
}

function parseCustomWeekly(key) {
  if (!key) return null;
  const match = WEEKLY_CUSTOM_TRIGGER_PATTERN.exec(key.toUpperCase());
  if (!match) return null;
  const dayCode = match[1];
  const hour = Number(match[2]);
  if (!WEEKDAY_CODE_MAP[dayCode]) {
    return null;
  }
  if (!Number.isFinite(hour) || hour < 0 || hour > 23) {
    return null;
  }
  return { dayCode: dayCode, hour: hour };
}

function formatWeekdayLabel(dayCode) {
  const entry = WEEKDAY_CODE_MAP[dayCode];
  if (entry && entry.label) {
    return entry.label;
  }
  return dayCode || 'jour';
}

function getStoredTriggerFrequency() {
  const props = PropertiesService.getScriptProperties();
  const rawValue = props.getProperty(TRIGGER_FREQUENCY_PROPERTY);
  const sanitized = sanitizeTriggerFrequency(rawValue);
  if (!rawValue || rawValue !== sanitized) {
    props.setProperty(TRIGGER_FREQUENCY_PROPERTY, sanitized);
  }
  return sanitized;
}

function setStoredTriggerFrequency(key) {
  const sanitized = sanitizeTriggerFrequency(key);
  PropertiesService.getScriptProperties().setProperty(TRIGGER_FREQUENCY_PROPERTY, sanitized);
  return sanitized;
}

function persistTriggerFrequencyToSheet(frequencyKey) {
  try {
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    if (!spreadsheet) return;
    const activeSheet = spreadsheet.getActiveSheet();
    if (!activeSheet) return;
    const existingConfig = readFormConfigFromSheet(activeSheet) || {};
    const sanitized = sanitizeTriggerFrequency(frequencyKey);
    if (
      existingConfig.trigger_frequency &&
      existingConfig.trigger_frequency.toString().trim() === sanitized
    ) {
      return;
    }
    const mergedConfig = Object.assign({}, existingConfig, {
      trigger_frequency: sanitized
    });
    writeFormConfigToSheet(activeSheet, mergedConfig);
  } catch (e) {
    uiHandleException('persistTriggerFrequencyToSheet', e, { frequencyKey });
  }
}

function onOpen() {
  afficheMenu();
  console.log("Fin de onOpen");
  const props = PropertiesService.getDocumentProperties();
  const projectId = props.getProperty('BQ_PROJECT_ID');
  const dataset = props.getProperty('BQ_DATASET');
  const location = props.getProperty('BQ_LOCATION');
  console.log(`ScriptProperties BQ -> project=${projectId || 'NULL'}, dataset=${dataset || 'NULL'}, location=${location || 'NULL'}`);
  try {
    getStoredTriggerFrequency();
  } catch (e) {
    uiHandleException('onOpen.triggerFrequency', e);
  }
  ensureDeduplicationTrigger();
}

/**
 * Définit les propriétés du script avec un état d'exécution donné.
 *
 * @param {string} etat - L'état d'exécution à définir.
 */
function setScriptProperties(etat){
  var scriptProperties = PropertiesService.getScriptProperties();
  scriptProperties.setProperty('etatExecution', etat);
}

function getEtatExecution() {
  return PropertiesService.getScriptProperties().getProperty('etatExecution');
}

function initBigQueryConfigFromSheet() {
  try {
    const ui = SpreadsheetApp.getUi();
    const defaults = libKizeo.initBigQueryConfig();
    const refreshedProps = PropertiesService.getDocumentProperties();
    refreshedProps.setProperties({
      BQ_PROJECT_ID: defaults.projectId,
      BQ_DATASET: defaults.datasetId,
      BQ_LOCATION: defaults.location || ''
    }, true);
    try {
      libKizeo.ensureBigQueryCoreTables();
    } catch (ensureError) {
      libKizeo.handleException('initBigQueryConfigFromSheet.ensureCore', ensureError);
    }
    const finalProject = refreshedProps.getProperty('BQ_PROJECT_ID');
    const finalDataset = refreshedProps.getProperty('BQ_DATASET');
    const finalLocation = refreshedProps.getProperty('BQ_LOCATION');
    Logger.log(`initBigQueryConfigFromSheet -> project=${finalProject}, dataset=${finalDataset}, location=${finalLocation}`);
    ui.alert(`Configuration BigQuery initialisée :\nProjet=${finalProject}\nDataset=${finalDataset}\nLocation=${finalLocation || 'default'}`);
  } catch (e) {
    libKizeo.handleException('initBigQueryConfigFromSheet', e);
  }
}

// ----------------------
// Helpers Export (libérables dans libKizeo)
// ----------------------
/**
 * Retourne l'ID d'un sous‑répertoire existant ou fraîchement créé sous un dossier parent.
 * @param {string} parentFolderId – dossier parent
 * @param {string} subFolderName – nom du sous dossier à récupérer / créer
 * @return {string} id du sous‑dossier
 */
function getOrCreateSubFolder(parentFolderId, subFolderName) {
  const parent = DriveApp.getFolderById(parentFolderId);
  const it = parent.getFoldersByName(subFolderName);
  // Vérifie si un dossier avec ce nom existe déjà
  if (it.hasNext()) {
    // Si oui, retourne l'ID du dossier existant
    return it.next().getId();
  } else {
    // Sinon, crée un nouveau dossier et retourne son ID
    return parent.createFolder(subFolderName).getId();
  }
}

/**
 * Génère un nom de fichier stable pour un média en y ajoutant l'ID Drive si possible.
 * @param {{name:string,fileName:string,driveFileId:string,dataId:string}} media
 * @return {string}
 */
function buildMediaDisplayName(media) {
  const baseName = media.name || media.fileName || `media_${media.dataId || 'unknown'}`;
  const driveId = media.driveFileId || '';
  if (!driveId) {
    return baseName;
  }
  const sanitizedId = driveId.replace(/[^A-Za-z0-9_-]/g, '');
  if (!sanitizedId || baseName.indexOf(sanitizedId) !== -1) {
    return baseName;
  }
  return `${baseName}__${sanitizedId}`;
}

/**
 * Sauvegarde un blob PDF dans un dossier cible.
 */
function exportPdfBlob(formulaireNom, dataId, pdfBlob, targetFolderId) {
  const fileName = `${formulaireNom}_${dataId}_${new Date()
    .toISOString()
    .replace(/[:.]/g, '-')}`;
  libKizeo.saveBlobToFolder(pdfBlob, targetFolderId, fileName);
}

/**
 * Copie des médias vers un sous‑dossier « media » sans écraser les fichiers déjà présents.
 * Un média est considéré comme déjà présent s'il existe un fichier du même nom dans le dossier cible.
 */
function exportMedias(mediaList, targetFolderId) {
  if (!mediaList?.length) return;

  const mediaFolderId = getOrCreateSubFolder(targetFolderId, 'media');
  const mediaFolder = DriveApp.getFolderById(mediaFolderId);

  mediaList.forEach((m) => {
    try {
      const displayName = buildMediaDisplayName(m);

      const candidateId = m.driveFileId || '';
      if (!candidateId && !m.id) {
        console.log(`ID manquant pour le média ${displayName}`);
        return;
      }

      // Extraire l'ID du fichier de la formule HYPERLINK si aucun ID dédié n'est présent
      let fileId = candidateId;
      if (!fileId && typeof m.id === 'string' && m.id.includes('id=')) {
        fileId = m.id.split('id=')[1].split('"')[0];
      }

      if (!fileId && typeof m.driveUrl === 'string' && m.driveUrl.includes('id=')) {
        fileId = m.driveUrl.split('id=')[1].split('&')[0];
      }

      if (!fileId) {
        console.log(`Impossible de déterminer l'ID Drive pour ${displayName}`);
        return;
      }

      const alreadyThere = mediaFolder.getFilesByName(displayName);
      if (alreadyThere.hasNext()) return;

      const file = DriveApp.getFileById(fileId);
      file.makeCopy(displayName, mediaFolder);
      
    } catch (e) {
      // Utiliser m.id au lieu de fileId qui pourrait ne pas être défini en cas d'erreur précoce
      console.log(`Erreur copie média ${m.name || m.fileName} : ${e.message}\nID original: ${m.driveFileId || m.id}`);
    }
  });
}

function readFormConfigFromSheet(sheet) {
  if (!sheet) return {};
  try {
    if (typeof libKizeo !== 'undefined' && typeof libKizeo.readConfigFromSheet === 'function') {
      const config = libKizeo.readConfigFromSheet(sheet);
      if (config) return config;
    }
  } catch (err) {
    console.log(`readFormConfigFromSheet fallback: ${err && err.message ? err.message : err}`);
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return {};
  }
  const values = sheet.getRange(2, 1, lastRow - 1, 2).getValues();
  const config = {};
  values.forEach((row) => {
    const key = row[0];
    if (!key) return;
    config[String(key).trim()] = row[1];
  });
  return config;
}

function writeFormConfigToSheet(sheet, config) {
  if (!sheet) return;
  const existingRowCount = Math.max(sheet.getLastRow() - 1, 0);
  if (existingRowCount > 0) {
    sheet.getRange(2, 1, existingRowCount, 2).clearContent();
  }

  const entries = new Map();
  if (config && typeof config === 'object') {
    Object.keys(config).forEach((key) => {
      const trimmedKey = String(key || '').trim();
      if (!trimmedKey) return;
      entries.set(trimmedKey, config[key]);
    });
  }

  entries.delete('bq_alias');

  const rows = [];
  CONFIG_HEADERS.forEach((header) => {
    const value = entries.has(header) ? entries.get(header) : '';
    rows.push([header, value]);
    entries.delete(header);
  });
  entries.forEach((value, key) => {
    rows.push([key, value]);
  });

  if (rows.length) {
    sheet.getRange(2, 1, rows.length, 2).setValues(rows);
  }
}

function resolveFormulaireContext(spreadsheetBdD) {
  const sheet = spreadsheetBdD.getActiveSheet();
  if (!sheet) {
    return null;
  }
  const config = readFormConfigFromSheet(sheet) || {};
  return {
    sheet,
    config,
    batchLimit: getConfiguredBatchLimit(config)
  };
}

function createActionCode() {
  const timestamp = new Date().getTime().toString(36);
  const randomSegment = Utilities.getUuid().replace(/-/g, '').substring(0, 12);
  return `act_${timestamp}${randomSegment}`.substring(0, 30);
}

function validateFormConfig(rawConfig, sheet) {
  const config = rawConfig || {};
  const errors = [];
  const sanitized = {};

  REQUIRED_CONFIG_KEYS.forEach((key) => {
    const rawValue = config[key];
    const value = rawValue !== undefined && rawValue !== null ? String(rawValue).trim() : '';
    if (!value) {
      errors.push({ key, message: `Champ ${key} manquant ou vide.` });
    } else {
      sanitized[key] = value;
    }
  });

  const rawBatchLimit = config[CONFIG_BATCH_LIMIT_KEY];
  const sanitizedBatchLimit = sanitizeBatchLimitValue(rawBatchLimit);
  if (
    rawBatchLimit !== undefined &&
    rawBatchLimit !== null &&
    rawBatchLimit !== '' &&
    sanitizedBatchLimit === null
  ) {
    errors.push({ key: CONFIG_BATCH_LIMIT_KEY, message: 'batch_limit doit être un entier positif.' });
  } else {
    sanitized[CONFIG_BATCH_LIMIT_KEY] =
      sanitizedBatchLimit !== null ? sanitizedBatchLimit : DEFAULT_KIZEO_BATCH_LIMIT;
  }

  const tableNameCandidate =
    config.bq_table_name !== undefined && config.bq_table_name !== null
      ? String(config.bq_table_name).trim()
      : config.bq_alias !== undefined && config.bq_alias !== null
      ? String(config.bq_alias).trim()
      : '';

  const formIdForTable = sanitized.form_id || (config.form_id ? String(config.form_id).trim() : '');
  const formNameForTable = sanitized.form_name || (config.form_name ? String(config.form_name).trim() : '');

  let computedTableName = '';
  try {
    if (typeof libKizeo.bqComputeTableName === 'function') {
      computedTableName = libKizeo.bqComputeTableName(formIdForTable, formNameForTable, tableNameCandidate);
    }
  } catch (computeError) {
    console.log(`validateFormConfig: échec calcul table -> ${computeError}`);
  }

  if (!computedTableName) {
    errors.push({ key: 'bq_table_name', message: 'bq_table_name manquant ou invalide.' });
  } else {
    if (computedTableName.length > MAX_BQ_TABLE_NAME_LENGTH) {
      errors.push({
        key: 'bq_table_name',
        message: `bq_table_name doit contenir ${MAX_BQ_TABLE_NAME_LENGTH} caractères maximum.`
      });
    } else {
      sanitized.bq_table_name = computedTableName;
    }
  }

  return {
    isValid: errors.length === 0,
    config: sanitized,
    errors,
    sheetName: sheet ? sheet.getName() : ''
  };
}

function notifyConfigErrors(validation) {
  const lines = validation.errors.map((error) => `• ${error.message}`).join('\n');
  const message = `${validation.sheetName ? validation.sheetName + '\n' : ''}${lines}`;

  try {
    const ui = SpreadsheetApp.getUi();
    ui.alert('Configuration invalide', message, ui.ButtonSet.OK);
  } catch (uiError) {
    try {
      const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
      spreadsheet.toast(message, 'Configuration invalide', 10);
    } catch (toastError) {
      console.log(`Impossible d'afficher une alerte UI: ${toastError}`);
    }
  }

  console.log(`Configuration invalide détectée: ${message}`);
}

/**
 * Met à jour les données pour le formulaire configuré dans l'onglet unique du classeur.
 * Les réponses sont ingérées dans BigQuery et les exports Drive (PDF/médias) sont déclenchés si requis.
 */
function main() {
  const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  const context = resolveFormulaireContext(spreadsheetBdD);

  if (!context) {
    console.log('main: aucun formulaire configuré pour ce classeur.');
    return;
  }

  const validation = validateFormConfig(context.config, context.sheet);
  if (!validation.isValid) {
    notifyConfigErrors(validation);
    return;
  }

  context.config = Object.assign({}, context.config, validation.config);

  if (getEtatExecution() === 'enCours') {
    console.log('Exécution précédente toujours en cours.');
    console.log("En cas de blocage, réinitialisez l'état manuellement ou exécutez setScriptProperties('termine').");
    return;
  }

  setScriptProperties('enCours');

  const runStart = Date.now();

  try {
    const tableName = validation.config.bq_table_name;
    let aliasPart = tableName;
    try {
      if (typeof libKizeo.bqExtractAliasPart === 'function') {
        aliasPart = libKizeo.bqExtractAliasPart(tableName, validation.config.form_id);
      }
    } catch (aliasError) {
      console.log(`main: impossible d'extraire l'alias -> ${aliasError}`);
    }

    const formulaire = {
      nom: validation.config.form_name,
      id: validation.config.form_id,
      tableName,
      alias: aliasPart
    };
    const batchLimit = context.batchLimit || DEFAULT_KIZEO_BATCH_LIMIT;
    const action = validation.config.action;

    // ---------- Récupération des nouvelles données pour les exports ----------
    const unreadResp = libKizeo.requeteAPIDonnees(
      'GET',
      `/forms/${formulaire.id}/data/unread/${action}/${batchLimit}?includeupdated`
    );

    if (!unreadResp || unreadResp.data.status !== 'ok') {
      console.log(`Erreur API unread : ${unreadResp?.data?.status}`);
      return;
    }

    const nouvellesDonnees = Array.isArray(unreadResp.data.data) ? unreadResp.data.data : [];

    // ---------- Préparation BigQuery et ingestion ----------
    const processResult = libKizeo.processData(spreadsheetBdD, formulaire, action, batchLimit);
    const runDurationMs = Math.max(0, Date.now() - runStart);
    const canPersistRun =
      processResult &&
      processResult.status !== 'ERROR' &&
      (processResult.runTimestamp || processResult.latestRecord || typeof processResult.rowCount === 'number');

    if (canPersistRun) {
      const refreshedConfig = Object.assign({}, context.config, {
        form_id: formulaire.id,
        form_name: formulaire.nom,
        bq_table_name: formulaire.tableName
      });
      const hasRowCount = typeof processResult.rowCount === 'number' && !Number.isNaN(processResult.rowCount);
      if (hasRowCount) {
        refreshedConfig.last_saved_row_count = processResult.rowCount;
      }
      if (Number.isFinite(runDurationMs)) {
        const runDurationSeconds = Math.round(runDurationMs / 1000);
        refreshedConfig.last_run_duration_s = Math.max(0, runDurationSeconds);
      }
      if (processResult.runTimestamp) {
        refreshedConfig.last_run_at = processResult.runTimestamp;
      }
      if (processResult.latestRecord) {
        const latest = processResult.latestRecord;
        const latestId = latest.id || latest._id || '';
        if (latestId) {
          refreshedConfig.last_data_id = latestId;
        }
        const latestUpdate = latest.update_time || latest._update_time || '';
        if (latestUpdate) {
          refreshedConfig.last_update_time = latestUpdate;
        }
        const latestAnswer = latest.answer_time || latest._answer_time || '';
        if (latestAnswer) {
          refreshedConfig.last_answer_time = latestAnswer;
        }
      }
      writeFormConfigToSheet(context.sheet, refreshedConfig);
      context.config = refreshedConfig;
    }
    const mediasIndexes = processResult?.medias || [];

    if (!nouvellesDonnees.length) {
      Logger.log('Pas de nouveaux enregistrements');
      return;
    }

    // ---------- Boucle par nouvel enregistrement ----------
    nouvellesDonnees.forEach((data) => {
      const dataFields = data || {};
      const dataId = data._id;

      /* localisation de l'export */
      const driveUrlEntry = Object.entries(dataFields).find(([k, v]) => k.includes('driveexport') && v);
      const driveUrl = driveUrlEntry ? driveUrlEntry[1] : null;

      if (!driveUrl) {
        // Pas de driveexport → on arrête ici cet iteration de forEach
        return;
      }
      console.log(`Un export est configuré pour ${dataId}, l'adresse est ${driveUrl}`);
      if (typeof driveUrl !== 'string' || !driveUrl.includes('drive.google.com')) return;
      const folderIdMatch =
        driveUrl.match(/drive\/u\/\d+\/folders\/([^?\s/]+)/) || driveUrl.match(/drive\/folders\/([^?\s/]+)/);
      if (!folderIdMatch) return;
      const folderId = folderIdMatch[1];

      /* sous‑répertoire optionnel */
      const subFolderName = Object.entries(dataFields).find(([k, v]) => k.includes('sousrepertoireexport') && v)?.[1] || null;
      const targetFolderId = subFolderName ? getOrCreateSubFolder(folderId, subFolderName) : folderId;

      /* type d'export */
      const typeExport =
        (Object.entries(dataFields).find(([k, v]) => k.includes('typeexport') && v)?.[1] || 'pdf')
          .toString()
          .toLowerCase(); // si aucun champ typeexport n'est trouvé on traite comme pdf
      console.log(`Type d'export ${typeExport} pour ${dataId}`);
      /* actions */
      if (['pdf', 'pdfmedia'].includes(typeExport)) {
        console.log('Export type PDF pour ' + dataId);
        try {
          const pdfResp = libKizeo.requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/${dataId}/pdf`);
          exportPdfBlob(formulaire.nom, dataId, pdfResp.data, targetFolderId);
        } catch (e) {
          Logger.log(`Erreur export PDF : ${e.message}`);
        }
      }
      if (['media', 'pdfmedia'].includes(typeExport)) {
        console.log('Export type media pour ' + dataId);
        const mediasPourRecord = mediasIndexes.filter((m) => m.dataId === dataId);
        exportMedias(mediasPourRecord, targetFolderId);
      }
    });
  } catch (e) {
    libKizeo.handleException('main', e);
  } finally {
    setScriptProperties('termine');
  }
}

function runBigQueryDeduplication() {
  if (getEtatExecution() === 'enCours') {
    console.log('runBigQueryDeduplication: exécution principale en cours, déduplication reportée.');
    return {
      status: 'SKIPPED',
      reason: 'RUN_IN_PROGRESS',
      message: 'Une mise à jour est déjà en cours.'
    };
  }

  const spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  const context = resolveFormulaireContext(spreadsheetBdD);
  if (!context) {
    console.log('runBigQueryDeduplication: aucun formulaire configuré, déduplication ignorée.');
    return {
      status: 'SKIPPED',
      reason: 'NO_FORM_CONFIG',
      message: 'Aucun formulaire sélectionné.'
    };
  }

  const validation = validateFormConfig(context.config, context.sheet);
  if (!validation.isValid) {
    console.log('runBigQueryDeduplication: configuration invalide, déduplication ignorée.');
    return {
      status: 'SKIPPED',
      reason: 'INVALID_CONFIG',
      message: 'Configuration invalide.',
      errors: validation.errors
    };
  }

  const tableName = validation.config.bq_table_name;
  let aliasPart = tableName;
  try {
    if (typeof libKizeo.bqExtractAliasPart === 'function') {
      aliasPart = libKizeo.bqExtractAliasPart(tableName, validation.config.form_id);
    }
  } catch (aliasError) {
    console.log(`runBigQueryDeduplication: impossible d'extraire l'alias -> ${aliasError}`);
  }

  const formulaire = {
    nom: validation.config.form_name,
    id: validation.config.form_id,
    tableName,
    alias: aliasPart
  };

  Logger.log(`runBigQueryDeduplication: lancement pour ${formulaire.id} (${tableName})`);

  try {
    const summary = libKizeo.bqRunDeduplicationForForm(formulaire);
    if (summary) {
      Logger.log(
        `runBigQueryDeduplication: terminé -> parent supprimé=${summary.parent.deleted}, tables filles traitées=${summary.subTables.length}`
      );
      return {
        status: 'DONE',
        parent: summary.parent,
        subTables: summary.subTables
      };
    }
    return {
      status: 'DONE',
      parent: null,
      subTables: []
    };
  } catch (e) {
    libKizeo.handleException('runBigQueryDeduplication', e, {
      formId: formulaire.id,
      tableName
    });
    return {
      status: 'ERROR',
      reason: 'EXCEPTION',
      message: e && e.message ? e.message : String(e)
    };
  }
}

function launchManualDeduplication() {
  const ui = SpreadsheetApp.getUi();
  let result;
  try {
    result = runBigQueryDeduplication();
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    ui.alert('Déduplication BigQuery', `Erreur inattendue: ${message}`, ui.ButtonSet.OK);
    return;
  }

  if (!result) {
    ui.alert('Déduplication BigQuery', "Aucun résultat retourné par la déduplication.", ui.ButtonSet.OK);
    return;
  }

  if (result.status === 'SKIPPED') {
    const reason = result.reason || 'UNKNOWN';
    const message =
      reason === 'RUN_IN_PROGRESS'
        ? 'Une mise à jour est déjà en cours. Réessayez après sa fin.'
        : reason === 'NO_FORM_CONFIG'
        ? 'Aucun formulaire n’est configuré pour ce classeur. Sélectionnez un formulaire avant de lancer la déduplication.'
        : reason === 'INVALID_CONFIG'
        ? `Configuration invalide.\n${(result.errors && result.errors.length && result.errors[0].message) || ''}`.trim()
        : result.message || 'Opération ignorée.';
    ui.alert('Déduplication BigQuery', message, ui.ButtonSet.OK);
    return;
  }

  if (result.status === 'ERROR') {
    ui.alert(
      'Déduplication BigQuery',
      `Échec de la déduplication: ${result.message || 'Erreur inconnue.'}`,
      ui.ButtonSet.OK
    );
    return;
  }

  const subTables = Array.isArray(result.subTables) ? result.subTables : [];
  const entriesToReview = [];
  const streamingBlocked = [];

  const parentEntry = result.parent || null;
  if (parentEntry && parentEntry.skipped) {
    entriesToReview.push(parentEntry);
    if (parentEntry.reason && parentEntry.reason.indexOf('STREAMING_BUFFER') === 0) {
      streamingBlocked.push(parentEntry.tableId || 'table parent');
    }
  }

  subTables.forEach((entry) => {
    if (!entry) return;
    if (entry.skipped) {
      entriesToReview.push(entry);
      if (entry.reason && entry.reason.indexOf('STREAMING_BUFFER') === 0) {
        streamingBlocked.push(entry.tableId || 'table fille');
      }
    }
  });

  if (streamingBlocked.length) {
    const list = streamingBlocked.map((name) => `• ${name}`).join('\n');
    ui.alert(
      'Déduplication BigQuery',
      `Déduplication reportée: BigQuery signale un buffer de streaming actif sur:\n${list}\nRéessayez dans quelques minutes.`,
      ui.ButtonSet.OK
    );
    return;
  }

  const parentDeleted = parentEntry && !parentEntry.skipped ? Number(parentEntry.deleted || 0) : 0;
  const subDeleted = subTables
    .filter((entry) => entry && !entry.skipped)
    .reduce((acc, entry) => acc + Number(entry.deleted || 0), 0);

  const successMessage = [
    `Déduplication terminée.`,
    `Doublons parent supprimés : ${parentDeleted}`,
    `Doublons tables filles supprimés : ${subDeleted}`
  ].join('\n');

  if (entriesToReview.length) {
    const otherIssues = entriesToReview
      .filter((entry) => !(entry.reason && entry.reason.indexOf('STREAMING_BUFFER') === 0))
      .map((entry) => `• ${entry.tableId || 'table inconnue'} (${entry.reason || 'raison inconnue'})`)
      .join('\n');
    const message = otherIssues ? `${successMessage}\n\nTables ignorées :\n${otherIssues}` : successMessage;
    ui.alert('Déduplication BigQuery', message, ui.ButtonSet.OK);
    return;
  }

  ui.alert('Déduplication BigQuery', successMessage, ui.ButtonSet.OK);
}

/**
 * Réinitialise complètement la feuille si l'utilisateur est d'accord.
 * Toutes les feuilles sauf 'Reinit' sont supprimées et 'Reinit' est vidée.
 * Tous les déclencheurs sont supprimés et l'utilisateur est invité à définir une nouvelle durée de déclenchement.
 * Les données sont marquées comme non lues sur le serveur Kizeo
 * Ensuite, la fonction onOpen est exécutée et la fonction de sélection de formulaire est chargée.
 */
