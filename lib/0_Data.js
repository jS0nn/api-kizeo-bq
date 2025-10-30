// Version 0.7.0
/*
  ToDo :
    Optimiser : envoyer le token depuis l'appel de fonction (et eviter de le lire à chaque appel api)
    pour  majListeExterne : ne pas traiter les données mises à jour mais seulement les nvlles données
*/

const LEGACY_USAGE_PROPERTY_PREFIX = 'LEGACY_USAGE_';
const DATA_LOG_PREFIX = 'lib:Data';

function ensureProcessManager() {
  if (typeof ProcessManager === 'undefined' || !ProcessManager) {
    throw new Error('ProcessManager indisponible');
  }
  return ProcessManager;
}

function resolveLogFunction(loggerCandidate) {
  return ensureProcessManager().resolveLogFunction(loggerCandidate);
}

function createIngestionServices(overrides) {
  return ensureProcessManager().createIngestionServices(overrides);
}

function getLibraryPropertiesStore() {
  try {
    return PropertiesService.getDocumentProperties();
  } catch (e) {
    console.log(`${DATA_LOG_PREFIX}: DocumentProperties indisponibles (${e})`);
    return PropertiesService.getScriptProperties();
  }
}

function isLegacySheetsSyncEnabled() {
  return false;
}

function recordLegacyUsage(marker) {
  try {
    const store = getLibraryPropertiesStore();
    store.setProperty(LEGACY_USAGE_PROPERTY_PREFIX + marker, new Date().toISOString());
  } catch (e) {
    console.log(`${DATA_LOG_PREFIX}: enregistrement usage legacy impossible (${e})`);
  }
}

function logLegacyUsageStats() {
  try {
    const store = getLibraryPropertiesStore();
    if (!store || typeof store.getProperties !== 'function') {
      console.log(`${DATA_LOG_PREFIX}: logLegacyUsageStats -> store indisponible.`);
      return 0;
    }
    const props = store.getProperties() || {};
    const legacyKeys = Object.keys(props).filter((key) => key.indexOf(LEGACY_USAGE_PROPERTY_PREFIX) === 0);
    if (!legacyKeys.length) {
      console.log(`${DATA_LOG_PREFIX}: aucune utilisation legacy recensée.`);
      return 0;
    }
    legacyKeys.sort();
    legacyKeys.forEach((key) => {
      console.log(`${DATA_LOG_PREFIX}: usage legacy -> ${key} = ${props[key]}`);
    });
    console.log(`${DATA_LOG_PREFIX}: total flags legacy = ${legacyKeys.length}`);
    return legacyKeys.length;
  } catch (e) {
    handleException('logLegacyUsageStats', e);
    return null;
  }
}

function parseConfigBooleanFlag(raw, defaultValue) {
  if (raw === null || raw === undefined || raw === '') {
    return !!defaultValue;
  }
  if (typeof raw === 'boolean') {
    return raw;
  }
  const normalized = raw.toString().trim().toLowerCase();
  if (!normalized) {
    return !!defaultValue;
  }
  if (['true', '1', 'yes', 'y', 'oui'].indexOf(normalized) !== -1) {
    return true;
  }
  if (['false', '0', 'no', 'n', 'non'].indexOf(normalized) !== -1) {
    return false;
  }
  return !!defaultValue;
}

function buildExecutionTargets(existingConfig, overrideTargets) {
  return ensureProcessManager().buildExecutionTargets(existingConfig, overrideTargets);
}

function ingestBigQueryPayloads(formulaire, bigQueryContext, medias, services, logFn) {
  return ensureProcessManager().ingestBigQueryPayloads(
    formulaire,
    bigQueryContext,
    medias,
    services,
    logFn
  );
}

function runExternalListsSync(formulaire, snapshot, fetchFn, apiPath, logFn) {
  return ensureProcessManager().runExternalListsSync(formulaire, snapshot, fetchFn, apiPath, logFn);
}

function resolveIsoTimestamp(nowProvider) {
  return ensureProcessManager().resolveIsoTimestamp(nowProvider);
}

function pickMostRecentRecord(current, reference) {
  return ensureProcessManager().pickMostRecentRecord(current, reference);
}

function fetchDetailedRecord(fetchFn, formulaire, recordSummaryId, log) {
  return ensureProcessManager().fetchDetailedRecord(fetchFn, formulaire, recordSummaryId, log);
}

function resolveUnreadDataset(fetchFn, formulaire, apiPath, hasPreviousRun, prefetchedPayload, log) {
  return ensureProcessManager().resolveUnreadDataset(
    fetchFn,
    formulaire,
    apiPath,
    hasPreviousRun,
    prefetchedPayload,
    log
  );
}

function resolveBatchLimit(rawLimit) {
  return ensureProcessManager().resolveBatchLimit(rawLimit);
}

/**
 * Récupère les enregistrements non lus, les écrit dans le classeur et collecte la liste
 * des médias créés sur Drive pendant l'opération.
 * @return {Object} { medias: [{dataId,name,id}, …] }
 */
function processData(spreadsheetBdD, formulaire, action, nbFormulairesACharger, options) {
  return ensureProcessManager().processData(spreadsheetBdD, formulaire, action, nbFormulairesACharger, options);
}


function bqBackfillForm(formId, options) {
  const normalizedId = formId ? String(formId).trim() : '';
  if (!normalizedId) {
    throw new Error('bqBackfillForm: formId requis');
  }
  try {
    return runBigQueryBackfillForForm(normalizedId, options || {});
  } catch (e) {
    handleException('bqBackfillForm', e, { formId: normalizedId });
    throw e;
  }
}

function runBigQueryBackfillForForm(formId, opts) {
  const options = opts || {};
  const services = createIngestionServices(options.services);
  const log = resolveLogFunction(services.logger);
  const fetchFn = typeof services.fetch === 'function' ? services.fetch : requeteAPIDonnees;
  const chunkSize = resolveBackfillChunkSize(options.chunkSize);
  const startBoundary = parseBackfillBoundary(options.startDate);
  const endBoundary = parseBackfillBoundary(options.endDate);
  if (startBoundary !== null && endBoundary !== null && startBoundary > endBoundary) {
    throw new Error('bqBackfillForm: startDate doit être antérieure ou égale à endDate');
  }

  log(
    `bqBackfillForm start -> formId=${formId}, start=${
      startBoundary !== null ? new Date(startBoundary).toISOString() : 'n/a'
    }, end=${endBoundary !== null ? new Date(endBoundary).toISOString() : 'n/a'}, chunk=${chunkSize}`
  );

  const formulaire = resolveBackfillFormMetadata(formId, options, fetchFn, log);
  prepareBigQueryTargetsForBackfill(formulaire, services, log);

  const listResponse = fetchFn('GET', `/forms/${formulaire.id}/data/all`);
  if (!listResponse || listResponse.responseCode !== 200) {
    throw new Error(
      `bqBackfillForm: impossible de récupérer /forms/${formulaire.id}/data/all (code=${listResponse?.responseCode || 'n/a'})`
    );
  }

  const payload = listResponse.data || {};
  const rawDataArray = Array.isArray(payload.data) ? payload.data.slice() : [];
  if (!rawDataArray.length) {
    log(`bqBackfillForm: aucune donnée renvoyée par data/all pour form=${formulaire.id}`);
    return buildBackfillSummary(formulaire, startBoundary, endBoundary, {
      totalFetched: 0,
      totalChunks: 0,
      totalRawInserted: 0,
      totalParentInserted: 0,
      totalSubRowsInserted: 0,
      totalMediasInserted: 0
    });
  }

  const candidateSummaries = filterBackfillSummaries(rawDataArray, startBoundary, endBoundary, options.limit || null);
  if (!candidateSummaries.length) {
    log(`bqBackfillForm: aucune donnée dans l'intervalle demandé (form=${formulaire.id}).`);
    return buildBackfillSummary(formulaire, startBoundary, endBoundary, {
      totalFetched: rawDataArray.length,
      totalChunks: 0,
      totalRawInserted: 0,
      totalParentInserted: 0,
      totalSubRowsInserted: 0,
      totalMediasInserted: 0
    });
  }

  log(
    `bqBackfillForm: ${candidateSummaries.length} enregistrements à traiter après filtrage (sur ${rawDataArray.length}).`
  );

  let totalRawInserted = 0;
  let totalParentInserted = 0;
  let totalSubRowsInserted = 0;
  let totalMediasInserted = 0;
  let totalChunks = 0;

  for (let index = 0; index < candidateSummaries.length; index += chunkSize) {
    const chunkSummaries = candidateSummaries.slice(index, index + chunkSize);
    const chunkResult = ingestBackfillChunk(formulaire, chunkSummaries, {
      services,
      fetchFn,
      log,
      startBoundary,
      endBoundary,
      includeMedia: options.includeMedia === true,
      spreadsheetId: options.spreadsheetId || null
    });
    totalChunks += 1;
    totalRawInserted += chunkResult.rawInserted;
    totalParentInserted += chunkResult.parentInserted;
    totalSubRowsInserted += chunkResult.subRowsInserted;
    totalMediasInserted += chunkResult.mediasInserted;
  }

  log(
    `bqBackfillForm: terminé -> raw=${totalRawInserted}, parent=${totalParentInserted}, subRows=${totalSubRowsInserted}, medias=${totalMediasInserted}, chunks=${totalChunks}`
  );

  return buildBackfillSummary(formulaire, startBoundary, endBoundary, {
    totalFetched: candidateSummaries.length,
    totalChunks,
    totalRawInserted,
    totalParentInserted,
    totalSubRowsInserted,
    totalMediasInserted
  });
}

function ingestBackfillChunk(formulaire, summaries, context) {
  const fetchFn = context.fetchFn;
  const log = context.log;
  const services = context.services;
  const startBoundary = context.startBoundary;
  const endBoundary = context.endBoundary;
  const includeMedia = context.includeMedia === true;
  const mediasTarget = [];
  let spreadsheetForMedia = null;

  if (includeMedia) {
    try {
      const spreadsheetId = context.spreadsheetId;
      if (spreadsheetId) {
        spreadsheetForMedia = SpreadsheetApp.openById(spreadsheetId);
      } else {
        spreadsheetForMedia = SpreadsheetApp.getActiveSpreadsheet();
      }
    } catch (e) {
      handleException('ingestBackfillChunk.openSpreadsheet', e);
      spreadsheetForMedia = null;
    }
  }

  const bigQueryContext = {
    rawRows: [],
    parentRows: [],
    parentColumns: {},
    subTables: {}
  };

  summaries.forEach((summary) => {
    const recordId = summary?._id || summary?.id || summary?.data_id;
    if (!recordId) {
      log('bqBackfillForm: enregistrement sans identifiant, ignoré.');
      return;
    }

    const detail = fetchDetailedRecord(fetchFn, formulaire, recordId, log);
    if (!detail) {
      return;
    }

    if (!isRecordWithinBoundaries(detail, startBoundary, endBoundary)) {
      return;
    }

    bigQueryContext.rawRows.push(detail);
    const parentPrepared = bqPrepareParentRow(formulaire, detail);
    if (parentPrepared) {
      bigQueryContext.parentRows.push(parentPrepared.row);
      parentPrepared.columns.forEach((col) => {
        if (!col || !col.name) return;
        bigQueryContext.parentColumns[col.name] = col;
      });
      if (Array.isArray(parentPrepared.subforms) && parentPrepared.subforms.length) {
        parentPrepared.subforms.forEach((subform) => {
          if (!subform || !subform.tableId) return;
          const existing = bigQueryContext.subTables[subform.tableId] || { rows: [], columns: {} };
          if (Array.isArray(subform.rows) && subform.rows.length) {
            existing.rows = existing.rows.concat(subform.rows);
          }
          if (Array.isArray(subform.columns) && subform.columns.length) {
            subform.columns.forEach((col) => {
              if (!col || !col.name) return;
              existing.columns[col.name] = col;
            });
          }
          bigQueryContext.subTables[subform.tableId] = existing;
        });
      }
    }

    if (includeMedia && spreadsheetForMedia) {
      collectMediasForRecord(formulaire, detail, mediasTarget, spreadsheetForMedia);
    }
  });

  ingestBigQueryPayloads(formulaire, bigQueryContext, mediasTarget, services, log);

  return {
    rawInserted: bigQueryContext.rawRows.length,
    parentInserted: bigQueryContext.parentRows.length,
    subRowsInserted: computeSubRowCount(bigQueryContext.subTables),
    mediasInserted: mediasTarget.length
  };
}

function computeSubRowCount(subTablesMap) {
  if (!subTablesMap) return 0;
  return Object.keys(subTablesMap).reduce((acc, tableId) => {
    const definition = subTablesMap[tableId];
    if (!definition || !Array.isArray(definition.rows)) return acc;
    return acc + definition.rows.length;
  }, 0);
}

function collectMediasForRecord(formulaire, detail, mediasCollector, spreadsheet) {
  try {
    buildRowSnapshot(spreadsheet, formulaire, detail, mediasCollector);
  } catch (e) {
    handleException('collectMediasForRecord', e, { formId: formulaire.id, dataId: detail?.id || '' });
  }
}

function resolveBackfillFormMetadata(formId, options, fetchFn, log) {
  const formulaireBase =
    options && options.formulaire && typeof options.formulaire === 'object' ? Object.assign({}, options.formulaire) : {};
  formulaireBase.id = formId;

  if (!formulaireBase.nom) {
    try {
      const formResponse = fetchFn('GET', `/forms/${formId}`);
      const formPayload = formResponse && formResponse.data ? formResponse.data : null;
      const formData = formPayload && formPayload.form ? formPayload.form : formPayload;
      const extractedName =
        (formData && (formData.name || formData.libelle || formData.label)) || formulaireBase.nom || `form_${formId}`;
      formulaireBase.nom = extractedName;
      log(`bqBackfillForm: nom formulaire résolu -> "${formulaireBase.nom}"`);
    } catch (e) {
      handleException('resolveBackfillFormMetadata', e, { formId });
      if (!formulaireBase.nom) {
        formulaireBase.nom = `form_${formId}`;
      }
    }
  }

  const tableCandidate =
    options.tableName || formulaireBase.tableName || formulaireBase.alias || formulaireBase.nom || `form_${formId}`;
  const computedTableName = bqComputeTableName(formulaireBase.id, formulaireBase.nom || '', tableCandidate);
  formulaireBase.tableName = computedTableName;
  formulaireBase.alias = bqExtractAliasPart(computedTableName, formulaireBase.id);

  return formulaireBase;
}

function prepareBigQueryTargetsForBackfill(formulaire, services, log) {
  const bigQueryServices = services.bigQuery || {};
  const getConfigFn =
    typeof bigQueryServices.getConfig === 'function' ? bigQueryServices.getConfig : getBigQueryConfig;
  const ensureDatasetFn =
    typeof bigQueryServices.ensureDataset === 'function' ? bigQueryServices.ensureDataset : bqEnsureDataset;
  const ensureRawTableFn =
    typeof bigQueryServices.ensureRawTable === 'function' ? bigQueryServices.ensureRawTable : bqEnsureRawTable;
  const ensureParentTableFn =
    typeof bigQueryServices.ensureParentTable === 'function'
      ? bigQueryServices.ensureParentTable
      : bqEnsureParentTable;

  const config = getConfigFn();
  if (!config) {
    throw new Error('bqBackfillForm: configuration BigQuery indisponible');
  }

  ensureDatasetFn(config);
  ensureRawTableFn(config);
  ensureParentTableFn(config, bqParentTableId(formulaire));

  log(`bqBackfillForm: BigQuery prêt -> ${config.projectId}.${config.datasetId}.${formulaire.tableName}`);
}

function filterBackfillSummaries(rawDataArray, startBoundary, endBoundary, limit) {
  const limitNumeric = Number(limit);
  const effectiveLimit = Number.isFinite(limitNumeric) && limitNumeric > 0 ? Math.floor(limitNumeric) : null;
  const filtered = rawDataArray
    .filter((item) => isRecordWithinBoundaries(item, startBoundary, endBoundary))
    .sort((a, b) => {
      const aTs = extractRecordTimestamp(a) || 0;
      const bTs = extractRecordTimestamp(b) || 0;
      return aTs - bTs;
    });
  if (effectiveLimit !== null && filtered.length > effectiveLimit) {
    return filtered.slice(0, effectiveLimit);
  }
  return filtered;
}

function isRecordWithinBoundaries(record, startBoundary, endBoundary) {
  const timestamp = extractRecordTimestamp(record);
  if (timestamp === null) return true;
  if (startBoundary !== null && timestamp < startBoundary) return false;
  if (endBoundary !== null && timestamp > endBoundary) return false;
  return true;
}

function extractRecordTimestamp(record) {
  if (!record) return null;
  const candidate =
    record.update_time ||
    record._update_time ||
    record.answer_time ||
    record._answer_time ||
    record.timestamp ||
    record.created_at;
  if (!candidate) return null;
  const parsed = Date.parse(candidate);
  return Number.isFinite(parsed) ? parsed : null;
}

function resolveBackfillChunkSize(raw) {
  const numeric = Number(raw);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 25;
  }
  return Math.min(250, Math.max(1, Math.floor(numeric)));
}

function parseBackfillBoundary(value) {
  if (value === null || value === undefined || value === '') return null;
  if (value instanceof Date) {
    const time = value.getTime();
    return Number.isFinite(time) ? time : null;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildBackfillSummary(formulaire, startBoundary, endBoundary, stats) {
  return {
    formId: formulaire.id,
    formName: formulaire.nom,
    targetTable: formulaire.tableName,
    startDate: startBoundary !== null ? new Date(startBoundary).toISOString() : null,
    endDate: endBoundary !== null ? new Date(endBoundary).toISOString() : null,
    totalFetched: stats.totalFetched,
    chunkCount: stats.totalChunks,
    inserted: {
      raw: stats.totalRawInserted,
      parent: stats.totalParentInserted,
      subRows: stats.totalSubRowsInserted,
      medias: stats.totalMediasInserted
    }
  };
}

function persistLegacySnapshotToSheet(spreadsheetBdD, formulaire, snapshot) {
  return resolveSnapshotService().persistSnapshot(spreadsheetBdD, formulaire, snapshot);
}

function collectResponseArtifacts(records, context) {
  return ensureProcessManager().collectResponseArtifacts(records, context);
}

/**
 * La fonction `handleResponses` traite les réponses du formulaire (récupérées via l API)
 * et les stocke dans une feuille de calcul Google.
 *
 * @param {Object} spreadsheetBdD - La feuille de calcul Google où seront stockées les données.
 * @param {Object} formulaire - L'objet du formulaire contenant les informations du formulaire (id, nom, etc).
 * @param {string} apiPath - Le chemin vers l'API qui renvoie les données du formulaire.
 * @param {Array} medias – tableau collecteur muté pour accumuler les médias
 * @param {boolean} hasPreviousRun – indique si une ingestion précédente existe (évite fallback inutile)
 * @returns {Object|null} - Renvoie un objet contenant les données traitées ou null en cas d'erreur.
 */
function handleResponses(spreadsheetBdD, formulaire, apiPath, action, medias, hasPreviousRun, options) {
  return ensureProcessManager().handleResponses(
    spreadsheetBdD,
    formulaire,
    apiPath,
    action,
    medias,
    hasPreviousRun,
    options
  );
}

function markResponsesAsRead(formulaire, action, dataIds, fetchFn) {
  return ensureProcessManager().markResponsesAsRead(formulaire, action, dataIds, fetchFn);
}
/**
 * @deprecated Migration BigQuery: conserver uniquement si un fallback Sheets est nécessaire.
 * Fonction pour stocker les données provenant d'une réponse dans une feuille de calcul Google Spreadsheets.
 *
 * @param {Object} spreadsheetBdD - Objet représentant le tableau Google Spreadsheet.
 * @param {Object} dataResponse - Objet contenant les données de la réponse à stocker.
 * @param {Object} formulaire - Objet contenant les informations sur le formulaire.
 * @param {Object} sheetFormulaire - Objet représentant la feuille de calcul où stocker les données.
 * @return {Object} Un objet contenant les données d'exécution de la fonction, ou null en cas d'erreur.
 */
function resolveSnapshotService() {
  if (typeof ExternalSnapshot !== 'undefined') {
    return ExternalSnapshot;
  }
  if (typeof SheetSnapshot !== 'undefined') {
    return SheetSnapshot;
  }
  throw new Error('ExternalSnapshot indisponible');
}

function saveDataToSheet(spreadsheetBdD, dataResponse, formulaire, sheetFormulaire, medias) {
  recordLegacyUsage('saveDataToSheet_call');
  const service = resolveSnapshotService();
  const start = new Date();
  const result = service.saveDataToSheet(spreadsheetBdD, dataResponse, formulaire, sheetFormulaire, medias);
  if (result) {
    console.log(`Durée saveDataToSheet : ${new Date() - start}`);
  }
  return result;
}

/**
 * Prépare les données pour être stockées dans la feuille de calcul.
 *
 * @param {Object} dataResponse - Objet contenant les données de la réponse.
 * @return {Array} Un tableau contenant les en-têtes et les valeurs des données, ou null en cas d'erreur.
 */
/**
 * @deprecated Migration BigQuery: ancienne préparation des en-têtes/valeurs pour Sheets.
 */
function prepareDataForSheet(dataResponse) {
  return resolveSnapshotService().prepareDataForSheet(dataResponse);
}

function buildRowSnapshot(spreadsheetBdD, formulaire, dataResponse, medias) {
  return resolveSnapshotService().buildRowSnapshot(spreadsheetBdD, formulaire, dataResponse, medias);
}


/**
 * Prépare un tableau contenant les champs des données de réponse de l'API.
 *
 * @param {Object} dataResponse - Les données de réponse.
 * @return {Array|null} - Le tableau des champs ou null en cas d'erreur.
 */
function getDataFromFields(dataResponse) {
  return resolveSnapshotService().extractFields(dataResponse);
}


/**
 * Prépare la feuille Google en ajoutant les en-têtes si elle est vide.
 *
 * @param {Object} sheetFormulaire - La feuille Google Sheets sur laquelle écrire.
 * @param {Array} headers - Les en-têtes à ajouter si la feuille est vide.
 * @return {null} - Retourne null en cas d'erreur.
 */
/**
 * @deprecated Migration BigQuery: initialisation de feuille héritée.
 */
function prepareSheet(sheetFormulaire, headers) {
  return resolveSnapshotService().prepareSheet(sheetFormulaire, headers);
}


/**
 * Retourne les indices des colonnes correspondantes aux valeurs dans le tableau de données.
 *
 * @param {Array} values - Le tableau des valeurs.
 * @param {Array} headers - Le tableau des entêtes.
 * @param {Array} existingHeaders - Les entêtes existantes dans la feuille de calcul.
 * @param {Object} sheetEnCours - La feuille de calcul en cours.
 * @return {Array|null} - Le tableau des indices de colonnes ou null en cas d'erreur.
 */
/**
 * @deprecated Migration BigQuery: recherche/ajout de colonnes dans le Sheet.
 */
function getColumnIndices(values, headers, existingHeaders, sheetEnCours) {
  return resolveSnapshotService().getColumnIndices(values, headers, existingHeaders, sheetEnCours);
}

/**
 * Retourne un tableau de valeurs de rangées adaptées à l'écriture dans la feuille de calcul.
 *
 * @param {Object} spreadsheetBdD - La feuille de calcul de la base de données.
 * @param {Array} values - Le tableau des valeurs.
 * @param {Array} columnIndices - Les indices de colonnes correspondants aux valeurs.
 * @param {Array} donneeBaseEnregistrement - Les données de base pour l'enregistrement.
 * @param {Array} tabFields - Les champs du tableau.
 * @param {Object} formulaire - Le formulaire.
 * @param {Object} dataResponse - Les données de réponse.
 * @return {Array|null} - Le tableau des valeurs de rangées ou null en cas d'erreur.
 */
/**
 * @deprecated Migration BigQuery: logique d’alignement colonnes pour Sheets.
 */
function prepareDataToRowFormat(spreadsheetBdD, values, columnIndices, baseData, tabFields, formulaire, dataResponse, medias) {
  return resolveSnapshotService().prepareDataToRowFormat(
    spreadsheetBdD,
    values,
    columnIndices,
    baseData,
    tabFields,
    formulaire,
    dataResponse,
    medias
  );
}
