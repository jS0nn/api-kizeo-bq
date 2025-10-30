/*
  ToDo :
    Optimiser : envoyer le token depuis l'appel de fonction (et eviter de le lire à chaque appel api)
    pour  majListeExterne : ne pas traiter les données mises à jour mais seulement les nvlles données
*/

const LEGACY_SHEETS_FLAG_PROPERTY = 'ENABLE_LEGACY_SHEETS_SYNC';
const LEGACY_USAGE_PROPERTY_PREFIX = 'LEGACY_USAGE_';
const DATA_LOG_PREFIX = 'lib:Data';

function resolveLogFunction(loggerCandidate) {
  if (loggerCandidate && typeof loggerCandidate.log === 'function') {
    return loggerCandidate.log.bind(loggerCandidate);
  }
  return console.log.bind(console);
}

function createIngestionServices(overrides) {
  const defaultFetch =
    typeof KizeoClient !== 'undefined' && typeof KizeoClient.requeteAPIDonnees === 'function'
      ? KizeoClient.requeteAPIDonnees
      : requeteAPIDonnees;

  const defaultBigQuery =
    typeof BigQueryService !== 'undefined' && BigQueryService && typeof BigQueryService.getConfig === 'function'
      ? BigQueryService
      : {
          getConfig: getBigQueryConfig,
          ensureDataset: bqEnsureDataset,
          ensureRawTable: bqEnsureRawTable,
          ensureParentTable: bqEnsureParentTable,
          ensureSubTable: bqEnsureSubTable,
          ensureMediaTable: bqEnsureMediaTable,
          ensureColumns: bqEnsureColumns,
          ingestRawBatch: bqIngestRawKizeoBatch,
          ingestParentBatch: bqIngestParentBatch,
          ingestSubTablesBatch: bqIngestSubTablesBatch,
          ingestMediaBatch: bqIngestMediaBatch,
          recordAudit: bqRecordAudit,
          runDeduplicationForForm: bqRunDeduplicationForForm
        };

  const base = {
    fetch: defaultFetch,
    now: () => new Date(),
    logger: console,
    bigQuery: Object.assign({}, defaultBigQuery)
  };

  if (!overrides) {
    return base;
  }

  const resolved = {
    fetch: overrides.fetch || base.fetch,
    now: overrides.now || base.now,
    logger: overrides.logger || base.logger,
    bigQuery: Object.assign({}, base.bigQuery, overrides.bigQuery || {})
  };

  return resolved;
}

function getLibraryPropertiesStore() {
  try {
    return PropertiesService.getDocumentProperties();
  } catch (e) {
    console.log(`${DATA_LOG_PREFIX}: DocumentProperties indisponibles (${e})`);
    return PropertiesService.getScriptProperties();
  }
}

function isLegacySheetsSyncEnabled(targets) {
  if (targets && targets.sheet === false) {
    return false;
  }
  try {
    const store = getLibraryPropertiesStore();
    const raw = store.getProperty(LEGACY_SHEETS_FLAG_PROPERTY);
    return parseConfigBooleanFlag(raw, false);
  } catch (e) {
    console.log(`${DATA_LOG_PREFIX}: lecture flag legacy impossible (${e})`);
    return false;
  }
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
  const base = {
    bigQuery: parseConfigBooleanFlag(
      existingConfig ? existingConfig[CONFIG_INGEST_BIGQUERY_KEY] : null,
      true
    ),
    sheet: true,
    externalLists: true
  };

  if (!overrideTargets) {
    return base;
  }

  return Object.assign({}, base, overrideTargets);
}

function ingestBigQueryPayloads(formulaire, bigQueryContext, medias, services, targets, logFn) {
  const log = typeof logFn === 'function' ? logFn : console.log.bind(console);
  const shouldUseBigQuery = !targets || targets.bigQuery !== false;
  if (!shouldUseBigQuery) {
    log(`${DATA_LOG_PREFIX}: ingestion BigQuery désactivée via targets, saut des écritures.`);
    return;
  }

  const subTableCount = Object.keys(bigQueryContext.subTables || {}).length;
  log(
    `${DATA_LOG_PREFIX}: Total enregistrements récupérés : ${bigQueryContext.rawRows.length} (parentRows=${bigQueryContext.parentRows.length}, tablesFilles=${subTableCount}, medias=${medias.length})`
  );

  const bigQueryServices = (services && services.bigQuery) || {};
  const ingestRawFn =
    typeof bigQueryServices.ingestRawBatch === 'function' ? bigQueryServices.ingestRawBatch : bqIngestRawKizeoBatch;
  const ingestParentFn =
    typeof bigQueryServices.ingestParentBatch === 'function'
      ? bigQueryServices.ingestParentBatch
      : bqIngestParentBatch;
  const ingestSubTablesFn =
    typeof bigQueryServices.ingestSubTablesBatch === 'function'
      ? bigQueryServices.ingestSubTablesBatch
      : bqIngestSubTablesBatch;
  const ingestMediaFn =
    typeof bigQueryServices.ingestMediaBatch === 'function' ? bigQueryServices.ingestMediaBatch : bqIngestMediaBatch;

  if (bigQueryContext.rawRows.length) {
    ingestRawFn(formulaire, bigQueryContext.rawRows);
  }

  if (bigQueryContext.parentRows.length) {
    const columnDefs = Object.keys(bigQueryContext.parentColumns || {}).map(
      (key) => bigQueryContext.parentColumns[key]
    );
    log(
      `${DATA_LOG_PREFIX}: Ingestion parent préparée : ${bigQueryContext.parentRows.length} lignes, ${columnDefs.length} colonnes dynamiques.`
    );
    ingestParentFn(formulaire, bigQueryContext.parentRows, columnDefs);
  }

  if (subTableCount) {
    log(`${DATA_LOG_PREFIX}: Ingestion tables filles préparée : ${subTableCount} tables.`);
    ingestSubTablesFn(formulaire, bigQueryContext.subTables);
  }

  if (medias.length) {
    log(`${DATA_LOG_PREFIX}: Ingestion médias préparée : ${medias.length} éléments.`);
    ingestMediaFn(formulaire, medias);
  }
}

function runExternalListsSync(formulaire, snapshot, fetchFn, apiPath, logFn) {
  const log = typeof logFn === 'function' ? logFn : console.log.bind(console);
  if (!snapshot) {
    return { metadataUpdateStatus: 'SKIPPED' };
  }

  const requester = typeof fetchFn === 'function' ? fetchFn : requeteAPIDonnees;
  let metadataUpdateStatus = 'SKIPPED';

  try {
    const finalUnreadResponse = requester('GET', apiPath);
    const finalUnread = finalUnreadResponse ? finalUnreadResponse.data : null;
    if (!Array.isArray(finalUnread?.data) || finalUnread.data.length !== 0) {
      return { metadataUpdateStatus };
    }

    try {
      const listeAjour = majListeExterne(formulaire, snapshot);
      if (listeAjour === null) {
        log("majListeExterne: mise à jour échouée, on conserve néanmoins le résumé d'ingestion.");
        metadataUpdateStatus = 'FAILED';
      } else {
        metadataUpdateStatus = listeAjour || 'OK';
      }
    } catch (metadataError) {
      handleException('handleResponses.majListeExterne', metadataError, {
        formId: formulaire?.id || 'unknown'
      });
      metadataUpdateStatus = 'ERROR';
    }
  } catch (finalUnreadError) {
    handleException('handleResponses.finalUnreadCheck', finalUnreadError, {
      formId: formulaire?.id || 'unknown'
    });
    metadataUpdateStatus = 'ERROR';
  }

  return { metadataUpdateStatus };
}

function resolveIsoTimestamp(nowProvider) {
  try {
    const candidate = typeof nowProvider === 'function' ? nowProvider() : new Date();
    if (candidate instanceof Date) {
      return candidate.toISOString();
    }
    const parsed = new Date(candidate);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString();
    }
  } catch (e) {
    // ignore and fallback à la date courante
  }
  return new Date().toISOString();
}

function pickMostRecentRecord(current, reference) {
  if (!current) return reference || null;
  if (!reference) return current;
  const toTimestamp = (record) =>
    Date.parse(
      record.update_time ||
        record._update_time ||
        record.answer_time ||
        record._answer_time ||
        record.timestamp ||
        0
    );
  const currentUpdate = toTimestamp(current);
  const referenceUpdate = toTimestamp(reference);
  if (Number.isNaN(currentUpdate) && Number.isNaN(referenceUpdate)) {
    return current;
  }
  if (Number.isNaN(referenceUpdate)) {
    return current;
  }
  if (Number.isNaN(currentUpdate)) {
    return reference;
  }
  return currentUpdate >= referenceUpdate ? current : reference;
}

function fetchDetailedRecord(fetchFn, formulaire, recordSummaryId, log) {
  try {
    log(`${DATA_LOG_PREFIX}: récupération détail data_id=${recordSummaryId}`);
    const detailPath = `/forms/${formulaire.id}/data/${recordSummaryId}`;
    const response = fetchFn('GET', detailPath);
    const payload = response ? response.data : null;
    if (!payload || !payload.data) {
      log(
        `${DATA_LOG_PREFIX}: impossible de récupérer les détails pour data_id=${
          recordSummaryId || 'unknown'
        }, responseCode=${response ? response.responseCode : 'n/a'}`
      );
      handleException('handleResponses.detail', new Error('Donnée détaillée absente'), {
        formId: formulaire.id,
        dataId: recordSummaryId
      });
      return null;
    }
    return payload.data;
  } catch (e) {
    handleException('handleResponses.detail', e, {
      formId: formulaire.id,
      dataId: recordSummaryId
    });
    return null;
  }
}

function resolveUnreadDataset(fetchFn, formulaire, apiPath, hasPreviousRun, prefetchedPayload, log) {
  const describePayload = (payload) => {
    if (!payload) {
      return { hasData: false, type: 'undefined', keys: [], isArray: false, status: 'n/a', length: 'n/a' };
    }
    const dataProp = payload.data;
    return {
      hasData: !!payload,
      type: typeof payload,
      keys: typeof payload === 'object' ? Object.keys(payload).slice(0, 10) : [],
      isArray: Array.isArray(payload),
      status: payload.status || 'n/a',
      length: Array.isArray(dataProp) ? dataProp.length : 'n/a'
    };
  };

  const getArrayFromPayload = (payload) => {
    if (!payload) return null;
    if (Array.isArray(payload.data)) return payload.data;
    if (Array.isArray(payload)) return payload;
    return null;
  };

  let unreadPayload = prefetchedPayload || null;
  let unreadResponseCode = null;

  if (!unreadPayload) {
    const unreadResponse = fetchFn('GET', apiPath);
    unreadPayload = unreadResponse ? unreadResponse.data : null;
    unreadResponseCode = unreadResponse ? unreadResponse.responseCode : null;
  }

  const unreadInfo = describePayload(unreadPayload);
    log(
      `${DATA_LOG_PREFIX}: analyse réponse unread -> hasData=${unreadInfo.hasData}, type=${unreadInfo.type}, keys=${unreadInfo.keys}, isArray=${unreadInfo.isArray}, status=${unreadInfo.status}, length=${unreadInfo.length}`
    );

  const unreadArray = getArrayFromPayload(unreadPayload);
  if (!unreadArray) {
    log(
      `${DATA_LOG_PREFIX}: réponse "unread" inattendue (status=${unreadInfo.status}, code=${unreadResponseCode || 'n/a'})`
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
    log(`${DATA_LOG_PREFIX}: aucune donnée non lue pour form=${formulaire.id}.`);
    return {
      type: 'NO_UNREAD',
      payload: unreadPayload
    };
  }

  log(
    `${DATA_LOG_PREFIX}: aucune donnée non lue pour form=${formulaire.id}. Tentative de chargement complet via data/all.`
  );
  const fullResponse = fetchFn('GET', `/forms/${formulaire.id}/data/all`);
  const fallbackPayload = fullResponse ? fullResponse.data : null;
  const fallbackArray = getArrayFromPayload(fallbackPayload) || [];

  if (!fallbackArray.length) {
    const fallbackInfo = describePayload(fallbackPayload);
    log(
      `${DATA_LOG_PREFIX}: fallback data/all sans enregistrements (status=${fallbackInfo.status}). keys=${fallbackInfo.keys}`
    );
    return {
      type: 'FALLBACK_EMPTY',
      payload: fallbackPayload
    };
  }

  const fallbackInfo = describePayload(fallbackPayload);
  log(
    `${DATA_LOG_PREFIX}: récupération fallback réussie (${fallbackArray.length} enregistrements via data/all). Status=${fallbackInfo.status}, type=${fallbackInfo.type}, keys=${fallbackInfo.keys}`
  );
  if (fallbackArray[0] && typeof fallbackArray[0] === 'object') {
    const firstKeys = Object.keys(fallbackArray[0]).slice(0, 15);
    log(`${DATA_LOG_PREFIX}: aperçu fallback[0] keys=${firstKeys}`);
  }

  return {
    type: 'FALLBACK_OK',
    payload: {
      status: fallbackPayload && fallbackPayload.status ? fallbackPayload.status : 'fallback_all',
      data: fallbackArray
    }
  };
}

function resolveBatchLimit(rawLimit) {
  const numeric = Number(rawLimit);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    throw new Error(`Batch limit invalide: ${rawLimit}`);
  }
  return Math.floor(numeric);
}

/**
 * Récupère les enregistrements non lus, les écrit dans le classeur et collecte la liste
 * des médias créés sur Drive pendant l'opération.
 * @return {Object} { medias: [{dataId,name,id}, …] }
 */
function processData(spreadsheetBdD, formulaire, action, nbFormulairesACharger, options) {
  try {
    const opts = options || {};
    const services = createIngestionServices(opts.services);
    const log = resolveLogFunction(services.logger);

    const medias = []; // collection partagée
    const batchLimit = resolveBatchLimit(nbFormulairesACharger);
    const apiPath = `/forms/${formulaire.id}/data/unread/${action}/${batchLimit}?includeupdated`;

    const existingConfig = getFormConfig(spreadsheetBdD, formulaire.id) || null;
    const targets = buildExecutionTargets(existingConfig, opts.targets);
    const rawTableName = formulaire.tableName
      ? String(formulaire.tableName).trim()
      : '';
    const configTableName = existingConfig && (existingConfig.bq_table_name || existingConfig.bq_alias)
      ? String(existingConfig.bq_table_name || existingConfig.bq_alias).trim()
      : '';
    const aliasCandidate = formulaire.alias
      ? String(formulaire.alias).trim()
      : '';
    const tableNameSource = rawTableName || configTableName || aliasCandidate || formulaire.nom;
    const computedTableName = bqComputeTableName(formulaire.id, formulaire.nom, tableNameSource);
    formulaire.tableName = computedTableName;
    formulaire.alias = bqExtractAliasPart(computedTableName, formulaire.id);
    formulaire.action = action;

    log(
      `processData start -> form_id=${formulaire.id}, action=${action}, table=${computedTableName}, alias=${formulaire.alias}`
    );

    const bigQueryServices = services.bigQuery || {};
    const getBigQueryConfigFn =
      typeof bigQueryServices.getConfig === 'function' ? bigQueryServices.getConfig : getBigQueryConfig;
    const ensureDatasetFn =
      typeof bigQueryServices.ensureDataset === 'function' ? bigQueryServices.ensureDataset : bqEnsureDataset;
    const ensureRawTableFn =
      typeof bigQueryServices.ensureRawTable === 'function' ? bigQueryServices.ensureRawTable : bqEnsureRawTable;
    const ensureParentTableFn =
      typeof bigQueryServices.ensureParentTable === 'function'
        ? bigQueryServices.ensureParentTable
        : bqEnsureParentTable;

    const shouldUseBigQuery = targets.bigQuery !== false;
    const bqConfig = shouldUseBigQuery ? getBigQueryConfigFn() : null;

    if (bqConfig && shouldUseBigQuery) {
      log(
        `processData BigQuery config -> project=${bqConfig.projectId}, dataset=${bqConfig.datasetId}, location=${bqConfig.location}`
      );
      try {
        ensureDatasetFn(bqConfig);
        log(`Dataset prêt : ${bqConfig.projectId}.${bqConfig.datasetId}`);
      } catch (e) {
        handleException('processData.ensureDataset', e, { formId: formulaire.id });
      }

      try {
        ensureRawTableFn(bqConfig);
        log(`Table raw prête : ${BQ_RAW_TABLE_ID}`);
      } catch (e) {
        handleException('processData.ensureRaw', e, { formId: formulaire.id });
      }

      try {
        const parentTableId = bqParentTableId(formulaire);
        formulaire.tableName = parentTableId;
        formulaire.alias = bqExtractAliasPart(parentTableId, formulaire.id);
        ensureParentTableFn(bqConfig, parentTableId);
        log(`Table parent prête : ${parentTableId}`);
      } catch (e) {
        handleException('processData.ensureParent', e, { formId: formulaire.id });
      }
    } else if (!shouldUseBigQuery) {
      log('processData: ingestion BigQuery désactivée via targets.');
    } else {
      log('processData: configuration BigQuery indisponible, les tables ne seront pas créées.');
    }

    const hasPreviousRun = !!(existingConfig && existingConfig.last_data_id);
    const handled = handleResponses(
      spreadsheetBdD,
      formulaire,
      apiPath,
      action,
      medias,
      hasPreviousRun,
      {
        services,
        targets,
        unreadPayload: opts.unreadPayload || null
      }
    );

    if (handled === null) {
      log('processData: handleResponses a retourné null (échec ingestion).');
      return {
        medias: [],
        latestRecord: null,
        lastSnapshot: null,
        rowCount: null,
        runTimestamp: null,
        metadataUpdateStatus: 'FAILED',
        status: 'ERROR'
      };
    }
    log(
      `Fin de processData -> status=${handled.status || 'UNKNOWN'}, rowCount=${handled.rowCount || 0}, metadata=${handled.metadataUpdateStatus}`
    );
    return {
      medias,
      latestRecord: handled.latestRecord || null,
      lastSnapshot: handled.lastSnapshot || null,
      rowCount: typeof handled.rowCount === 'number' ? handled.rowCount : 0,
      runTimestamp: handled.runTimestamp || new Date().toISOString(),
      metadataUpdateStatus: handled.metadataUpdateStatus || 'SKIPPED',
      status: handled.status || 'UNKNOWN'
    };
  } catch (e) {
    handleException('processData', e);
    return {
      medias: [],
      latestRecord: null,
      lastSnapshot: null,
      rowCount: null,
      runTimestamp: null,
      metadataUpdateStatus: 'FAILED',
      status: 'ERROR'
    };
  }
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

  ingestBigQueryPayloads(
    formulaire,
    bigQueryContext,
    mediasTarget,
    services,
    {
      bigQuery: true,
      sheet: false,
      externalLists: false
    },
    log
  );

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
  const {
    fetchFn,
    formulaire,
    log,
    spreadsheetBdD,
    medias,
    legacySheetSyncEnabled
  } = context;

  const bigQueryContext = {
    rawRows: [],
    parentRows: [],
    parentColumns: {},
    subTables: {}
  };
  const processedDataIds = new Set();
  let latestRecord = null;
  let lastSnapshot = null;

  if (legacySheetSyncEnabled) {
    recordLegacyUsage('legacy_flag_enabled');
  }

  records.forEach((rep) => {
    const recordSummaryId = rep?._id || rep?.id || rep?.data_id;
    if (!recordSummaryId) {
      log(`${DATA_LOG_PREFIX}: enregistrement sans identifiant, passage.`);
      return;
    }

    const recordData = fetchDetailedRecord(fetchFn, formulaire, recordSummaryId, log);
    if (!recordData) {
      return;
    }

    bigQueryContext.rawRows.push(recordData);
    latestRecord = pickMostRecentRecord(recordData, latestRecord);
    const parentPrepared = bqPrepareParentRow(formulaire, recordData);
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

    const snapshot = buildRowSnapshot(spreadsheetBdD, formulaire, recordData, medias);
    if (snapshot) {
      const persistedSnapshot = legacySheetSyncEnabled
        ? persistLegacySnapshotToSheet(spreadsheetBdD, formulaire, snapshot)
        : snapshot;
      lastSnapshot = persistedSnapshot || lastSnapshot;
    }

    const finalDataId = recordData.id || recordData._id || recordSummaryId;
    if (finalDataId !== undefined && finalDataId !== null && finalDataId !== '') {
      processedDataIds.add(String(finalDataId));
    } else {
      log(
        `${DATA_LOG_PREFIX}: impossible de déterminer l'ID à marquer comme lu (form=${formulaire.id}, record=${recordSummaryId})`
      );
    }
  });

  return { bigQueryContext, processedDataIds, latestRecord, lastSnapshot };
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
  try {
    const opts = options || {};
    const services = opts.services || createIngestionServices();
    const targets = Object.assign(
      {
        bigQuery: true,
        sheet: true,
        externalLists: true
      },
      opts.targets || {}
    );
    const log = resolveLogFunction(services.logger);
    const fetchFn = typeof services.fetch === 'function' ? services.fetch : requeteAPIDonnees;
    const runTimestamp = resolveIsoTimestamp(services.now);
    const buildResult = (overrides) =>
      Object.assign(
        {
          latestRecord: null,
          lastSnapshot: null,
          rowCount: 0,
          runTimestamp,
          metadataUpdateStatus: 'SKIPPED',
          status: 'NO_DATA'
        },
        overrides || {}
      );
    let metadataUpdateStatus = 'SKIPPED';

    const unreadResolution = resolveUnreadDataset(
      fetchFn,
      formulaire,
      apiPath,
      hasPreviousRun,
      opts.unreadPayload || null,
      log
    );

    if (!unreadResolution || unreadResolution.type === 'INVALID') {
      return null;
    }

    if (unreadResolution.type === 'NO_UNREAD') {
      return buildResult({ status: 'NO_UNREAD' });
    }

    if (unreadResolution.type === 'FALLBACK_EMPTY') {
      return buildResult({ status: 'FALLBACK_EMPTY' });
    }

    const listeReponses = unreadResolution.payload;
    if (!listeReponses || !Array.isArray(listeReponses.data)) {
      return null;
    }

    log(
      `Nombre d'enregistrements à traiter : ${listeReponses.data.length} (form=${formulaire.id}, action=${action}, alias=${formulaire.alias})`
    );

    const legacySheetSyncEnabled = isLegacySheetsSyncEnabled(targets);
    const processingResult = collectResponseArtifacts(listeReponses.data, {
      fetchFn,
      formulaire,
      apiPath,
      log,
      spreadsheetBdD,
      medias,
      legacySheetSyncEnabled
    });

    const { bigQueryContext, processedDataIds, latestRecord, lastSnapshot } = processingResult;

    ingestBigQueryPayloads(formulaire, bigQueryContext, medias, services, targets, log);

    if (processedDataIds.size) {
      markResponsesAsRead(formulaire, action, Array.from(processedDataIds), fetchFn);
    }
    if (latestRecord) {
      log(`${DATA_LOG_PREFIX}: dernier enregistrement traité : ${latestRecord.id || latestRecord._id || 'unknown'}`);
    }

    if (targets.externalLists !== false) {
      const externalSync = runExternalListsSync(formulaire, lastSnapshot, fetchFn, apiPath, log);
      metadataUpdateStatus = externalSync.metadataUpdateStatus;
    } else {
      metadataUpdateStatus = 'SKIPPED';
    }

    return buildResult({
      latestRecord,
      lastSnapshot,
      rowCount: bigQueryContext.rawRows.length,
      metadataUpdateStatus,
      status: 'INGESTED'
    });
  } catch (e) {
    handleException('handleResponses', e);
    return null;
  }
}

function markResponsesAsRead(formulaire, action, dataIds, fetchFn) {
  if (!formulaire || !formulaire.id || !action) {
    console.log(`${DATA_LOG_PREFIX}: markResponsesAsRead paramètres manquants, abandon.`);
    return;
  }
  if (!Array.isArray(dataIds) || !dataIds.length) return;

  const requester = typeof fetchFn === 'function' ? fetchFn : requeteAPIDonnees;
  const chunkSize = 50;

  for (let index = 0; index < dataIds.length; index += chunkSize) {
    const batch = dataIds.slice(index, index + chunkSize).filter((id) => id !== null && id !== undefined && id !== '');
    if (!batch.length) continue;
    try {
      const payload = { data_ids: batch };
      const response = requester('POST', `/forms/${formulaire.id}/markasreadbyaction/${action}`, payload);
      console.log(
        `${DATA_LOG_PREFIX}: markResponsesAsRead -> ${batch.length} réponses marquées comme lues (form=${formulaire.id}, action=${action}, code=${response?.responseCode || 'n/a'})`
      );
    } catch (e) {
      handleException('markResponsesAsRead', e, {
        formId: formulaire.id,
        action: action,
        batchSize: batch.length
      });
    }
  }
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
  if (typeof SheetSnapshot === 'undefined') {
    throw new Error('SheetSnapshot indisponible');
  }
  return SheetSnapshot;
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
