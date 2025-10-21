/*
  ToDo :
    Optimiser : envoyer le token depuis l'appel de fonction (et eviter de le lire à chaque appel api)
    pour  majListeExterne : ne pas traiter les données mises à jour mais seulement les nvlles données
*/

function resolveLogFunction(loggerCandidate) {
  if (loggerCandidate && typeof loggerCandidate.log === 'function') {
    return loggerCandidate.log.bind(loggerCandidate);
  }
  return console.log.bind(console);
}

function createIngestionServices(overrides) {
  const base = {
    fetch: requeteAPIDonnees,
    now: () => new Date(),
    logger: console,
    bigQuery: {
      getConfig: getBigQueryConfig,
      ensureDataset: bqEnsureDataset,
      ensureRawTable: bqEnsureRawTable,
      ensureParentTable: bqEnsureParentTable,
      ensureSubTable: bqEnsureSubTable,
      ensureMediaTable: bqEnsureMediaTable,
      ingestRawBatch: bqIngestRawKizeoBatch,
      ingestParentBatch: bqIngestParentBatch,
      ingestSubTablesBatch: bqIngestSubTablesBatch,
      ingestMediaBatch: bqIngestMediaBatch
    }
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
    log('handleResponses: ingestion BigQuery désactivée via targets, saut des écritures.');
    return;
  }

  const subTableCount = Object.keys(bigQueryContext.subTables || {}).length;
  log(
    `Total enregistrements récupérés : ${bigQueryContext.rawRows.length} (parentRows=${bigQueryContext.parentRows.length}, tablesFilles=${subTableCount}, medias=${medias.length})`
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
      `Ingestion parent préparée : ${bigQueryContext.parentRows.length} lignes, ${columnDefs.length} colonnes dynamiques.`
    );
    ingestParentFn(formulaire, bigQueryContext.parentRows, columnDefs);
  }

  if (subTableCount) {
    log(`Ingestion tables filles préparée : ${subTableCount} tables.`);
    ingestSubTablesFn(formulaire, bigQueryContext.subTables);
  }

  if (medias.length) {
    log(`Ingestion médias préparée : ${medias.length} éléments.`);
    ingestMediaFn(formulaire, medias);
  }
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
    log(`handleResponses: récupération détail data_id=${recordSummaryId}`);
    const detailPath = `/forms/${formulaire.id}/data/${recordSummaryId}`;
    const response = fetchFn('GET', detailPath);
    const payload = response ? response.data : null;
    if (!payload || !payload.data) {
      log(
        `handleResponses: impossible de récupérer les détails pour data_id=${
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
    `handleResponses: analyse réponse unread -> hasData=${unreadInfo.hasData}, type=${unreadInfo.type}, keys=${unreadInfo.keys}, isArray=${unreadInfo.isArray}, status=${unreadInfo.status}, length=${unreadInfo.length}`
  );

  const unreadArray = getArrayFromPayload(unreadPayload);
  if (!unreadArray) {
    log(
      `handleResponses: réponse "unread" inattendue (status=${unreadInfo.status}, code=${unreadResponseCode || 'n/a'})`
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
    log(`handleResponses: aucune donnée non lue pour form=${formulaire.id}.`);
    return {
      type: 'NO_UNREAD',
      payload: unreadPayload
    };
  }

  log(
    `handleResponses: aucune donnée non lue pour form=${formulaire.id}. Tentative de chargement complet via data/all.`
  );
  const fullResponse = fetchFn('GET', `/forms/${formulaire.id}/data/all`);
  const fallbackPayload = fullResponse ? fullResponse.data : null;
  const fallbackArray = getArrayFromPayload(fallbackPayload) || [];

  if (!fallbackArray.length) {
    const fallbackInfo = describePayload(fallbackPayload);
    log(
      `handleResponses: fallback data/all sans enregistrements (status=${fallbackInfo.status}). keys=${fallbackInfo.keys}`
    );
    return {
      type: 'FALLBACK_EMPTY',
      payload: fallbackPayload
    };
  }

  const fallbackInfo = describePayload(fallbackPayload);
  log(
    `handleResponses: récupération fallback réussie (${fallbackArray.length} enregistrements via data/all). Status=${fallbackInfo.status}, type=${fallbackInfo.type}, keys=${fallbackInfo.keys}`
  );
  if (fallbackArray[0] && typeof fallbackArray[0] === 'object') {
    const firstKeys = Object.keys(fallbackArray[0]).slice(0, 15);
    log(`handleResponses: aperçu fallback[0] keys=${firstKeys}`);
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

    const bigQueryContext = {
      rawRows: [],
      parentRows: [],
      parentColumns: {},
      subTables: {}
    };
    const processedDataIds = new Set();
    let latestRecord = null;
    let lastSnapshot = null;

    listeReponses.data.forEach((rep) => {
      const recordSummaryId = rep?._id || rep?.id || rep?.data_id;
      if (!recordSummaryId) {
        log('handleResponses: enregistrement sans identifiant, passage.');
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

      lastSnapshot = buildRowSnapshot(spreadsheetBdD, formulaire, recordData, medias) || lastSnapshot;

      const finalDataId = recordData.id || recordData._id || recordSummaryId;
      if (finalDataId !== undefined && finalDataId !== null && finalDataId !== '') {
        processedDataIds.add(String(finalDataId));
      } else {
        log(
          `handleResponses: impossible de déterminer l'ID à marquer comme lu (form=${formulaire.id}, record=${recordSummaryId})`
        );
      }
    });

    ingestBigQueryPayloads(formulaire, bigQueryContext, medias, services, targets, log);

    if (processedDataIds.size) {
      markResponsesAsRead(formulaire, action, Array.from(processedDataIds), fetchFn);
    }
    if (latestRecord) {
      log(`Dernier enregistrement traité : ${latestRecord.id || latestRecord._id || 'unknown'}`);
    }

    if (targets.externalLists !== false && lastSnapshot) {
      const finalUnreadResponse = fetchFn('GET', apiPath);
      const finalUnread = finalUnreadResponse ? finalUnreadResponse.data : null;
      if (Array.isArray(finalUnread?.data) && finalUnread.data.length === 0) {
        try {
          const listeAjour = majListeExterne(formulaire, lastSnapshot);
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
      }
    } else if (targets.externalLists === false) {
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
    console.log('markResponsesAsRead: paramètres manquants, abandon.');
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
        `markResponsesAsRead: ${batch.length} réponses marquées comme lues (form=${formulaire.id}, action=${action}, code=${response?.responseCode || 'n/a'})`
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
function saveDataToSheet(spreadsheetBdD, dataResponse, formulaire, sheetFormulaire, medias) {
  try {
    const start = new Date();
    const [headers, values, baseData, tabFields] = prepareDataForSheet(dataResponse);
    if (prepareSheet(sheetFormulaire, headers) === null) return null;
    const existingHeaders = sheetFormulaire.getRange(1, 1, 1, sheetFormulaire.getLastColumn()).getValues()[0];
    const columnIndices = getColumnIndices(values, headers, existingHeaders, sheetFormulaire);
    if (columnIndices === null) return null;

    const rowValues = prepareDataToRowFormat(
      spreadsheetBdD,
      values,
      columnIndices,
      baseData,
      tabFields,
      formulaire,
      dataResponse,
      medias // <- collecteur
    );

    sheetFormulaire.appendRow(rowValues);

    const duree = new Date() - start;
    console.log(`Durée saveDataToSheet : ${duree}`);

    return { rowEnCours: rowValues, existingHeaders };
  } catch (e) {
    handleException('saveDataToSheet', e);
    return null;
  }
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
  try {
    // Extraction des données de base de la réponse
    const baseResponseData = [
      dataResponse.form_id,
      dataResponse.form_unique_id,
      dataResponse.id,
      dataResponse.user_id,
      dataResponse.last_name,
      dataResponse.first_name,
      dataResponse.answer_time,
      dataResponse.update_time,
      dataResponse.origin_answer
    ];

    // Obtention des champs de l'objet dataResponse
    const kizeoData = getDataFromFields(dataResponse);   //[Nom du champ,type,valeur]
    if(kizeoData===null){
      return null;
    }

    // Préparation des en-têtes et des valeurs à insérer dans la feuille de calcul
    const headers = [
      "form_id",
      "form_unique_id",
      "id",
      "user_id",
      "last_name",
      "first_name",
      "answer_time",
      "update_time",
      "origin_answer",
      ...kizeoData[0]
    ];
    const values = [...baseResponseData, ...kizeoData[2].map(value => isNumeric(value) ? parseFloat(value) : value)];

    return [headers, values,baseResponseData,kizeoData];
  } catch (e) {
    // Gestion des erreurs avec la fonction handleException
    handleException('prepareDataForSheet', e);
    return null;
  }
}

function buildRowSnapshot(spreadsheetBdD, formulaire, dataResponse, medias) {
  try {
    const prepared = prepareDataForSheet(dataResponse);
    if (prepared === null) return null;
    const [headers, values, baseData, tabFields] = prepared;
    const baseLength = baseData.length;

    for (let i = 0; i < tabFields[0].length; i++) {
      const fieldName = tabFields[0][i];
      const fieldTypeRaw = tabFields[1][i];
      const fieldValue = tabFields[2][i];
      const targetIndex = baseLength + i;

      if (isSubformField(fieldTypeRaw, fieldValue)) {
        const subformRows = normalizeSubformRows(fieldValue);
        values[targetIndex] = JSON.stringify(subformRows);
        continue;
      }

      const fieldType = (fieldTypeRaw || '').toString().toLowerCase();
      if (fieldType === 'photo' || fieldType === 'signature') {
        const mediaInfo = gestionChampImage(
          formulaire.id,
          dataResponse.id,
          fieldName,
          fieldValue,
          spreadsheetBdD,
          { fieldType: fieldType }
        );
        values[targetIndex] = mediaInfo && mediaInfo.formula ? mediaInfo.formula : '';
        if (medias && mediaInfo && Array.isArray(mediaInfo.files) && mediaInfo.files.length) {
          mediaInfo.files.forEach((fileMeta) => {
            const displayName = [fieldName, fileMeta.mediaId, fileMeta.fileId]
              .filter((part) => part && part !== '')
              .join('_');
            medias.push({
              dataId: dataResponse.id,
              formId: formulaire.id,
              formName: formulaire.nom,
              formUniqueId: dataResponse.form_unique_id || '',
              fieldName,
              fieldType,
              mediaId: fileMeta.mediaId || '',
              fileName: fileMeta.fileName,
              driveFileId: fileMeta.fileId,
              driveUrl: fileMeta.driveUrl,
              driveViewUrl: fileMeta.driveViewUrl || '',
              drivePublicUrl:
                fileMeta.drivePublicUrl ||
                normalizeDrivePublicUrl(fileMeta.driveUrl || fileMeta.driveViewUrl || ''),
              folderId: fileMeta.folderId || '',
              folderUrl: fileMeta.folderUrl || '',
              id: mediaInfo.formula,
              formula: mediaInfo.formula,
              name: displayName,
              parentAnswerTime: dataResponse.answer_time || dataResponse._answer_time || '',
              parentUpdateTime: dataResponse.update_time || dataResponse._update_time || ''
            });
          });
        }
        continue;
      }

      if (Array.isArray(fieldValue)) {
        values[targetIndex] = fieldValue.map((v) => (v === null || v === undefined ? '' : v)).join(', ');
      } else if (fieldValue && typeof fieldValue === 'object') {
        values[targetIndex] = JSON.stringify(fieldValue);
      } else {
        values[targetIndex] = fieldValue;
      }
    }

    return { existingHeaders: headers, rowEnCours: values };
  } catch (e) {
    handleException('buildRowSnapshot', e);
    return null;
  }
}


/**
 * Prépare un tableau contenant les champs des données de réponse de l'API.
 *
 * @param {Object} dataResponse - Les données de réponse.
 * @return {Array|null} - Le tableau des champs ou null en cas d'erreur.
 */
function getDataFromFields(dataResponse) {
  try {
    let fieldsData = [[], [], []];  // Initialisation du tableau Fields
    let i = 0;
    for (let champ in dataResponse.fields) { // Parcours des champs de dataResponse
      fieldsData[0][i] = champ;               // Nom du champ
      fieldsData[1][i] = dataResponse.fields[champ].type;  // Type du champ
      fieldsData[2][i] = dataResponse.fields[champ].value; // Valeur du champ
      i++;
    }
    return fieldsData;
  } catch (e) {
    handleException('getDataFromFields', e);
    return null;
  }
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
  try {
    // Si la feuille est vide, ajoutez les en-têtes
    if (sheetFormulaire.getLastRow() === 0) {
      sheetFormulaire.getRange(1, 1, 1, headers.length).setValues([headers]);
    }
    return "sheet preparee"
  } catch (e) {
    handleException('prepareSheet', e);
    return null;
  }
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
  try{
    return values.map((value, index) => {
      const headerIndex = existingHeaders.indexOf(headers[index]);
      if (headerIndex === -1) {
        // Si l'entête n'est pas encore dans la feuille, on l'ajoute à la fin
        sheetEnCours.getRange(1, sheetEnCours.getLastColumn() + 1).setValue(headers[index]);
        return sheetEnCours.getLastColumn();
      }
      // On retourne l'indice de la colonne (+1 car l'indexation commence à 0)
      return headerIndex + 1;
    });
  }catch (e) {
    handleException('getColumnIndices', e);
    return null;
  }
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
  try {
    const rowValues = [];
    const dataId = dataResponse.id; // utile pour medias

    const baseLength = baseData.length;

    for (let i = 0; i < values.length; i++) {
      if (i < baseLength) {
        rowValues[columnIndices[i] - 1] = values[i];
        continue;
      }

      const offset = i - baseLength;
      const fieldName = tabFields[0][offset];
      const fieldTypeRaw = tabFields[1][offset];
      const fieldValue = tabFields[2][offset];
      const columnIndex = columnIndices[i] - 1;

      if (isSubformField(fieldTypeRaw, fieldValue)) {
        const subformRows = normalizeSubformRows(fieldValue);
        rowValues[columnIndex] = subformRows.length
          ? gestionTableaux(spreadsheetBdD, formulaire, dataId, fieldName, subformRows)
          : '';
        continue;
      }

      const fieldType = (fieldTypeRaw || '').toString().toLowerCase();

      if (fieldType === 'photo' || fieldType === 'signature') {
        const mediaInfo = gestionChampImage(
          formulaire.id,
          dataId,
          fieldName,
          fieldValue,
          spreadsheetBdD,
          { fieldType: fieldType }
        );
        const formula = mediaInfo && mediaInfo.formula ? mediaInfo.formula : '';
        rowValues[columnIndex] = formula;

        if (mediaInfo && Array.isArray(mediaInfo.files) && mediaInfo.files.length) {
          mediaInfo.files.forEach((fileMeta) => {
            const displayName = [fieldName, fileMeta.mediaId, fileMeta.fileId]
              .filter((part) => part && part !== '')
              .join('_');
            medias.push({
              dataId,
              formId: formulaire.id,
              formName: formulaire.nom,
              formUniqueId: dataResponse.form_unique_id || '',
              fieldName,
              fieldType,
              mediaId: fileMeta.mediaId || '',
              fileName: fileMeta.fileName,
              driveFileId: fileMeta.fileId,
              driveUrl: fileMeta.driveUrl,
              driveViewUrl: fileMeta.driveViewUrl || '',
              drivePublicUrl:
                fileMeta.drivePublicUrl ||
                normalizeDrivePublicUrl(fileMeta.driveUrl || fileMeta.driveViewUrl || ''),
              folderId: fileMeta.folderId || '',
              folderUrl: fileMeta.folderUrl || '',
              id: formula,
              formula: formula,
              name: displayName,
              parentAnswerTime: dataResponse.answer_time || dataResponse._answer_time || '',
              parentUpdateTime: dataResponse.update_time || dataResponse._update_time || ''
            });
          });
        }
        continue;
      }

      if (Array.isArray(fieldValue)) {
        rowValues[columnIndex] = fieldValue
          .map((v) => (v === null || v === undefined ? '' : v))
          .join(', ');
        continue;
      }

      if (fieldValue && typeof fieldValue === 'object') {
        rowValues[columnIndex] = JSON.stringify(fieldValue);
        continue;
      }

      rowValues[columnIndex] = values[i];
    }
    return rowValues;
  } catch (e) {
    handleException('prepareDataToRowFormat', e);
    return null;
  }
}
