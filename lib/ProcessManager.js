// ProcessManager Version 0.1.0
(function (global) {
  if (typeof ProcessManager !== 'undefined') {
    return;
  }

  const DEFAULT_DATA_LOG_PREFIX = 'lib:Data';

  function getLogPrefix() {
    return typeof global.DATA_LOG_PREFIX === 'string' ? global.DATA_LOG_PREFIX : DEFAULT_DATA_LOG_PREFIX;
  }

  function resolveLogFunction(loggerCandidate) {
    if (loggerCandidate && typeof loggerCandidate.log === 'function') {
      return loggerCandidate.log.bind(loggerCandidate);
    }
    return console.log.bind(console);
  }

  function createIngestionServices(overrides) {
    const defaultFetch =
      typeof global.KizeoClient !== 'undefined' && typeof global.KizeoClient.requeteAPIDonnees === 'function'
        ? global.KizeoClient.requeteAPIDonnees
        : global.requeteAPIDonnees;

    const defaultBigQuery =
      typeof global.BigQueryService !== 'undefined' &&
      global.BigQueryService &&
      typeof global.BigQueryService.getConfig === 'function'
        ? global.BigQueryService
        : {
            getConfig: global.getBigQueryConfig,
            ensureDataset: global.bqEnsureDataset,
            ensureRawTable: global.bqEnsureRawTable,
            ensureParentTable: global.bqEnsureParentTable,
            ensureSubTable: global.bqEnsureSubTable,
            ensureMediaTable: global.bqEnsureMediaTable,
            ensureColumns: global.bqEnsureColumns,
            ingestRawBatch: global.bqIngestRawKizeoBatch,
            ingestParentBatch: global.bqIngestParentBatch,
            ingestSubTablesBatch: global.bqIngestSubTablesBatch,
            ingestMediaBatch: global.bqIngestMediaBatch,
            recordAudit: global.bqRecordAudit,
            runDeduplicationForForm: global.bqRunDeduplicationForForm
          };

    const defaultSnapshot = typeof global.SheetSnapshot !== 'undefined' ? global.SheetSnapshot : null;

    const base = {
      fetch: defaultFetch,
      now: () => new Date(),
      logger: console,
      bigQuery: Object.assign({}, defaultBigQuery),
      snapshot: defaultSnapshot
    };

    if (!overrides) {
      return base;
    }

    return {
      fetch: overrides.fetch || base.fetch,
      now: overrides.now || base.now,
      logger: overrides.logger || base.logger,
      bigQuery: Object.assign({}, base.bigQuery, overrides.bigQuery || {}),
      snapshot: overrides.snapshot || base.snapshot
    };
  }

  function buildExecutionTargets(existingConfig, overrideTargets) {
    const configKey =
      typeof global.CONFIG_INGEST_BIGQUERY_KEY === 'string' && global.CONFIG_INGEST_BIGQUERY_KEY
        ? global.CONFIG_INGEST_BIGQUERY_KEY
        : 'ingest_bigquery';
    const parseBoolean =
      typeof global.parseConfigBooleanFlag === 'function'
        ? global.parseConfigBooleanFlag
        : function (raw, defaultValue) {
            if (raw === null || raw === undefined || raw === '') {
              return !!defaultValue;
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
          };

    const base = {
      bigQuery: parseBoolean(existingConfig ? existingConfig[configKey] : null, true),
      sheet: true,
      externalLists: true
    };

    if (!overrideTargets) {
      return base;
    }

    return Object.assign({}, base, overrideTargets);
  }

  function resolveBatchLimit(rawLimit) {
    const numeric = Number(rawLimit);
    if (!Number.isFinite(numeric) || numeric <= 0) {
      throw new Error(`Batch limit invalide: ${rawLimit}`);
    }
    return Math.floor(numeric);
  }

  function ingestBigQueryPayloads(formulaire, bigQueryContext, medias, services, targets, logFn) {
    const log = typeof logFn === 'function' ? logFn : console.log.bind(console);
    const shouldUseBigQuery = !targets || targets.bigQuery !== false;
    if (!shouldUseBigQuery) {
      log(`${getLogPrefix()}: ingestion BigQuery désactivée via targets, saut des écritures.`);
      return;
    }

    const subTableCount = Object.keys(bigQueryContext.subTables || {}).length;
    log(
      `${getLogPrefix()}: Total enregistrements récupérés : ${bigQueryContext.rawRows.length} (parentRows=${bigQueryContext.parentRows.length}, tablesFilles=${subTableCount}, medias=${medias.length})`
    );

    const bigQueryServices = (services && services.bigQuery) || {};
    const ingestRawFn =
      typeof bigQueryServices.ingestRawBatch === 'function'
        ? bigQueryServices.ingestRawBatch
        : global.bqIngestRawKizeoBatch;
    const ingestParentFn =
      typeof bigQueryServices.ingestParentBatch === 'function'
        ? bigQueryServices.ingestParentBatch
        : global.bqIngestParentBatch;
    const ingestSubTablesFn =
      typeof bigQueryServices.ingestSubTablesBatch === 'function'
        ? bigQueryServices.ingestSubTablesBatch
        : global.bqIngestSubTablesBatch;
    const ingestMediaFn =
      typeof bigQueryServices.ingestMediaBatch === 'function'
        ? bigQueryServices.ingestMediaBatch
        : global.bqIngestMediaBatch;

    if (bigQueryContext.rawRows.length) {
      ingestRawFn(formulaire, bigQueryContext.rawRows);
    }

    if (bigQueryContext.parentRows.length) {
      const columnDefs = Object.keys(bigQueryContext.parentColumns || {}).map(
        (key) => bigQueryContext.parentColumns[key]
      );
      log(
        `${getLogPrefix()}: Ingestion parent préparée : ${bigQueryContext.parentRows.length} lignes, ${columnDefs.length} colonnes dynamiques.`
      );
      ingestParentFn(formulaire, bigQueryContext.parentRows, columnDefs);
    }

    if (subTableCount) {
      log(`${getLogPrefix()}: Ingestion tables filles préparée : ${subTableCount} tables.`);
      ingestSubTablesFn(formulaire, bigQueryContext.subTables);
    }

    if (medias.length) {
      log(`${getLogPrefix()}: Ingestion médias préparée : ${medias.length} éléments.`);
      ingestMediaFn(formulaire, medias);
    }
  }

  function runExternalListsSync(formulaire, snapshot, fetchFn, apiPath, logFn) {
    const log = typeof logFn === 'function' ? logFn : console.log.bind(console);
    if (!snapshot) {
      return { metadataUpdateStatus: 'SKIPPED' };
    }

    const requester = typeof fetchFn === 'function' ? fetchFn : global.requeteAPIDonnees;
    let metadataUpdateStatus = 'SKIPPED';

    try {
      const finalUnreadResponse = requester('GET', apiPath);
      const finalUnread = finalUnreadResponse ? finalUnreadResponse.data : null;
      if (!Array.isArray(finalUnread?.data) || finalUnread.data.length !== 0) {
        return { metadataUpdateStatus };
      }

      try {
        const listeAjour = global.majListeExterne(formulaire, snapshot);
        if (listeAjour === null) {
          log("majListeExterne: mise à jour échouée, on conserve néanmoins le résumé d'ingestion.");
          metadataUpdateStatus = 'FAILED';
        } else {
          metadataUpdateStatus = listeAjour || 'OK';
        }
      } catch (metadataError) {
        global.handleException('handleResponses.majListeExterne', metadataError, {
          formId: formulaire?.id || 'unknown'
        });
        metadataUpdateStatus = 'ERROR';
      }
    } catch (finalUnreadError) {
      global.handleException('handleResponses.finalUnreadCheck', finalUnreadError, {
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
      // ignore -> fallback plus bas
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
          record.created_at
      );
    const currentTs = toTimestamp(current);
    const referenceTs = toTimestamp(reference);
    if (!Number.isFinite(referenceTs)) return current;
    if (!Number.isFinite(currentTs)) return reference;
    return currentTs >= referenceTs ? current : reference;
  }

  function fetchDetailedRecord(fetchFn, formulaire, recordSummaryId, log) {
    try {
      if (typeof log === 'function') {
        log(`${getLogPrefix()}: récupération détail data_id=${recordSummaryId}`);
      }
      const detailPath = `/forms/${formulaire.id}/data/${recordSummaryId}`;
      const response = fetchFn('GET', detailPath);
      const payload = response ? response.data : null;
      if (!payload || !payload.data) {
        log(
          `${getLogPrefix()}: impossible de récupérer les détails pour data_id=${
            recordSummaryId || 'unknown'
          }, responseCode=${response ? response.responseCode : 'n/a'}`
        );
        global.handleException('handleResponses.detail', new Error('Donnée détaillée absente'), {
          formId: formulaire.id,
          dataId: recordSummaryId
        });
        return null;
      }
      return payload.data;
    } catch (e) {
      global.handleException('handleResponses.detail', e, {
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
      `${getLogPrefix()}: analyse réponse unread -> hasData=${unreadInfo.hasData}, type=${unreadInfo.type}, keys=${unreadInfo.keys}, isArray=${unreadInfo.isArray}, status=${unreadInfo.status}, length=${unreadInfo.length}`
    );

    const unreadArray = getArrayFromPayload(unreadPayload);
    if (!unreadArray) {
      log(
        `${getLogPrefix()}: réponse "unread" inattendue (status=${unreadInfo.status}, code=${unreadResponseCode || 'n/a'})`
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
      log(`${getLogPrefix()}: aucune donnée non lue pour form=${formulaire.id}.`);
      return {
        type: 'NO_UNREAD',
        payload: unreadPayload
      };
    }

    log(
      `${getLogPrefix()}: aucune donnée non lue pour form=${formulaire.id}. Tentative de chargement complet via data/all.`
    );
    const fullResponse = fetchFn('GET', `/forms/${formulaire.id}/data/all`);
    const fallbackPayload = fullResponse ? fullResponse.data : null;
    const fallbackArray = getArrayFromPayload(fallbackPayload) || [];

    if (!fallbackArray.length) {
      const fallbackInfo = describePayload(fallbackPayload);
      log(
        `${getLogPrefix()}: fallback data/all sans enregistrements (status=${fallbackInfo.status}). keys=${fallbackInfo.keys}`
      );
      return {
        type: 'FALLBACK_EMPTY',
        payload: fallbackPayload
      };
    }

    const fallbackInfo = describePayload(fallbackPayload);
    log(
      `${getLogPrefix()}: récupération fallback réussie (${fallbackArray.length} enregistrements via data/all). Status=${fallbackInfo.status}, type=${fallbackInfo.type}, keys=${fallbackInfo.keys}`
    );
    if (fallbackArray[0] && typeof fallbackArray[0] === 'object') {
      const firstKeys = Object.keys(fallbackArray[0]).slice(0, 15);
      log(`${getLogPrefix()}: aperçu fallback[0] keys=${firstKeys}`);
    }

    return {
      type: 'FALLBACK_OK',
      payload: {
        status: fallbackPayload && fallbackPayload.status ? fallbackPayload.status : 'fallback_all',
        data: fallbackArray
      }
    };
  }

  function collectResponseArtifacts(records, context) {
    const {
      fetchFn,
      formulaire,
      log,
      spreadsheetBdD,
      medias,
      legacySheetSyncEnabled,
      snapshotService
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

    if (legacySheetSyncEnabled && typeof global.recordLegacyUsage === 'function') {
      global.recordLegacyUsage('legacy_flag_enabled');
    }

    records.forEach((rep) => {
      const recordSummaryId = rep?._id || rep?.id || rep?.data_id;
      if (!recordSummaryId) {
        log(`${getLogPrefix()}: enregistrement sans identifiant, passage.`);
        return;
      }

      const recordData = fetchDetailedRecord(fetchFn, formulaire, recordSummaryId, log);
      if (!recordData) {
        return;
      }

      bigQueryContext.rawRows.push(recordData);
      latestRecord = pickMostRecentRecord(recordData, latestRecord);
      const parentPrepared = global.bqPrepareParentRow(formulaire, recordData);
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

      let snapshot = null;
      if (snapshotService && typeof snapshotService.buildRowSnapshot === 'function') {
        snapshot = snapshotService.buildRowSnapshot(spreadsheetBdD, formulaire, recordData, medias);
      } else if (typeof global.buildRowSnapshot === 'function') {
        snapshot = global.buildRowSnapshot(spreadsheetBdD, formulaire, recordData, medias);
      }
      if (snapshot) {
        if (legacySheetSyncEnabled) {
          let persistedSnapshot = snapshot;
          if (snapshotService && typeof snapshotService.persistSnapshot === 'function') {
            persistedSnapshot = snapshotService.persistSnapshot(spreadsheetBdD, formulaire, snapshot);
          } else if (typeof global.persistLegacySnapshotToSheet === 'function') {
            persistedSnapshot = global.persistLegacySnapshotToSheet(spreadsheetBdD, formulaire, snapshot);
          }
          if (persistedSnapshot) {
            lastSnapshot = persistedSnapshot;
          }
        } else {
          lastSnapshot = snapshot;
        }
      }

      const finalDataId = recordData.id || recordData._id || recordSummaryId;
      if (finalDataId !== undefined && finalDataId !== null && finalDataId !== '') {
        processedDataIds.add(String(finalDataId));
      } else {
        log(
          `${getLogPrefix()}: impossible de déterminer l'ID à marquer comme lu (form=${formulaire.id}, record=${recordSummaryId})`
        );
      }
    });

    return { bigQueryContext, processedDataIds, latestRecord, lastSnapshot };
  }

  function markResponsesAsRead(formulaire, action, dataIds, fetchFn) {
    if (!formulaire || !formulaire.id || !action) {
      console.log(`${getLogPrefix()}: markResponsesAsRead paramètres manquants, abandon.`);
      return;
    }
    if (!Array.isArray(dataIds) || !dataIds.length) return;

    const requester = typeof fetchFn === 'function' ? fetchFn : global.requeteAPIDonnees;
    const chunkSize = 50;

    for (let index = 0; index < dataIds.length; index += chunkSize) {
      const batch = dataIds.slice(index, index + chunkSize).filter((id) => id !== null && id !== undefined && id !== '');
      if (!batch.length) continue;
      try {
        const payload = { data_ids: batch };
        const response = requester('POST', `/forms/${formulaire.id}/markasreadbyaction/${action}`, payload);
        console.log(
          `${getLogPrefix()}: markResponsesAsRead -> ${batch.length} réponses marquées comme lues (form=${formulaire.id}, action=${action}, code=${response?.responseCode || 'n/a'})`
        );
      } catch (e) {
        global.handleException('markResponsesAsRead', e, {
          formId: formulaire.id,
          action: action,
          batchSize: batch.length
        });
      }
    }
  }

  function processData(spreadsheetBdD, formulaire, action, nbFormulairesACharger, options) {
    try {
      const opts = options || {};
      const services = createIngestionServices(opts.services);
      const log = resolveLogFunction(services.logger);

      const medias = [];
      const batchLimit = resolveBatchLimit(nbFormulairesACharger);
      const apiPath = `/forms/${formulaire.id}/data/unread/${action}/${batchLimit}?includeupdated`;

      const existingConfig = global.getFormConfig ? global.getFormConfig(spreadsheetBdD, formulaire.id) || null : null;
      const targets = buildExecutionTargets(existingConfig, opts.targets);
      const rawTableName = formulaire.tableName ? String(formulaire.tableName).trim() : '';
      const configTableName =
        existingConfig && (existingConfig.bq_table_name || existingConfig.bq_alias)
          ? String(existingConfig.bq_table_name || existingConfig.bq_alias).trim()
          : '';
      const aliasCandidate = formulaire.alias ? String(formulaire.alias).trim() : '';
      const tableNameSource = rawTableName || configTableName || aliasCandidate || formulaire.nom;
      const computedTableName = global.bqComputeTableName(formulaire.id, formulaire.nom, tableNameSource);
      formulaire.tableName = computedTableName;
      formulaire.alias = global.bqExtractAliasPart(computedTableName, formulaire.id);
      formulaire.action = action;

      log(
        `processData start -> form_id=${formulaire.id}, action=${action}, table=${computedTableName}, alias=${formulaire.alias}`
      );

      const bigQueryServices = services.bigQuery || {};
      const getBigQueryConfigFn =
        typeof bigQueryServices.getConfig === 'function' ? bigQueryServices.getConfig : global.getBigQueryConfig;
      const ensureDatasetFn =
        typeof bigQueryServices.ensureDataset === 'function' ? bigQueryServices.ensureDataset : global.bqEnsureDataset;
      const ensureRawTableFn =
        typeof bigQueryServices.ensureRawTable === 'function'
          ? bigQueryServices.ensureRawTable
          : global.bqEnsureRawTable;
      const ensureParentTableFn =
        typeof bigQueryServices.ensureParentTable === 'function'
          ? bigQueryServices.ensureParentTable
          : global.bqEnsureParentTable;

      const shouldUseBigQuery = targets.bigQuery !== false;
      let bqConfig = null;
      if (shouldUseBigQuery) {
        try {
          bqConfig = getBigQueryConfigFn({ throwOnMissing: true });
        } catch (configError) {
          log(`processData: configuration BigQuery invalide -> ${configError.message}`);
          global.handleException('processData.getBigQueryConfig', configError, { formId: formulaire.id });
        }
      }

      if (bqConfig && shouldUseBigQuery) {
        log(
          `processData BigQuery config -> project=${bqConfig.projectId}, dataset=${bqConfig.datasetId}, location=${bqConfig.location}`
        );
        try {
          ensureDatasetFn(bqConfig);
          log(`Dataset prêt : ${bqConfig.projectId}.${bqConfig.datasetId}`);
        } catch (e) {
          global.handleException('processData.ensureDataset', e, { formId: formulaire.id });
        }

        try {
          ensureRawTableFn(bqConfig);
          log(`Table raw prête : ${global.BQ_RAW_TABLE_ID}`);
        } catch (e) {
          global.handleException('processData.ensureRaw', e, { formId: formulaire.id });
        }

        try {
          const parentTableId = global.bqParentTableId(formulaire);
          formulaire.tableName = parentTableId;
          formulaire.alias = global.bqExtractAliasPart(parentTableId, formulaire.id);
          ensureParentTableFn(bqConfig, parentTableId);
          log(`Table parent prête : ${parentTableId}`);
        } catch (e) {
          global.handleException('processData.ensureParent', e, { formId: formulaire.id });
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
      global.handleException('processData', e);
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
      const fetchFn = typeof services.fetch === 'function' ? services.fetch : global.requeteAPIDonnees;
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

      const legacySheetSyncEnabled =
        typeof global.isLegacySheetsSyncEnabled === 'function'
          ? global.isLegacySheetsSyncEnabled(targets)
          : targets.sheet !== false;
      const processingResult = collectResponseArtifacts(listeReponses.data, {
        fetchFn,
        formulaire,
        apiPath,
        log,
        spreadsheetBdD,
        medias,
        legacySheetSyncEnabled,
        snapshotService: services.snapshot || null
      });

      const { bigQueryContext, processedDataIds, latestRecord, lastSnapshot } = processingResult;

      ingestBigQueryPayloads(formulaire, bigQueryContext, medias, services, targets, log);

      if (processedDataIds.size) {
        markResponsesAsRead(formulaire, action, Array.from(processedDataIds), fetchFn);
      }
      if (latestRecord) {
        log(`${getLogPrefix()}: dernier enregistrement traité : ${latestRecord.id || latestRecord._id || 'unknown'}`);
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
      global.handleException('handleResponses', e);
      return null;
    }
  }

  const manager = {
    resolveLogFunction,
    createIngestionServices,
    buildExecutionTargets,
    resolveBatchLimit,
    ingestBigQueryPayloads,
    runExternalListsSync,
    resolveIsoTimestamp,
    pickMostRecentRecord,
    fetchDetailedRecord,
    resolveUnreadDataset,
    collectResponseArtifacts,
    processData,
    handleResponses,
    markResponsesAsRead
  };

  global.ProcessManager = manager;

  const exposures = {
    resolveLogFunction,
    createIngestionServices,
    buildExecutionTargets,
    resolveBatchLimit,
    ingestBigQueryPayloads,
    runExternalListsSync,
    resolveIsoTimestamp,
    pickMostRecentRecord,
    fetchDetailedRecord,
    resolveUnreadDataset,
    collectResponseArtifacts,
    processData,
    handleResponses,
    markResponsesAsRead
  };

  Object.keys(exposures).forEach((name) => {
    global[name] = exposures[name];
  });
})(this);
