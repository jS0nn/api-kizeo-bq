// ProcessManager Version 0.3.0

function markResponsesAsRead(formulaire, action, dataIds, fetchFn) {
  if (!formulaire || !formulaire.id || !action) {
    console.log(processGetLogPrefix() + ': markResponsesAsRead paramètres manquants, abandon.');
    return;
  }
  if (!Array.isArray(dataIds) || !dataIds.length) return;

  var requester = typeof fetchFn === 'function' ? fetchFn : requeteAPIDonnees;
  var chunkSize = 50;

  for (var index = 0; index < dataIds.length; index += chunkSize) {
    var batch = dataIds
      .slice(index, index + chunkSize)
      .filter(function (id) {
        return id !== null && id !== undefined && id !== '';
      });
    if (!batch.length) continue;
    try {
      var payload = { data_ids: batch };
      var response = requester('POST', '/forms/' + formulaire.id + '/markasreadbyaction/' + action, payload);
      console.log(
        processGetLogPrefix() +
          ': markResponsesAsRead -> ' +
          batch.length +
          ' réponses marquées comme lues (form=' +
          formulaire.id +
          ', action=' +
          action +
          ', code=' +
          (response && response.responseCode ? response.responseCode : 'n/a') +
          ')'
      );
    } catch (error) {
      if (typeof handleException === 'function') {
        handleException('markResponsesAsRead', error, {
          formId: formulaire.id,
          action: action,
          batchSize: batch.length
        });
      }
    }
  }
}

function processData(spreadsheetBdD, formulaire, action, nbFormulairesACharger, options) {
  try {
    var opts = options || {};
    var services = createIngestionServices(opts.services);
    var log = resolveLogFunction(services.logger);

    var medias = [];
    var batchLimit = resolveBatchLimit(nbFormulairesACharger);
    var apiPath = '/forms/' + formulaire.id + '/data/unread/' + action + '/' + batchLimit + '?includeupdated';

    var existingConfig =
      typeof getFormConfig === 'function' ? getFormConfig(spreadsheetBdD, formulaire.id) || null : null;
    var targets = buildExecutionTargets(existingConfig);
    var rawTableName = formulaire.tableName ? String(formulaire.tableName).trim() : '';
    var configTableName =
      existingConfig && (existingConfig.bq_table_name || existingConfig.bq_alias)
        ? String(existingConfig.bq_table_name || existingConfig.bq_alias).trim()
        : '';
    var aliasCandidate = formulaire.alias ? String(formulaire.alias).trim() : '';
    var tableNameSource = rawTableName || configTableName || aliasCandidate || formulaire.nom;
    if (typeof bqComputeTableName === 'function') {
      var computedTableName = bqComputeTableName(formulaire.id, formulaire.nom, tableNameSource);
      formulaire.tableName = computedTableName;
      if (typeof bqExtractAliasPart === 'function') {
        formulaire.alias = bqExtractAliasPart(computedTableName, formulaire.id);
      }
    }
    formulaire.action = action;

    log(
      'processData start -> form_id=' +
        formulaire.id +
        ', action=' +
        action +
        ', table=' +
        (formulaire.tableName || 'n/a') +
        ', alias=' +
        (formulaire.alias || 'n/a')
    );

    var bigQueryServices = services.bigQuery || {};
    var getBigQueryConfigFn =
      typeof bigQueryServices.getConfig === 'function'
        ? bigQueryServices.getConfig
        : typeof getBigQueryConfig === 'function'
        ? getBigQueryConfig
        : null;
    var ensureDatasetFn =
      typeof bigQueryServices.ensureDataset === 'function'
        ? bigQueryServices.ensureDataset
        : typeof bqEnsureDataset === 'function'
        ? bqEnsureDataset
        : null;
    var ensureRawTableFn =
      typeof bigQueryServices.ensureRawTable === 'function'
        ? bigQueryServices.ensureRawTable
        : typeof bqEnsureRawTable === 'function'
        ? bqEnsureRawTable
        : null;
    var ensureParentTableFn =
      typeof bigQueryServices.ensureParentTable === 'function'
        ? bigQueryServices.ensureParentTable
        : typeof bqEnsureParentTable === 'function'
        ? bqEnsureParentTable
        : null;

    var shouldUseBigQuery = targets.bigQuery !== false;
    var bqConfig = null;
    if (shouldUseBigQuery && getBigQueryConfigFn) {
      try {
        bqConfig = getBigQueryConfigFn({ throwOnMissing: true });
      } catch (configError) {
        log('processData: configuration BigQuery invalide -> ' + configError.message);
        if (typeof handleException === 'function') {
          handleException('processData.getBigQueryConfig', configError, { formId: formulaire.id });
        }
      }
    }

    if (bqConfig && shouldUseBigQuery) {
      log(
        'processData BigQuery config -> project=' +
          bqConfig.projectId +
          ', dataset=' +
          bqConfig.datasetId +
          ', location=' +
          (bqConfig.location || 'default')
      );
      try {
        if (ensureDatasetFn) {
          ensureDatasetFn(bqConfig);
          log('Dataset prêt : ' + bqConfig.projectId + '.' + bqConfig.datasetId);
        }
      } catch (datasetError) {
        if (typeof handleException === 'function') {
          handleException('processData.ensureDataset', datasetError, { formId: formulaire.id });
        }
      }

      try {
        if (ensureRawTableFn) {
          ensureRawTableFn(bqConfig);
          var rawTableId = typeof BQ_RAW_TABLE_ID !== 'undefined' ? BQ_RAW_TABLE_ID : 'kizeo_raw_events';
          log('Table raw prête : ' + rawTableId);
        }
      } catch (rawError) {
        if (typeof handleException === 'function') {
          handleException('processData.ensureRaw', rawError, { formId: formulaire.id });
        }
      }

      try {
        if (ensureParentTableFn && typeof bqParentTableId === 'function') {
          var parentTableId = bqParentTableId(formulaire);
          formulaire.tableName = parentTableId;
          if (typeof bqExtractAliasPart === 'function') {
            formulaire.alias = bqExtractAliasPart(parentTableId, formulaire.id);
          }
          ensureParentTableFn(bqConfig, parentTableId);
          log('Table parent prête : ' + parentTableId);
        }
      } catch (parentError) {
        if (typeof handleException === 'function') {
          handleException('processData.ensureParent', parentError, { formId: formulaire.id });
        }
      }
    } else if (shouldUseBigQuery) {
      log('processData: configuration BigQuery indisponible, les tables ne seront pas créées.');
    }

    var hasPreviousRun = !!(existingConfig && existingConfig.last_data_id);
    var handled = handleResponses(spreadsheetBdD, formulaire, apiPath, action, medias, hasPreviousRun, {
      services: services,
      unreadPayload: opts.unreadPayload || null
    });

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
      'Fin de processData -> status=' +
        (handled.status || 'UNKNOWN') +
        ', rowCount=' +
        (handled.rowCount || 0) +
        ', metadata=' +
        handled.metadataUpdateStatus
    );
    return {
      medias: medias,
      latestRecord: handled.latestRecord || null,
      lastSnapshot: handled.lastSnapshot || null,
      rowCount: typeof handled.rowCount === 'number' ? handled.rowCount : 0,
      runTimestamp: handled.runTimestamp || new Date().toISOString(),
      metadataUpdateStatus: handled.metadataUpdateStatus || 'SKIPPED',
      status: handled.status || 'UNKNOWN'
    };
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('processData', error);
    }
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
    var opts = options || {};
    var services = opts.services || createIngestionServices();
    var targets = {
      bigQuery: true,
      sheet: false,
      externalLists: true
    };
    var log = resolveLogFunction(services.logger);
    var fetchFn = typeof services.fetch === 'function' ? services.fetch : requeteAPIDonnees;
    var runTimestamp = resolveIsoTimestamp(services.now);
    var buildResult = function (overrides) {
      return Object.assign(
        {
          latestRecord: null,
          lastSnapshot: null,
          rowCount: 0,
          runTimestamp: runTimestamp,
          metadataUpdateStatus: 'SKIPPED',
          status: 'NO_DATA'
        },
        overrides || {}
      );
    };
    var metadataUpdateStatus = 'SKIPPED';

    var unreadResolution = resolveUnreadDataset(fetchFn, formulaire, apiPath, hasPreviousRun, opts.unreadPayload || null, log);

    if (!unreadResolution || unreadResolution.type === 'INVALID') {
      return null;
    }

    if (unreadResolution.type === 'NO_UNREAD') {
      return buildResult({ status: 'NO_UNREAD' });
    }

    if (unreadResolution.type === 'FALLBACK_EMPTY') {
      return buildResult({ status: 'FALLBACK_EMPTY' });
    }

    var listeReponses = unreadResolution.payload;
    if (!listeReponses || !Array.isArray(listeReponses.data)) {
      return null;
    }

    log(
      "Nombre d'enregistrements à traiter : " +
        listeReponses.data.length +
        ' (form=' +
        formulaire.id +
        ', action=' +
        action +
        ', alias=' +
        (formulaire.alias || 'n/a') +
        ')'
    );

    var processingResult = collectResponseArtifacts(listeReponses.data, {
      fetchFn: fetchFn,
      formulaire: formulaire,
      apiPath: apiPath,
      log: log,
      spreadsheetBdD: spreadsheetBdD,
      medias: medias,
      snapshotService: services.snapshot || null
    });

    var bigQueryContext = processingResult.bigQueryContext;
    var processedDataIds = processingResult.processedDataIds;
    var latestRecord = processingResult.latestRecord;
    var lastSnapshot = processingResult.lastSnapshot;

    ingestBigQueryPayloads(formulaire, bigQueryContext, medias, services, log);

    if (processedDataIds.size) {
      markResponsesAsRead(formulaire, action, Array.from(processedDataIds), fetchFn);
    }
    if (latestRecord) {
      log(
        processGetLogPrefix() +
          ': dernier enregistrement traité : ' +
          (latestRecord.id || latestRecord._id || 'unknown')
      );
    }

    if (targets.externalLists !== false) {
      var externalSync = runExternalListsSync(formulaire, lastSnapshot, fetchFn, apiPath, log);
      metadataUpdateStatus = externalSync.metadataUpdateStatus;
    } else {
      metadataUpdateStatus = 'SKIPPED';
    }

    return buildResult({
      latestRecord: latestRecord,
      lastSnapshot: lastSnapshot,
      rowCount: bigQueryContext.rawRows.length,
      metadataUpdateStatus: metadataUpdateStatus,
      status: 'INGESTED'
    });
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('handleResponses', error);
    }
    return null;
  }
}

// Expose fonctions principales sans passerelle supplémentaire
this.processData = processData;
this.handleResponses = handleResponses;
this.markResponsesAsRead = markResponsesAsRead;
