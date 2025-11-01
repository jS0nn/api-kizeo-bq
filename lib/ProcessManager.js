// ProcessManager Version 0.4.0

function resolveExecutionTargets(overrides) {
  var base = {
    bigQuery: true,
    externalLists: true
  };
  if (overrides && typeof overrides === 'object') {
    return Object.assign({}, base, overrides);
  }
  return base;
}

function prepareFormulaireForRun(formulaire, action, existingConfig) {
  var target = formulaire || {};
  target.action = action;

  var rawTableName = target.tableName ? String(target.tableName).trim() : '';
  var configTableName =
    existingConfig && (existingConfig.bq_table_name || existingConfig.bq_alias)
      ? String(existingConfig.bq_table_name || existingConfig.bq_alias).trim()
      : '';
  var aliasCandidate = target.alias ? String(target.alias).trim() : '';
  var tableNameSource = rawTableName || configTableName || aliasCandidate || target.nom;

  if (typeof bqComputeTableName === 'function') {
    var computedTableName = bqComputeTableName(target.id, target.nom, tableNameSource);
    target.tableName = computedTableName;
    if (typeof bqExtractAliasPart === 'function') {
      target.alias = bqExtractAliasPart(computedTableName, target.id);
    }
  }

  return target;
}

function ensureBigQueryForForm(formulaire, services, targets, log) {
  var resolvedTargets = resolveExecutionTargets(targets);
  if (resolvedTargets.bigQuery === false) {
    log('processData: cible BigQuery désactivée, préparation sautée.');
    return null;
  }

  var bigQueryServices = services.bigQuery || {};
  var getConfigFn =
    typeof bigQueryServices.getConfig === 'function'
      ? bigQueryServices.getConfig
      : typeof getBigQueryConfig === 'function'
      ? getBigQueryConfig
      : null;

  if (!getConfigFn) {
    log('processData: getBigQueryConfig indisponible, préparation BigQuery ignorée.');
    return null;
  }

  var config = null;
  try {
    config = getConfigFn({ throwOnMissing: true });
  } catch (configError) {
    log('processData: configuration BigQuery invalide -> ' + configError.message);
    if (typeof handleException === 'function') {
      handleException('processData.getBigQueryConfig', configError, { formId: formulaire.id });
    }
    return null;
  }

  log(
    'processData BigQuery config -> project=' +
      config.projectId +
      ', dataset=' +
      config.datasetId +
      ', location=' +
      (config.location || 'default')
  );

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

  if (ensureDatasetFn) {
    try {
      ensureDatasetFn(config);
      log('processData: dataset prêt -> ' + config.projectId + '.' + config.datasetId);
    } catch (datasetError) {
      if (typeof handleException === 'function') {
        handleException('processData.ensureDataset', datasetError, { formId: formulaire.id });
      }
    }
  }

  if (ensureRawTableFn) {
    try {
      ensureRawTableFn(config);
      var rawTableId = typeof BQ_RAW_TABLE_ID !== 'undefined' ? BQ_RAW_TABLE_ID : 'kizeo_raw_events';
      log('processData: table raw prête -> ' + rawTableId);
    } catch (rawError) {
      if (typeof handleException === 'function') {
        handleException('processData.ensureRaw', rawError, { formId: formulaire.id });
      }
    }
  }

  if (ensureParentTableFn && typeof bqParentTableId === 'function') {
    try {
      var parentTableId = bqParentTableId(formulaire);
      formulaire.tableName = parentTableId;
      if (typeof bqExtractAliasPart === 'function') {
        formulaire.alias = bqExtractAliasPart(parentTableId, formulaire.id);
      }
      ensureParentTableFn(config, parentTableId);
      log('processData: table parent prête -> ' + parentTableId);
    } catch (parentError) {
      if (typeof handleException === 'function') {
        handleException('processData.ensureParent', parentError, { formId: formulaire.id });
      }
    }
  }

  return config;
}

function buildProcessResult(medias, overrides, nowProvider) {
  var defaultTimestamp = resolveIsoTimestamp(typeof nowProvider === 'function' ? nowProvider : null);
  var base = {
    medias: Array.isArray(medias) ? medias : [],
    latestRecord: null,
    lastSnapshot: null,
    rowCount: 0,
    runTimestamp: defaultTimestamp,
    metadataUpdateStatus: 'SKIPPED',
    status: 'UNKNOWN'
  };
  var merged = Object.assign({}, base, overrides || {});

  if (!Array.isArray(merged.medias)) {
    merged.medias = Array.isArray(medias) ? medias : [];
  }
  if (merged.rowCount !== null) {
    if (typeof merged.rowCount !== 'number' || Number.isNaN(merged.rowCount)) {
      merged.rowCount = 0;
    }
  }
  if (!merged.runTimestamp) {
    merged.runTimestamp = defaultTimestamp;
  }
  if (!merged.metadataUpdateStatus) {
    merged.metadataUpdateStatus = 'SKIPPED';
  }
  if (!merged.status) {
    merged.status = 'UNKNOWN';
  }

  return merged;
}

function fetchUnreadResponses(fetchFn, formulaire, apiPath, hasPreviousRun, unreadPayload, log) {
  var resolution = resolveUnreadDataset(fetchFn, formulaire, apiPath, hasPreviousRun, unreadPayload || null, log);
  if (!resolution) {
    return { state: 'ERROR', status: 'INVALID' };
  }
  if (resolution.type === 'INVALID') {
    return { state: 'ERROR', status: 'INVALID' };
  }
  if (resolution.type === 'NO_UNREAD') {
    return { state: 'NO_DATA', status: 'NO_UNREAD' };
  }
  if (resolution.type === 'FALLBACK_EMPTY') {
    return { state: 'NO_DATA', status: 'FALLBACK_EMPTY' };
  }
  return { state: 'OK', status: resolution.type, payload: resolution.payload };
}

function ingestResponsesBatch(formulaire, unreadPayload, services, medias, log, targets, apiPath, action) {
  var fetchFn = typeof services.fetch === 'function' ? services.fetch : requeteAPIDonnees;
  var processingResult = collectResponseArtifacts(unreadPayload.data, {
    fetchFn: fetchFn,
    formulaire: formulaire,
    apiPath: apiPath,
    log: log,
    spreadsheetBdD: services.spreadsheet || null,
    medias: medias,
    snapshotService: services.snapshot || null
  });

  if (targets.bigQuery !== false) {
    ingestBigQueryPayloads(formulaire, processingResult.bigQueryContext, medias, services, log);
  } else {
    log(processGetLogPrefix() + ': ingestion BigQuery désactivée pour cette exécution.');
  }

  return processingResult;
}

function finalizeIngestionRun(formulaire, processingResult, services, action, targets, apiPath, log, runTimestamp) {
  var fetchFn = typeof services.fetch === 'function' ? services.fetch : requeteAPIDonnees;

  if (processingResult.processedDataIds.size) {
    markResponsesAsRead(formulaire, action, Array.from(processingResult.processedDataIds), fetchFn);
  }

  if (processingResult.latestRecord) {
    log(
      processGetLogPrefix() +
        ': dernier enregistrement traité : ' +
        (processingResult.latestRecord.id || processingResult.latestRecord._id || 'unknown')
    );
  }

  var metadataUpdateStatus = 'SKIPPED';
  if (targets.externalLists !== false) {
    var externalSync = runExternalListsSync(
      formulaire,
      processingResult.lastSnapshot,
      fetchFn,
      apiPath,
      log
    );
    metadataUpdateStatus = externalSync.metadataUpdateStatus;
  }

  return buildHandledResult(runTimestamp, {
    latestRecord: processingResult.latestRecord,
    lastSnapshot: processingResult.lastSnapshot,
    rowCount: processingResult.bigQueryContext.rawRows.length,
    metadataUpdateStatus: metadataUpdateStatus,
    status: 'INGESTED'
  });
}

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
  var services = null;
  try {
    var opts = options || {};
    services = createIngestionServices(opts.services);
    var log = resolveLogFunction(services.logger);

    var medias = [];
    var batchLimit = resolveBatchLimit(nbFormulairesACharger);
    var apiPath = '/forms/' + formulaire.id + '/data/unread/' + action + '/' + batchLimit + '?includeupdated';

    var existingConfig =
      typeof getFormConfig === 'function' ? getFormConfig(spreadsheetBdD, formulaire.id) || null : null;
    var targets = buildExecutionTargets(existingConfig, opts.targets);

    prepareFormulaireForRun(formulaire, action, existingConfig);

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

    ensureBigQueryForForm(formulaire, services, targets, log);

    var hasPreviousRun = !!(existingConfig && existingConfig.last_data_id);
    var handled = handleResponses(spreadsheetBdD, formulaire, apiPath, action, medias, hasPreviousRun, {
      services: services,
      unreadPayload: opts.unreadPayload || null,
      targets: targets
    });

    if (handled === null) {
      log('processData: handleResponses a retourné null (échec ingestion).');
      return buildProcessResult(
        [],
        { status: 'ERROR', rowCount: null, runTimestamp: null, metadataUpdateStatus: 'FAILED' },
        services.now
      );
    }

    log(
      'Fin de processData -> status=' +
        (handled.status || 'UNKNOWN') +
        ', rowCount=' +
        (handled.rowCount || 0) +
        ', metadata=' +
        handled.metadataUpdateStatus
    );
    return buildProcessResult(medias, handled, services.now);
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('processData', error);
    }
    return buildProcessResult(
      [],
      { status: 'ERROR', rowCount: null, runTimestamp: null, metadataUpdateStatus: 'FAILED' },
      services && services.now
    );
  }
}

function buildHandledResult(runTimestamp, overrides) {
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
}

function handleResponses(spreadsheetBdD, formulaire, apiPath, action, medias, hasPreviousRun, options) {
  try {
    var opts = options || {};
    var services = opts.services || createIngestionServices();
    var targets = resolveExecutionTargets(opts.targets);
    var log = resolveLogFunction(services.logger);
    var fetchFn = typeof services.fetch === 'function' ? services.fetch : requeteAPIDonnees;
    var runTimestamp = resolveIsoTimestamp(services.now);

    var unread = fetchUnreadResponses(fetchFn, formulaire, apiPath, hasPreviousRun, opts.unreadPayload || null, log);
    if (unread.state === 'ERROR') {
      return null;
    }
    if (unread.state === 'NO_DATA') {
      return buildHandledResult(runTimestamp, { status: unread.status || 'NO_DATA' });
    }

    var listeReponses = unread.payload;
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

    var processingResult = ingestResponsesBatch(
      formulaire,
      listeReponses,
      Object.assign({}, services, { spreadsheet: spreadsheetBdD }),
      medias,
      log,
      targets,
      apiPath,
      action
    );

    return finalizeIngestionRun(
      formulaire,
      processingResult,
      services,
      action,
      targets,
      apiPath,
      log,
      runTimestamp
    );
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
