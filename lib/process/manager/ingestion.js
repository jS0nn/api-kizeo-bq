// ProcessManager Ingestion Version 0.1.0

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
    snapshotService: services.snapshot || null,
    bigQuery: services.bigQuery || {}
  });

  if (targets.bigQuery !== false) {
    ingestBigQueryPayloads(formulaire, processingResult.bigQueryContext, medias, services, log);
  } else {
    log(processGetLogPrefix() + ': ingestion BigQuery désactivée pour cette exécution.');
  }

  return processingResult;
}
