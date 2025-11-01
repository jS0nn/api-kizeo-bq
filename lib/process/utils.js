// Process utilities for ingestion pipeline
var PROCESS_DEFAULT_DATA_LOG_PREFIX = 'lib:Data';
var PROCESS_UTILS_LOG_PREFIX = 'lib:ProcessUtils';

function utilsReportException(scope, error, context) {
  if (typeof handleException === 'function') {
    handleException(scope, error, context);
    return;
  }

  var message = error && error.message ? error.message : String(error);
  var serializedContext = '';
  if (context) {
    try {
      serializedContext = ' | context=' + JSON.stringify(context);
    } catch (stringifyError) {
      serializedContext = ' | context=<non-serializable>';
    }
  }
  console.error(PROCESS_UTILS_LOG_PREFIX + ':' + scope + ' -> ' + message + serializedContext);
}

function processGetLogPrefix() {
  if (typeof DATA_LOG_PREFIX === 'string' && DATA_LOG_PREFIX) {
    return DATA_LOG_PREFIX;
  }
  return PROCESS_DEFAULT_DATA_LOG_PREFIX;
}

function resolveLogFunction(loggerCandidate) {
  if (loggerCandidate && typeof loggerCandidate.log === 'function') {
    return loggerCandidate.log.bind(loggerCandidate);
  }
  return console.log.bind(console);
}

function createIngestionServices(overrides) {
  var baseFetch =
    (typeof KizeoClient !== 'undefined' && KizeoClient && typeof KizeoClient.requeteAPIDonnees === 'function'
      ? KizeoClient.requeteAPIDonnees
      : typeof requeteAPIDonnees === 'function'
      ? requeteAPIDonnees
      : null);

  function resolveFunction(candidate) {
    return typeof candidate === 'function' ? candidate : undefined;
  }

  var defaultBigQuery = {
    getConfig: resolveFunction(typeof getBigQueryConfig !== 'undefined' ? getBigQueryConfig : undefined),
    ensureDataset: resolveFunction(typeof bqEnsureDataset !== 'undefined' ? bqEnsureDataset : undefined),
    ensureRawTable: resolveFunction(typeof bqEnsureRawTable !== 'undefined' ? bqEnsureRawTable : undefined),
    ensureParentTable: resolveFunction(typeof bqEnsureParentTable !== 'undefined' ? bqEnsureParentTable : undefined),
    ensureSubTable: resolveFunction(typeof bqEnsureSubTable !== 'undefined' ? bqEnsureSubTable : undefined),
    ensureMediaTable: resolveFunction(typeof bqEnsureMediaTable !== 'undefined' ? bqEnsureMediaTable : undefined),
    ensureColumns: resolveFunction(typeof bqEnsureColumns !== 'undefined' ? bqEnsureColumns : undefined),
    ingestRawBatch: resolveFunction(typeof bqIngestRawKizeoBatch !== 'undefined' ? bqIngestRawKizeoBatch : undefined),
    ingestParentBatch: resolveFunction(typeof bqIngestParentBatch !== 'undefined' ? bqIngestParentBatch : undefined),
    ingestSubTablesBatch: resolveFunction(
      typeof bqIngestSubTablesBatch !== 'undefined' ? bqIngestSubTablesBatch : undefined
    ),
    ingestMediaBatch: resolveFunction(typeof bqIngestMediaBatch !== 'undefined' ? bqIngestMediaBatch : undefined),
    recordAudit: resolveFunction(typeof bqRecordAudit !== 'undefined' ? bqRecordAudit : undefined),
    runDeduplicationForForm: resolveFunction(
      typeof bqRunDeduplicationForForm !== 'undefined' ? bqRunDeduplicationForForm : undefined
    ),
    computeTableName: resolveFunction(typeof bqComputeTableName !== 'undefined' ? bqComputeTableName : undefined),
    extractAliasPart: resolveFunction(typeof bqExtractAliasPart !== 'undefined' ? bqExtractAliasPart : undefined),
    parentTableId: resolveFunction(typeof bqParentTableId !== 'undefined' ? bqParentTableId : undefined),
    prepareParentRow: resolveFunction(typeof bqPrepareParentRow !== 'undefined' ? bqPrepareParentRow : undefined),
    prepareSubformRows: resolveFunction(
      typeof bqPrepareSubformRows !== 'undefined' ? bqPrepareSubformRows : undefined
    ),
    prepareMediaRows: resolveFunction(typeof bqPrepareMediaRows !== 'undefined' ? bqPrepareMediaRows : undefined)
  };

  var defaultSnapshot =
    typeof FormResponseSnapshot !== 'undefined'
      ? FormResponseSnapshot
      : typeof ExternalSnapshot !== 'undefined'
      ? ExternalSnapshot
      : null;

  var defaultExternalLists =
    typeof ExternalListsService !== 'undefined' && ExternalListsService ? ExternalListsService : null;

  function ensureBigQueryFunction(name, fn) {
    if (typeof fn === 'function') {
      return fn;
    }
    return function () {
      throw new Error('services.bigQuery.' + name + ' indisponible');
    };
  }

  function buildBigQuery(overridesBigQuery) {
    var merged = Object.assign({}, defaultBigQuery, overridesBigQuery || {});
    Object.keys(merged).forEach(function (key) {
      merged[key] = ensureBigQueryFunction(key, merged[key]);
    });
    return merged;
  }

  var base = {
    fetch: baseFetch,
    now: function () {
      return new Date();
    },
    logger: console,
    bigQuery: buildBigQuery(),
    snapshot: defaultSnapshot,
    externalLists: defaultExternalLists
  };

  if (!overrides) {
    return base;
  }

  return {
    fetch: overrides.fetch || base.fetch,
    now: overrides.now || base.now,
    logger: overrides.logger || base.logger,
    bigQuery: buildBigQuery(overrides.bigQuery),
    snapshot: overrides.snapshot || base.snapshot,
    externalLists:
      overrides.externalLists !== undefined ? overrides.externalLists : base.externalLists
  };
}

function buildExecutionTargets(existingConfig, overrideTargets) {
  var baseTargets = {
    bigQuery: true,
    externalLists: true
  };
  if (overrideTargets && typeof overrideTargets === 'object') {
    return Object.assign({}, baseTargets, overrideTargets);
  }
  if (existingConfig && typeof existingConfig === 'object' && existingConfig.executionTargets) {
    return Object.assign({}, baseTargets, existingConfig.executionTargets);
  }
  return baseTargets;
}

function resolveBatchLimit(rawLimit) {
  var numeric = Number(rawLimit);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    throw new Error('Batch limit invalide: ' + rawLimit);
  }
  return Math.floor(numeric);
}

function resolveIsoTimestamp(nowProvider) {
  try {
    var candidate = typeof nowProvider === 'function' ? nowProvider() : new Date();
    if (candidate instanceof Date) {
      return candidate.toISOString();
    }
    var parsed = new Date(candidate);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString();
    }
  } catch (error) {
    // ignore -> fallback plus bas
  }
  return new Date().toISOString();
}

function pickMostRecentRecord(current, reference) {
  if (!current) return reference || null;
  if (!reference) return current;
  var toTimestamp = function (record) {
    return Date.parse(
      record.update_time ||
        record._update_time ||
        record.answer_time ||
        record._answer_time ||
        record.timestamp ||
        record.created_at
    );
  };
  var currentTs = toTimestamp(current);
  var referenceTs = toTimestamp(reference);
  if (!Number.isFinite(referenceTs)) return current;
  if (!Number.isFinite(currentTs)) return reference;
  return currentTs >= referenceTs ? current : reference;
}

function fetchDetailedRecord(fetchFn, formulaire, recordSummaryId, log) {
  try {
    if (typeof log === 'function') {
      log(processGetLogPrefix() + ': récupération détail data_id=' + recordSummaryId);
    }
    var detailPath = '/forms/' + formulaire.id + '/data/' + recordSummaryId;
    var response = fetchFn('GET', detailPath);
    var payload = response ? response.data : null;
    if (!payload || !payload.data) {
      if (typeof log === 'function') {
        log(
          processGetLogPrefix() +
            ': impossible de récupérer les détails pour data_id=' +
            (recordSummaryId || 'unknown') +
            ', responseCode=' +
            (response ? response.responseCode : 'n/a')
        );
      }
      utilsReportException('handleResponses.detail', new Error('Donnée détaillée absente'), {
        formId: formulaire.id,
        dataId: recordSummaryId
      });
      return null;
    }
    return payload.data;
  } catch (error) {
    utilsReportException('handleResponses.detail', error, {
      formId: formulaire.id,
      dataId: recordSummaryId
    });
    return null;
  }
}
