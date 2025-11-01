// Process utilities for ingestion pipeline
var PROCESS_DEFAULT_DATA_LOG_PREFIX = 'lib:Data';

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

  var defaultBigQuery = {
    getConfig: typeof getBigQueryConfig === 'function' ? getBigQueryConfig : undefined,
    ensureDataset: typeof bqEnsureDataset === 'function' ? bqEnsureDataset : undefined,
    ensureRawTable: typeof bqEnsureRawTable === 'function' ? bqEnsureRawTable : undefined,
    ensureParentTable: typeof bqEnsureParentTable === 'function' ? bqEnsureParentTable : undefined,
    ensureSubTable: typeof bqEnsureSubTable === 'function' ? bqEnsureSubTable : undefined,
    ensureMediaTable: typeof bqEnsureMediaTable === 'function' ? bqEnsureMediaTable : undefined,
    ensureColumns: typeof bqEnsureColumns === 'function' ? bqEnsureColumns : undefined,
    ingestRawBatch: typeof bqIngestRawKizeoBatch === 'function' ? bqIngestRawKizeoBatch : undefined,
    ingestParentBatch: typeof bqIngestParentBatch === 'function' ? bqIngestParentBatch : undefined,
    ingestSubTablesBatch: typeof bqIngestSubTablesBatch === 'function' ? bqIngestSubTablesBatch : undefined,
    ingestMediaBatch: typeof bqIngestMediaBatch === 'function' ? bqIngestMediaBatch : undefined,
    recordAudit: typeof bqRecordAudit === 'function' ? bqRecordAudit : undefined,
    runDeduplicationForForm:
      typeof bqRunDeduplicationForForm === 'function' ? bqRunDeduplicationForForm : undefined
  };

  var defaultSnapshot =
    typeof FormResponseSnapshot !== 'undefined'
      ? FormResponseSnapshot
      : typeof ExternalSnapshot !== 'undefined'
      ? ExternalSnapshot
      : null;

  var base = {
    fetch: baseFetch,
    now: function () {
      return new Date();
    },
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
      if (typeof handleException === 'function') {
        handleException('handleResponses.detail', new Error('Donnée détaillée absente'), {
          formId: formulaire.id,
          dataId: recordSummaryId
        });
      }
      return null;
    }
    return payload.data;
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('handleResponses.detail', error, {
        formId: formulaire.id,
        dataId: recordSummaryId
      });
    }
    return null;
  }
}
