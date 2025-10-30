(function (global) {
  if (typeof BigQueryService !== 'undefined') {
    return;
  }

  function expose(name) {
    if (typeof global[name] !== 'function') {
      return undefined;
    }
    return global[name];
  }

  const service = {
    getConfig: expose('getBigQueryConfig'),
    setDefaultConfig: expose('initBigQueryConfig'),
    ensureDataset: expose('bqEnsureDataset'),
    ensureRawTable: expose('bqEnsureRawTable'),
    ensureParentTable: expose('bqEnsureParentTable'),
    ensureSubTable: expose('bqEnsureSubTable'),
    ensureMediaTable: expose('bqEnsureMediaTable'),
    ensureColumns: expose('bqEnsureColumns'),
    safeInsertAll: expose('bqSafeInsertAll'),
    computeTableName: expose('bqComputeTableName'),
    extractAliasPart: expose('bqExtractAliasPart'),
    slugifyIdentifier: expose('bqSlugifyIdentifier'),
    normalizeTableIdentifier: expose('bqNormalizeTableIdentifier'),
    ingestRawBatch: expose('bqIngestRawKizeoBatch'),
    ingestParentBatch: expose('bqIngestParentBatch'),
    ingestSubTablesBatch: expose('bqIngestSubTablesBatch'),
    ingestMediaBatch: expose('bqIngestMediaBatch'),
    recordAudit: expose('bqRecordAudit'),
    runDeduplicationForForm: expose('bqRunDeduplicationForForm'),
    listFormSubTables: expose('bqListFormSubTables'),
    waitForStreamingBufferClear: expose('bqWaitForStreamingBufferClear')
  };

  global.BigQueryService = service;
})(this);
