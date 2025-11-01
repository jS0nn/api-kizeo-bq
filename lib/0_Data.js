// Version 1.1.0

const DATA_LOG_PREFIX = 'lib:Data';

(function (global) {
  var exportsMap = {
    DATA_LOG_PREFIX: DATA_LOG_PREFIX,
    handleException: typeof handleException === 'function' ? handleException : undefined,
    requeteAPIDonnees: typeof requeteAPIDonnees === 'function' ? requeteAPIDonnees : undefined,
    processData: typeof processData === 'function' ? processData : undefined,
    handleResponses: typeof handleResponses === 'function' ? handleResponses : undefined,
    markResponsesAsRead: typeof markResponsesAsRead === 'function' ? markResponsesAsRead : undefined,
    createIngestionServices:
      typeof createIngestionServices === 'function' ? createIngestionServices : undefined,
    buildExecutionTargets: typeof buildExecutionTargets === 'function' ? buildExecutionTargets : undefined,
    resolveBatchLimit: typeof resolveBatchLimit === 'function' ? resolveBatchLimit : undefined,
    resolveIsoTimestamp: typeof resolveIsoTimestamp === 'function' ? resolveIsoTimestamp : undefined,
    resolveUnreadDataset:
      typeof resolveUnreadDataset === 'function' ? resolveUnreadDataset : undefined,
    collectResponseArtifacts:
      typeof collectResponseArtifacts === 'function' ? collectResponseArtifacts : undefined,
    ingestBigQueryPayloads:
      typeof ingestBigQueryPayloads === 'function' ? ingestBigQueryPayloads : undefined,
    runExternalListsSync:
      typeof runExternalListsSync === 'function' ? runExternalListsSync : undefined,
    initBigQueryConfig: typeof initBigQueryConfig === 'function' ? initBigQueryConfig : undefined,
    ensureBigQueryCoreTables:
      typeof ensureBigQueryCoreTables === 'function' ? ensureBigQueryCoreTables : undefined,
    getBigQueryConfig: typeof getBigQueryConfig === 'function' ? getBigQueryConfig : undefined,
    bqComputeTableName:
      typeof bqComputeTableName === 'function' ? bqComputeTableName : undefined,
    bqExtractAliasPart:
      typeof bqExtractAliasPart === 'function' ? bqExtractAliasPart : undefined,
    bqRunDeduplicationForForm:
      typeof bqRunDeduplicationForForm === 'function' ? bqRunDeduplicationForForm : undefined,
    bqParentTableId: typeof bqParentTableId === 'function' ? bqParentTableId : undefined,
    bqBackfillForm: typeof bqBackfillForm === 'function' ? bqBackfillForm : undefined,
    DriveMediaService:
      typeof DriveMediaService !== 'undefined' ? DriveMediaService : undefined,
    ExternalListsService:
      typeof ExternalListsService !== 'undefined' ? ExternalListsService : undefined,
    SheetInterfaceHelpers:
      typeof SheetInterfaceHelpers !== 'undefined' ? SheetInterfaceHelpers : undefined,
    SheetConfigHelpers:
      typeof SheetConfigHelpers !== 'undefined' ? SheetConfigHelpers : undefined,
    SheetDriveExports:
      typeof SheetDriveExports !== 'undefined' ? SheetDriveExports : undefined,
    FormResponseSnapshot:
      typeof FormResponseSnapshot !== 'undefined' ? FormResponseSnapshot : undefined,
    gestionFeuilles: typeof gestionFeuilles === 'function' ? gestionFeuilles : undefined,
    isNumeric: typeof isNumeric === 'function' ? isNumeric : undefined,
    fetchUnreadResponses:
      typeof fetchUnreadResponses === 'function' ? fetchUnreadResponses : undefined,
    ingestResponsesBatch:
      typeof ingestResponsesBatch === 'function' ? ingestResponsesBatch : undefined,
    finalizeIngestionRun:
      typeof finalizeIngestionRun === 'function' ? finalizeIngestionRun : undefined
  };

  var libKizeo = Object.assign({}, global.libKizeo || {});

  Object.keys(exportsMap).forEach(function (key) {
    var value = exportsMap[key];
    if (value !== undefined && value !== null) {
      libKizeo[key] = value;
    }
  });

  global.libKizeo = libKizeo;
})(this);
