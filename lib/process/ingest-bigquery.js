function ingestBigQueryPayloads(formulaire, bigQueryContext, medias, services, logFn) {
  var log = typeof logFn === 'function' ? logFn : console.log.bind(console);
  var prefix = processGetLogPrefix();

  var subTableCount = Object.keys(bigQueryContext.subTables || {}).length;
  log(
    prefix +
      ': Total enregistrements récupérés : ' +
      bigQueryContext.rawRows.length +
      ' (parentRows=' +
      bigQueryContext.parentRows.length +
      ', tablesFilles=' +
      subTableCount +
      ', medias=' +
      medias.length +
      ')'
  );

  var bigQueryServices = (services && services.bigQuery) || {};
  var ingestRawFn =
    typeof bigQueryServices.ingestRawBatch === 'function' ? bigQueryServices.ingestRawBatch : bqIngestRawKizeoBatch;
  var ingestParentFn =
    typeof bigQueryServices.ingestParentBatch === 'function' ? bigQueryServices.ingestParentBatch : bqIngestParentBatch;
  var ingestSubTablesFn =
    typeof bigQueryServices.ingestSubTablesBatch === 'function'
      ? bigQueryServices.ingestSubTablesBatch
      : bqIngestSubTablesBatch;
  var ingestMediaFn =
    typeof bigQueryServices.ingestMediaBatch === 'function' ? bigQueryServices.ingestMediaBatch : bqIngestMediaBatch;

  if (bigQueryContext.rawRows.length) {
    ingestRawFn(formulaire, bigQueryContext.rawRows);
  }

  if (bigQueryContext.parentRows.length) {
    var columnDefs = Object.keys(bigQueryContext.parentColumns || {}).map(function (key) {
      return bigQueryContext.parentColumns[key];
    });
    log(
      prefix +
        ': Ingestion parent préparée : ' +
        bigQueryContext.parentRows.length +
        ' lignes, ' +
        columnDefs.length +
        ' colonnes dynamiques.'
    );
    ingestParentFn(formulaire, bigQueryContext.parentRows, columnDefs);
  }

  if (subTableCount) {
    log(prefix + ': Ingestion tables filles préparée : ' + subTableCount + ' tables.');
    ingestSubTablesFn(formulaire, bigQueryContext.subTables);
  }

  if (medias.length) {
    log(prefix + ': Ingestion médias préparée : ' + medias.length + ' éléments.');
    ingestMediaFn(formulaire, medias);
  }
}
