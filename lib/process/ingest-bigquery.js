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

  if (!services || !services.bigQuery) {
    throw new Error('ingestBigQueryPayloads: services.bigQuery indisponible');
  }

  var bigQueryServices = services.bigQuery;

  if (bigQueryContext.rawRows.length) {
    bigQueryServices.ingestRawBatch(formulaire, bigQueryContext.rawRows);
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
    bigQueryServices.ingestParentBatch(formulaire, bigQueryContext.parentRows, columnDefs);
  }

  if (subTableCount) {
    log(prefix + ': Ingestion tables filles préparée : ' + subTableCount + ' tables.');
    bigQueryServices.ingestSubTablesBatch(formulaire, bigQueryContext.subTables);
  }

  if (medias.length) {
    log(prefix + ': Ingestion médias préparée : ' + medias.length + ' éléments.');
    bigQueryServices.ingestMediaBatch(formulaire, medias);
  }
}
