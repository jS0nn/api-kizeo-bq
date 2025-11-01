// ProcessManager Finalize Version 0.1.0

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
      log,
      {
        externalService: services.externalLists || null,
        handleException: processReportException
      }
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
      processReportException('markResponsesAsRead', error, {
        formId: formulaire.id,
        action: action,
        batchSize: batch.length
      });
    }
  }
}
