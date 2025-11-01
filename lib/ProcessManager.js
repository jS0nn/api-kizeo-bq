// ProcessManager Version 0.5.0

function processData(spreadsheetBdD, formulaire, action, nbFormulairesACharger, options) {
  var services = null;
  try {
    var opts = options || {};
    services = createIngestionServices(opts.services);
    var log = resolveLogFunction(services.logger);

    var medias = [];
    var batchLimit = resolveBatchLimit(nbFormulairesACharger);
    var apiPath = '/forms/' + formulaire.id + '/data/unread/' + action + '/' + batchLimit + '?includeupdated';

    var existingConfig = null;
    if (
      typeof SheetConfigHelpers !== 'undefined' &&
      SheetConfigHelpers &&
      typeof SheetConfigHelpers.readStoredConfig === 'function'
    ) {
      existingConfig = SheetConfigHelpers.readStoredConfig(spreadsheetBdD, formulaire.id) || null;
    }
    var targets = buildExecutionTargets(existingConfig, opts.targets);

    prepareFormulaireForRun(formulaire, action, existingConfig, services);

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
    processReportException('processData', error);
    return buildProcessResult(
      [],
      { status: 'ERROR', rowCount: null, runTimestamp: null, metadataUpdateStatus: 'FAILED' },
      services && services.now
    );
  }
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
    processReportException('handleResponses', error);
    return null;
  }
}
