// ProcessManager Preparation Version 0.1.0

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

function prepareFormulaireForRun(formulaire, action, existingConfig, services) {
  var target = formulaire || {};
  target.action = action;

  var rawTableName = target.tableName ? String(target.tableName).trim() : '';
  var configTableName =
    existingConfig && (existingConfig.bq_table_name || existingConfig.bq_alias)
      ? String(existingConfig.bq_table_name || existingConfig.bq_alias).trim()
      : '';
  var aliasCandidate = target.alias ? String(target.alias).trim() : '';
  var tableNameSource = rawTableName || configTableName || aliasCandidate || target.nom;

  if (!services || !services.bigQuery) {
    throw new Error('prepareFormulaireForRun: services.bigQuery indisponible');
  }

  var bigQueryServices = services.bigQuery;

  if (typeof bigQueryServices.computeTableName !== 'function') {
    throw new Error('prepareFormulaireForRun: services.bigQuery.computeTableName indisponible');
  }

  var computedTableName = bigQueryServices.computeTableName(target.id, target.nom, tableNameSource);
  target.tableName = computedTableName;

  if (typeof bigQueryServices.extractAliasPart !== 'function') {
    throw new Error('prepareFormulaireForRun: services.bigQuery.extractAliasPart indisponible');
  }

  target.alias = bigQueryServices.extractAliasPart(computedTableName, target.id);

  return target;
}

function ensureBigQueryForForm(formulaire, services, targets, log) {
  var resolvedTargets = resolveExecutionTargets(targets);
  if (resolvedTargets.bigQuery === false) {
    log('processData: cible BigQuery désactivée, préparation sautée.');
    return null;
  }

  var bigQueryServices = (services && services.bigQuery) || {};
  var getConfigFn = bigQueryServices.getConfig;

  var config = null;
  try {
    config = getConfigFn({ throwOnMissing: true });
  } catch (configError) {
    log('processData: configuration BigQuery invalide -> ' + configError.message);
    processReportException('processData.getBigQueryConfig', configError, { formId: formulaire.id });
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

  var ensureDatasetFn = bigQueryServices.ensureDataset;
  if (typeof ensureDatasetFn !== 'function') {
    log('processData: ensureDataset non configuré, étape ignorée.');
  } else {
    try {
      ensureDatasetFn(config);
      log('processData: dataset prêt -> ' + config.projectId + '.' + config.datasetId);
    } catch (datasetError) {
      processReportException('processData.ensureDataset', datasetError, { formId: formulaire.id });
    }
  }

  var ensureRawTableFn = bigQueryServices.ensureRawTable;
  if (typeof ensureRawTableFn !== 'function') {
    log('processData: ensureRawTable non configuré, étape ignorée.');
  } else {
    try {
      ensureRawTableFn(config);
      var rawTableId = typeof BQ_RAW_TABLE_ID !== 'undefined' ? BQ_RAW_TABLE_ID : 'kizeo_raw_events';
      log('processData: table raw prête -> ' + rawTableId);
    } catch (rawError) {
      processReportException('processData.ensureRaw', rawError, { formId: formulaire.id });
    }
  }

  var ensureParentTableFn = bigQueryServices.ensureParentTable;
  var parentTableResolver = bigQueryServices.parentTableId;
  var extractAliasFn = bigQueryServices.extractAliasPart;

  if (typeof ensureParentTableFn !== 'function' || typeof parentTableResolver !== 'function') {
    log('processData: ensureParentTable ou bqParentTableId indisponible, étape ignorée.');
    return config;
  }

  try {
    var parentTableId = parentTableResolver(formulaire);
    formulaire.tableName = parentTableId;
    formulaire.alias = extractAliasFn(parentTableId, formulaire.id);
    ensureParentTableFn(config, parentTableId);
    log('processData: table parent prête -> ' + parentTableId);
  } catch (parentError) {
    processReportException('processData.ensureParent', parentError, { formId: formulaire.id });
  }

  return config;
}
