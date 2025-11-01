// BigQuery backfill utilities (ex-backfill regroupé)

function bqBackfillForm(formId, options) {
  var normalizedId = formId ? String(formId).trim() : '';
  if (!normalizedId) {
    throw new Error('bqBackfillForm: formId requis');
  }
  try {
    return runBigQueryBackfillForForm(normalizedId, options || {});
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('bqBackfillForm', error, { formId: normalizedId });
    }
    throw error;
  }
}

function runBigQueryBackfillForForm(formId, opts) {
  var options = opts || {};
  var services = createIngestionServices(options.services);
  var log = resolveLogFunction(services.logger);
  var fetchFn = typeof services.fetch === 'function' ? services.fetch : requeteAPIDonnees;
  var chunkSize = resolveBackfillChunkSize(options.chunkSize);
  var startBoundary = parseBackfillBoundary(options.startDate);
  var endBoundary = parseBackfillBoundary(options.endDate);

  if (startBoundary !== null && endBoundary !== null && startBoundary > endBoundary) {
    throw new Error('bqBackfillForm: startDate doit être antérieure ou égale à endDate');
  }

  log(
    'bqBackfillForm start -> formId=' +
      formId +
      ', start=' +
      (startBoundary !== null ? new Date(startBoundary).toISOString() : 'n/a') +
      ', end=' +
      (endBoundary !== null ? new Date(endBoundary).toISOString() : 'n/a') +
      ', chunk=' +
      chunkSize
  );

  var formulaire = resolveBackfillFormMetadata(formId, options, fetchFn, log);
  prepareBigQueryTargetsForBackfill(formulaire, services, log);

  var listResponse = fetchFn('GET', '/forms/' + formulaire.id + '/data/all');
  if (!listResponse || listResponse.responseCode !== 200) {
    throw new Error(
      'bqBackfillForm: impossible de récupérer /forms/' +
        formulaire.id +
        '/data/all (code=' +
        (listResponse && listResponse.responseCode ? listResponse.responseCode : 'n/a') +
        ')'
    );
  }

  var payload = listResponse.data || {};
  var rawDataArray = Array.isArray(payload.data) ? payload.data.slice() : [];
  if (!rawDataArray.length) {
    log('bqBackfillForm: aucune donnée renvoyée par data/all pour form=' + formulaire.id);
    return buildBackfillSummary(formulaire, startBoundary, endBoundary, {
      totalFetched: 0,
      totalChunks: 0,
      totalRawInserted: 0,
      totalParentInserted: 0,
      totalSubRowsInserted: 0,
      totalMediasInserted: 0
    });
  }

  var candidateSummaries = filterBackfillSummaries(rawDataArray, startBoundary, endBoundary, options.limit || null);
  if (!candidateSummaries.length) {
    log('bqBackfillForm: aucune donnée dans l’intervalle demandé (form=' + formulaire.id + ').');
    return buildBackfillSummary(formulaire, startBoundary, endBoundary, {
      totalFetched: rawDataArray.length,
      totalChunks: 0,
      totalRawInserted: 0,
      totalParentInserted: 0,
      totalSubRowsInserted: 0,
      totalMediasInserted: 0
    });
  }

  log(
    'bqBackfillForm: ' +
      candidateSummaries.length +
      ' enregistrements à traiter après filtrage (sur ' +
      rawDataArray.length +
      ').'
  );

  var totalRawInserted = 0;
  var totalParentInserted = 0;
  var totalSubRowsInserted = 0;
  var totalMediasInserted = 0;
  var totalChunks = 0;

  for (var index = 0; index < candidateSummaries.length; index += chunkSize) {
    var chunkSummaries = candidateSummaries.slice(index, index + chunkSize);
    var chunkResult = ingestBackfillChunk(formulaire, chunkSummaries, {
      services: services,
      fetchFn: fetchFn,
      log: log,
      startBoundary: startBoundary,
      endBoundary: endBoundary,
      includeMedia: options.includeMedia === true,
      spreadsheetId: options.spreadsheetId || null
    });
    totalChunks += 1;
    totalRawInserted += chunkResult.rawInserted;
    totalParentInserted += chunkResult.parentInserted;
    totalSubRowsInserted += chunkResult.subRowsInserted;
    totalMediasInserted += chunkResult.mediasInserted;
  }

  log(
    'bqBackfillForm: terminé -> raw=' +
      totalRawInserted +
      ', parent=' +
      totalParentInserted +
      ', subRows=' +
      totalSubRowsInserted +
      ', medias=' +
      totalMediasInserted +
      ', chunks=' +
      totalChunks
  );

  return buildBackfillSummary(formulaire, startBoundary, endBoundary, {
    totalFetched: candidateSummaries.length,
    totalChunks: totalChunks,
    totalRawInserted: totalRawInserted,
    totalParentInserted: totalParentInserted,
    totalSubRowsInserted: totalSubRowsInserted,
    totalMediasInserted: totalMediasInserted
  });
}

function ingestBackfillChunk(formulaire, summaries, context) {
  var fetchFn = context.fetchFn;
  var log = context.log;
  var services = context.services;
  var startBoundary = context.startBoundary;
  var endBoundary = context.endBoundary;
  var includeMedia = context.includeMedia === true;
  var mediasTarget = [];
  var spreadsheetForMedia = null;

  if (includeMedia) {
    try {
      var spreadsheetId = context.spreadsheetId;
      if (spreadsheetId) {
        spreadsheetForMedia = SpreadsheetApp.openById(spreadsheetId);
      } else {
        spreadsheetForMedia = SpreadsheetApp.getActiveSpreadsheet();
      }
    } catch (error) {
      if (typeof handleException === 'function') {
        handleException('ingestBackfillChunk.openSpreadsheet', error);
      }
      spreadsheetForMedia = null;
    }
  }

  var bigQueryContext = {
    rawRows: [],
    parentRows: [],
    parentColumns: {},
    subTables: {}
  };

  summaries.forEach(function (summary) {
    var recordId = (summary && (summary._id || summary.id || summary.data_id)) || null;
    if (!recordId) {
      log('bqBackfillForm: enregistrement sans identifiant, ignoré.');
      return;
    }

    var detail = fetchDetailedRecord(fetchFn, formulaire, recordId, log);
    if (!detail) {
      return;
    }

    if (!isRecordWithinBoundaries(detail, startBoundary, endBoundary)) {
      return;
    }

    bigQueryContext.rawRows.push(detail);
    var parentPrepared = bqPrepareParentRow(formulaire, detail);
    if (parentPrepared) {
      bigQueryContext.parentRows.push(parentPrepared.row);
      if (Array.isArray(parentPrepared.columns)) {
        parentPrepared.columns.forEach(function (col) {
          if (!col || !col.name) return;
          bigQueryContext.parentColumns[col.name] = col;
        });
      }
      if (Array.isArray(parentPrepared.subforms) && parentPrepared.subforms.length) {
        parentPrepared.subforms.forEach(function (subform) {
          if (!subform || !subform.tableId) return;
          var existing = bigQueryContext.subTables[subform.tableId] || { rows: [], columns: {} };
          if (Array.isArray(subform.rows) && subform.rows.length) {
            existing.rows = existing.rows.concat(subform.rows);
          }
          if (Array.isArray(subform.columns) && subform.columns.length) {
            subform.columns.forEach(function (col) {
              if (!col || !col.name) return;
              existing.columns[col.name] = col;
            });
          }
          bigQueryContext.subTables[subform.tableId] = existing;
        });
      }
    }

    if (includeMedia && spreadsheetForMedia) {
      collectMediasForRecord(formulaire, detail, mediasTarget, spreadsheetForMedia);
    }
  });

  ingestBigQueryPayloads(formulaire, bigQueryContext, mediasTarget, services, log);

  return {
    rawInserted: bigQueryContext.rawRows.length,
    parentInserted: bigQueryContext.parentRows.length,
    subRowsInserted: computeSubRowCount(bigQueryContext.subTables),
    mediasInserted: mediasTarget.length
  };
}

function computeSubRowCount(subTablesMap) {
  if (!subTablesMap) return 0;
  return Object.keys(subTablesMap).reduce(function (acc, tableId) {
    var definition = subTablesMap[tableId];
    if (!definition || !Array.isArray(definition.rows)) {
      return acc;
    }
    return acc + definition.rows.length;
  }, 0);
}

function collectMediasForRecord(formulaire, detail, mediasCollector, spreadsheet) {
  var snapshotService =
    typeof FormResponseSnapshot !== 'undefined' ? FormResponseSnapshot : typeof ExternalSnapshot !== 'undefined' ? ExternalSnapshot : null;
  if (!snapshotService || typeof snapshotService.buildRowSnapshot !== 'function') {
    return;
  }
  try {
    snapshotService.buildRowSnapshot(spreadsheet, formulaire, detail, mediasCollector);
  } catch (error) {
    if (typeof handleException === 'function') {
      handleException('collectMediasForRecord', error, {
        formId: formulaire.id,
        dataId: detail && (detail.id || detail._id) ? detail.id || detail._id : ''
      });
    }
  }
}

function resolveBackfillFormMetadata(formId, options, fetchFn, log) {
  var formulaireBase =
    options && options.formulaire && typeof options.formulaire === 'object' ? Object.assign({}, options.formulaire) : {};
  formulaireBase.id = formId;

  if (!formulaireBase.nom) {
    try {
      var formResponse = fetchFn('GET', '/forms/' + formId);
      var formPayload = formResponse && formResponse.data ? formResponse.data : null;
      var formData = formPayload && formPayload.form ? formPayload.form : formPayload;
      var extractedName =
        (formData && (formData.name || formData.libelle || formData.label)) || formulaireBase.nom || 'form_' + formId;
      formulaireBase.nom = extractedName;
      log('bqBackfillForm: nom formulaire résolu -> "' + formulaireBase.nom + '"');
    } catch (error) {
      if (typeof handleException === 'function') {
        handleException('resolveBackfillFormMetadata', error, { formId: formId });
      }
      if (!formulaireBase.nom) {
        formulaireBase.nom = 'form_' + formId;
      }
    }
  }

  var tableCandidate =
    options.tableName || formulaireBase.tableName || formulaireBase.alias || formulaireBase.nom || 'form_' + formId;
  var computedTableName = bqComputeTableName(formulaireBase.id, formulaireBase.nom || '', tableCandidate);
  formulaireBase.tableName = computedTableName;
  formulaireBase.alias = bqExtractAliasPart(computedTableName, formulaireBase.id);

  return formulaireBase;
}

function prepareBigQueryTargetsForBackfill(formulaire, services, log) {
  var bigQueryServices = services.bigQuery || {};
  var getConfigFn =
    typeof bigQueryServices.getConfig === 'function' ? bigQueryServices.getConfig : getBigQueryConfig;
  var ensureDatasetFn =
    typeof bigQueryServices.ensureDataset === 'function' ? bigQueryServices.ensureDataset : bqEnsureDataset;
  var ensureRawTableFn =
    typeof bigQueryServices.ensureRawTable === 'function' ? bigQueryServices.ensureRawTable : bqEnsureRawTable;
  var ensureParentTableFn =
    typeof bigQueryServices.ensureParentTable === 'function' ? bigQueryServices.ensureParentTable : bqEnsureParentTable;

  var config = getConfigFn();
  if (!config) {
    throw new Error('bqBackfillForm: configuration BigQuery indisponible');
  }

  ensureDatasetFn(config);
  ensureRawTableFn(config);
  ensureParentTableFn(config, bqParentTableId(formulaire));

  log(
    'bqBackfillForm: BigQuery prêt -> ' +
      config.projectId +
      '.' +
      config.datasetId +
      '.' +
      formulaire.tableName
  );
}

function filterBackfillSummaries(rawDataArray, startBoundary, endBoundary, limit) {
  var limitNumeric = Number(limit);
  var effectiveLimit = Number.isFinite(limitNumeric) && limitNumeric > 0 ? Math.floor(limitNumeric) : null;
  var filtered = rawDataArray
    .filter(function (item) {
      return isRecordWithinBoundaries(item, startBoundary, endBoundary);
    })
    .sort(function (a, b) {
      var aTs = extractRecordTimestamp(a) || 0;
      var bTs = extractRecordTimestamp(b) || 0;
      return aTs - bTs;
    });
  if (effectiveLimit !== null && filtered.length > effectiveLimit) {
    return filtered.slice(0, effectiveLimit);
  }
  return filtered;
}

function isRecordWithinBoundaries(record, startBoundary, endBoundary) {
  var timestamp = extractRecordTimestamp(record);
  if (timestamp === null) return true;
  if (startBoundary !== null && timestamp < startBoundary) return false;
  if (endBoundary !== null && timestamp > endBoundary) return false;
  return true;
}

function extractRecordTimestamp(record) {
  if (!record) return null;
  var candidate =
    record.update_time ||
    record._update_time ||
    record.answer_time ||
    record._answer_time ||
    record.timestamp ||
    record.created_at;
  if (!candidate) return null;
  var parsed = Date.parse(candidate);
  return Number.isFinite(parsed) ? parsed : null;
}

function resolveBackfillChunkSize(raw) {
  var numeric = Number(raw);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 25;
  }
  return Math.min(250, Math.max(1, Math.floor(numeric)));
}

function parseBackfillBoundary(value) {
  if (value === null || value === undefined || value === '') return null;
  if (value instanceof Date) {
    var time = value.getTime();
    return Number.isFinite(time) ? time : null;
  }
  var parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildBackfillSummary(formulaire, startBoundary, endBoundary, stats) {
  return {
    formId: formulaire.id,
    formName: formulaire.nom,
    targetTable: formulaire.tableName,
    startDate: startBoundary !== null ? new Date(startBoundary).toISOString() : null,
    endDate: endBoundary !== null ? new Date(endBoundary).toISOString() : null,
    totalFetched: stats.totalFetched,
    chunkCount: stats.totalChunks,
    inserted: {
      raw: stats.totalRawInserted,
      parent: stats.totalParentInserted,
      subRows: stats.totalSubRowsInserted,
      medias: stats.totalMediasInserted
    }
  };
}

