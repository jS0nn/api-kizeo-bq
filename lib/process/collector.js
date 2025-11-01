function collectResponseArtifacts(records, context) {
  var fetchFn = context.fetchFn || requeteAPIDonnees;
  var formulaire = context.formulaire;
  var log = typeof context.log === 'function' ? context.log : console.log.bind(console);
  var spreadsheetBdD = context.spreadsheetBdD;
  var medias = context.medias || [];
  var snapshotService =
    context.snapshotService ||
    (typeof FormResponseSnapshot !== 'undefined' ? FormResponseSnapshot : null);
  if (!context || !context.bigQuery || typeof context.bigQuery.prepareParentRow !== 'function') {
    throw new Error('collectResponseArtifacts: services.bigQuery.prepareParentRow indisponible');
  }

  var prepareParentRow = context.bigQuery.prepareParentRow;

  var bigQueryContext = {
    rawRows: [],
    parentRows: [],
    parentColumns: {},
    subTables: {}
  };
  var processedDataIds = new Set();
  var latestRecord = null;
  var lastSnapshot = null;
  var prefix = processGetLogPrefix();

  records.forEach(function (rep) {
    var recordSummaryId = (rep && (rep._id || rep.id || rep.data_id)) || null;
    if (!recordSummaryId) {
      log(prefix + ': enregistrement sans identifiant, passage.');
      return;
    }

    var recordData = fetchDetailedRecord(fetchFn, formulaire, recordSummaryId, log);
    if (!recordData) {
      return;
    }

    bigQueryContext.rawRows.push(recordData);
    latestRecord = pickMostRecentRecord(recordData, latestRecord);

    if (prepareParentRow) {
      var parentPrepared = prepareParentRow(formulaire, recordData);
      if (parentPrepared) {
        bigQueryContext.parentRows.push(parentPrepared.row);
        if (Array.isArray(parentPrepared.columns)) {
          parentPrepared.columns.forEach(function (col) {
            if (col && col.name) {
              bigQueryContext.parentColumns[col.name] = col;
            }
          });
        }
        if (Array.isArray(parentPrepared.subforms)) {
          parentPrepared.subforms.forEach(function (subform) {
            if (!subform || !subform.tableId) return;
            var existing = bigQueryContext.subTables[subform.tableId] || { rows: [], columns: {} };
            if (Array.isArray(subform.rows) && subform.rows.length) {
              existing.rows = existing.rows.concat(subform.rows);
            }
            if (Array.isArray(subform.columns)) {
              subform.columns.forEach(function (col) {
                if (col && col.name) {
                  existing.columns[col.name] = col;
                }
              });
            }
            bigQueryContext.subTables[subform.tableId] = existing;
          });
        }
      }
    }

    if (snapshotService && typeof snapshotService.buildRowSnapshot === 'function') {
      var snapshot = snapshotService.buildRowSnapshot(spreadsheetBdD, formulaire, recordData, medias);
      if (snapshot && typeof snapshotService.persistSnapshot === 'function') {
        lastSnapshot = snapshotService.persistSnapshot(spreadsheetBdD, formulaire, snapshot) || snapshot;
      } else if (snapshot) {
        lastSnapshot = snapshot;
      }
    }

    var finalDataId = recordData.id || recordData._id || recordSummaryId;
    if (finalDataId !== undefined && finalDataId !== null && finalDataId !== '') {
      processedDataIds.add(String(finalDataId));
    } else {
      log(
        prefix +
          ": impossible de déterminer l'ID à marquer comme lu (form=" +
          formulaire.id +
          ', record=' +
          recordSummaryId +
          ')'
      );
    }
  });

  return { bigQueryContext: bigQueryContext, processedDataIds: processedDataIds, latestRecord: latestRecord, lastSnapshot: lastSnapshot };
}
