/**
 * Fonctions utilitaires pour initialiser et alimenter BigQuery.
 * Les identifiants de projet et dataset sont lus dans les ScriptProperties:
 *   - BQ_PROJECT_ID
 *   - BQ_DATASET
 *   - BQ_LOCATION (optionnel, utilisé pour la journalisation)
 */
const BQ_RAW_TABLE_ID = 'kizeo_raw_events';
const BQ_CONFIG_KEYS = {
  project: 'BQ_PROJECT_ID',
  dataset: 'BQ_DATASET',
  location: 'BQ_LOCATION'
};

/**
 * Définit une fois pour toutes les ScriptProperties pour BQ.
 */
function initBigQueryConfig() {
  const props = PropertiesService.getScriptProperties();
  props.setProperties({
    BQ_PROJECT_ID: 'mon-fr-tpd-sarpi-datagrs-dev',
    BQ_DATASET: 'Kizeo',
    BQ_LOCATION: 'europe-west1' // optionnel
  }, true);
}

/**
 * Retourne la configuration BigQuery depuis les ScriptProperties.
 * @return {{projectId:string, datasetId:string, location:string|null}|null}
 */
function getBigQueryConfig() {
  const props = PropertiesService.getScriptProperties();
  const projectId = props.getProperty(BQ_CONFIG_KEYS.project);
  const datasetId = props.getProperty(BQ_CONFIG_KEYS.dataset);
  if (!projectId || !datasetId) {
    return null;
  }
  const location = props.getProperty(BQ_CONFIG_KEYS.location);
  return { projectId, datasetId, location: location || null };
}

/**
 * Ingestion brute des réponses Kizeo dans BigQuery.
 * @param {{id:string, nom:string}} formulaire
 * @param {Array<Object>} records
 */
function bqIngestRawKizeoBatch(formulaire, records) {
  if (!records || !records.length) return;
  const config = getBigQueryConfig();
  if (!config) {
    console.log('Configuration BigQuery absente, ingestion ignorée.');
    return;
  }

  try {
    bqEnsureRawTable(config);
    const rows = records
      .map((record) => bqBuildRawRow(formulaire, record))
      .filter((row) => row !== null);
    if (!rows.length) return;

    const requestBody = {
      kind: 'bigquery#tableDataInsertAllRequest',
      rows: rows.map((row) => ({ json: row.payload, insertId: row.insertId })),
      skipInvalidRows: false,
      ignoreUnknownValues: false
    };

    const response = BigQuery.Tabledata.insertAll(
      requestBody,
      config.projectId,
      config.datasetId,
      BQ_RAW_TABLE_ID
    );

    if (response.insertErrors && response.insertErrors.length) {
      throw new Error('BigQuery insertErrors: ' + JSON.stringify(response.insertErrors));
    }

    bqRecordAudit(config, formulaire, rows.length);
  } catch (e) {
    handleException('bqIngestRawKizeoBatch', e, {
      formId: formulaire?.id,
      dataset: config.datasetId,
      table: BQ_RAW_TABLE_ID
    });
  }
}

/**
 * Construit une ligne brute pour la table BigQuery.
 * @param {{id:string, nom:string}} formulaire
 * @param {Object} record
 * @return {{insertId:string, payload:Object}|null}
 */
function bqBuildRawRow(formulaire, record) {
  if (!record) return null;
  const dataId = record.id || record._id;
  if (!dataId) return null;
  const insertId = [formulaire?.id || 'unknown', dataId, record.update_time || record._update_time || 'na']
    .map((part) => (part ? String(part) : ''))
    .join('|');

  const answerTime = bqNormalizeTimestamp(record.answer_time || record._answer_time);
  const updateTime = bqNormalizeTimestamp(record.update_time || record._update_time);
  const ingestionTime = new Date().toISOString();

  const payload = {
    form_id: String(formulaire?.id || record.form_id || ''),
    form_name: formulaire?.nom || '',
    data_id: String(dataId),
    form_unique_id: record.form_unique_id || '',
    user_id: record.user_id || '',
    user_last_name: record.last_name || '',
    user_first_name: record.first_name || '',
    answer_time: answerTime,
    update_time: updateTime,
    ingestion_time: ingestionTime,
    payload: record
  };

  return { insertId, payload };
}

/**
 * Garantit l'existence de la table brute.
 * @param {{projectId:string, datasetId:string}} config
 */
function bqEnsureRawTable(config) {
  try {
    BigQuery.Tables.get(config.projectId, config.datasetId, BQ_RAW_TABLE_ID);
    return;
  } catch (err) {
    if (!bqIsNotFound(err)) throw err;
  }

  const tableResource = {
    tableReference: {
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId: BQ_RAW_TABLE_ID
    },
    schema: {
      fields: [
        { name: 'form_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'form_name', type: 'STRING' },
        { name: 'data_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'form_unique_id', type: 'STRING' },
        { name: 'user_id', type: 'STRING' },
        { name: 'user_last_name', type: 'STRING' },
        { name: 'user_first_name', type: 'STRING' },
        { name: 'answer_time', type: 'TIMESTAMP' },
        { name: 'update_time', type: 'TIMESTAMP' },
        { name: 'ingestion_time', type: 'TIMESTAMP' },
        { name: 'payload', type: 'JSON', mode: 'NULLABLE' }
      ]
    },
    timePartitioning: {
      type: 'DAY',
      field: 'ingestion_time'
    },
    clustering: {
      fields: ['form_id', 'data_id']
    }
  };

  BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
}

/**
 * Enregistre un point d'audit minimal dans les logs BigQuery (table `etl_audit`).
 * La création de la table est automatique si elle n'existe pas.
 * @param {{projectId:string, datasetId:string}} config
 * @param {{id:string, nom:string}} formulaire
 * @param {number} rowCount
 */
function bqRecordAudit(config, formulaire, rowCount) {
  const auditTableId = 'etl_audit';
  try {
    bqEnsureAuditTable(config, auditTableId);
    const now = new Date();
    const row = {
      run_id: `${now.getTime()}_${formulaire?.id || 'unknown'}`,
      form_id: formulaire?.id || '',
      form_name: formulaire?.nom || '',
      row_count: rowCount,
      status: 'SUCCESS',
      run_at: now.toISOString()
    };
    const requestBody = {
      kind: 'bigquery#tableDataInsertAllRequest',
      rows: [{ json: row, insertId: row.run_id }],
      skipInvalidRows: false,
      ignoreUnknownValues: false
    };
    const response = BigQuery.Tabledata.insertAll(
      requestBody,
      config.projectId,
      config.datasetId,
      auditTableId
    );
    if (response.insertErrors && response.insertErrors.length) {
      console.log('Audit insert errors: ' + JSON.stringify(response.insertErrors));
    }
  } catch (e) {
    handleException('bqRecordAudit', e, { table: auditTableId });
  }
}

/**
 * Crée la table d'audit si nécessaire.
 * @param {{projectId:string, datasetId:string}} config
 * @param {string} tableId
 */
function bqEnsureAuditTable(config, tableId) {
  try {
    BigQuery.Tables.get(config.projectId, config.datasetId, tableId);
    return;
  } catch (err) {
    if (!bqIsNotFound(err)) throw err;
  }

  const resource = {
    tableReference: {
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId
    },
    schema: {
      fields: [
        { name: 'run_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'form_id', type: 'STRING' },
        { name: 'form_name', type: 'STRING' },
        { name: 'row_count', type: 'INT64' },
        { name: 'status', type: 'STRING' },
        { name: 'run_at', type: 'TIMESTAMP' }
      ]
    },
    timePartitioning: {
      type: 'DAY',
      field: 'run_at'
    }
  };

  BigQuery.Tables.insert(resource, config.projectId, config.datasetId);
}

/**
 * Normalise un horodatage Kizeo vers un ISO8601 compatible BigQuery.
 * @param {string|null|undefined} value
 * @return {string|null}
 */
function bqNormalizeTimestamp(value) {
  if (!value) return null;
  try {
    const date = new Date(value);
    if (isNaN(date.getTime())) return null;
    return date.toISOString();
  } catch (e) {
    console.log('Impossible de convertir le timestamp: ' + value);
    return null;
  }
}

/**
 * Indique si une erreur BigQuery correspond à un 404 Not Found.
 * @param {*} err
 * @return {boolean}
 */
function bqIsNotFound(err) {
  if (!err) return false;
  const message = err.message || '';
  return message.indexOf('Not Found') !== -1 || message.indexOf('404') !== -1;
}
