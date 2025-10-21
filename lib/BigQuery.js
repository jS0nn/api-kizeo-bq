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

const BQ_DEFAULT_CONFIG = {
  projectId: 'fr-tpd-sarpi-datagrs-dev',
  datasetId: 'Kizeo',
  location: 'europe-west1'
};

const BQ_TIME_PARTITION_TYPE = 'DAY';

function bqBuildTimePartitioning(fieldName) {
  if (!fieldName) return null;
  return {
    type: BQ_TIME_PARTITION_TYPE,
    field: fieldName
  };
}

function getConfigStore() {
  try {
    return PropertiesService.getDocumentProperties();
  } catch (e) {
    console.log('DocumentProperties indisponibles, utilisation des ScriptProperties de la librairie.');
    return PropertiesService.getScriptProperties();
  }
}

function bqEnsureDataset(config) {
  try {
    console.log(`bqEnsureDataset: vérification dataset ${config.projectId}.${config.datasetId}`);
    BigQuery.Datasets.get(config.projectId, config.datasetId);
    console.log(`Dataset déjà existant: ${config.projectId}.${config.datasetId}`);
    return;
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureDataset: échec inattendu -> ${err}`);
      throw err;
    }
  }

  const datasetResource = {
    datasetReference: {
      projectId: config.projectId,
      datasetId: config.datasetId
    },
    location: config.location || BQ_DEFAULT_CONFIG.location
  };

  BigQuery.Datasets.insert(datasetResource, config.projectId);
  console.log(`Dataset créé: ${config.projectId}.${config.datasetId}`);
}

const BQ_PARENT_BASE_COLUMNS = [
  { name: 'form_id', type: 'STRING', mode: 'REQUIRED' },
  { name: 'form_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'data_id', type: 'STRING', mode: 'REQUIRED' },
  { name: 'form_unique_id', type: 'STRING', mode: 'NULLABLE' },
  { name: 'user_id', type: 'STRING', mode: 'NULLABLE' },
  { name: 'user_last_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'user_first_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'answer_time', type: 'TIMESTAMP', mode: 'NULLABLE' },
  { name: 'update_time', type: 'TIMESTAMP', mode: 'NULLABLE' },
  { name: 'origin_answer', type: 'STRING', mode: 'NULLABLE' },
  { name: 'ingestion_time', type: 'TIMESTAMP', mode: 'NULLABLE' }
];

const BQ_SUBTABLE_BASE_COLUMNS = [
  { name: 'form_id', type: 'STRING', mode: 'REQUIRED' },
  { name: 'form_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'parent_data_id', type: 'STRING', mode: 'REQUIRED' },
  { name: 'parent_form_unique_id', type: 'STRING', mode: 'NULLABLE' },
  { name: 'parent_answer_time', type: 'TIMESTAMP', mode: 'NULLABLE' },
  { name: 'parent_update_time', type: 'TIMESTAMP', mode: 'NULLABLE' },
  { name: 'sub_row_index', type: 'INT64', mode: 'NULLABLE' },
  { name: 'ingestion_time', type: 'TIMESTAMP', mode: 'NULLABLE' }
];

const BQ_MEDIA_BASE_COLUMNS = [
  { name: 'form_id', type: 'STRING', mode: 'REQUIRED' },
  { name: 'form_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'data_id', type: 'STRING', mode: 'REQUIRED' },
  { name: 'form_unique_id', type: 'STRING', mode: 'NULLABLE' },
  { name: 'field_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'media_id', type: 'STRING', mode: 'NULLABLE' },
  { name: 'media_type', type: 'STRING', mode: 'NULLABLE' },
  { name: 'file_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'drive_file_id', type: 'STRING', mode: 'NULLABLE' },
  { name: 'drive_url', type: 'STRING', mode: 'NULLABLE' },
  { name: 'drive_view_url', type: 'STRING', mode: 'NULLABLE' },
  { name: 'drive_public_url', type: 'STRING', mode: 'NULLABLE' },
  { name: 'folder_id', type: 'STRING', mode: 'NULLABLE' },
  { name: 'folder_url', type: 'STRING', mode: 'NULLABLE' },
  { name: 'parent_answer_time', type: 'TIMESTAMP', mode: 'NULLABLE' },
  { name: 'parent_update_time', type: 'TIMESTAMP', mode: 'NULLABLE' },
  { name: 'ingestion_time', type: 'TIMESTAMP', mode: 'NULLABLE' }
];

const BQ_IDENTIFIER_REGEX = /[^a-zA-Z0-9_]/g;
const BQ_TABLE_PART_SEPARATOR = '__';

/**
 * Définit les ScriptProperties BigQuery avec la configuration par défaut.
 */
function initBigQueryConfig() {
  const props = getConfigStore();
  props.setProperties({
    BQ_PROJECT_ID: BQ_DEFAULT_CONFIG.projectId,
    BQ_DATASET: BQ_DEFAULT_CONFIG.datasetId,
    BQ_LOCATION: BQ_DEFAULT_CONFIG.location || ''
  }, true);

  console.log(
    `initBigQueryConfig -> project=${BQ_DEFAULT_CONFIG.projectId}, dataset=${BQ_DEFAULT_CONFIG.datasetId}, location=${BQ_DEFAULT_CONFIG.location}`
  );

  const config = Object.assign({}, BQ_DEFAULT_CONFIG);
  try {
    bqEnsureDataset(config);
    bqEnsureRawTable(config);
    bqEnsureAuditTable(config, 'etl_audit');
  } catch (e) {
    handleException('initBigQueryConfig.ensureCore', e, {
      project: config.projectId,
      dataset: config.datasetId
    });
  }

  return config;
}

/**
 * Retourne la configuration BigQuery depuis les ScriptProperties.
 * @return {{projectId:string, datasetId:string, location:string|null}|null}
 */
function getBigQueryConfig() {
  const props = getConfigStore();
  const projectId = props.getProperty(BQ_CONFIG_KEYS.project) || BQ_DEFAULT_CONFIG.projectId;
  const datasetId = props.getProperty(BQ_CONFIG_KEYS.dataset) || BQ_DEFAULT_CONFIG.datasetId;
  const location = props.getProperty(BQ_CONFIG_KEYS.location) || BQ_DEFAULT_CONFIG.location;
  if (!projectId || !datasetId) {
    console.log('BigQuery config manquante (projectId/datasetId).');
    return null;
  }
  console.log(`BQ Config -> project=${projectId}, dataset=${datasetId}, location=${location || 'default'}`);
  return { projectId, datasetId, location: location || null };
}

function ensureBigQueryCoreTables() {
  const config = getBigQueryConfig();
  if (!config) {
    console.log('ensureBigQueryCoreTables: configuration BigQuery indisponible.');
    return null;
  }
  try {
    bqEnsureDataset(config);
    bqEnsureRawTable(config);
    bqEnsureAuditTable(config, 'etl_audit');
    return config;
  } catch (e) {
    handleException('ensureBigQueryCoreTables', e, {
      project: config.projectId,
      dataset: config.datasetId
    });
    throw e;
  }
}

function bqSlugifyIdentifier(source) {
  if (!source) return '';
  const normalized = source
    .toString()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(BQ_IDENTIFIER_REGEX, '_');
  const trimmed = normalized.replace(/^_+/, '').replace(/_+$/g, '');
  return trimmed ? trimmed.toLowerCase() : 'field';
}

function bqSanitizeTablePart(source, fallback) {
  if (source === null || source === undefined) return fallback;
  const normalizedSource = String(source).trim();
  if (!normalizedSource) return fallback;
  const slug = bqSlugifyIdentifier(normalizedSource);
  if (slug === 'field' && normalizedSource.toLowerCase() !== 'field') {
    return fallback;
  }
  return slug || fallback;
}

function bqJoinTableParts(parts) {
  return parts.filter((part) => part && part.length).join(BQ_TABLE_PART_SEPARATOR);
}

function bqNormalizeTableIdentifier(tableId) {
  if (tableId === null || tableId === undefined) return '';
  return tableId
    .toString()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(BQ_IDENTIFIER_REGEX, '_')
    .replace(/^_+/, '')
    .replace(/_+$/g, '')
    .toLowerCase();
}

function bqComputeTableName(formId, formName, rawCandidate) {
  const idPart = bqSanitizeTablePart(formId, 'form');
  const fallbackNamePart = bqSanitizeTablePart(formName, 'sheet');

  const trimmedCandidate = rawCandidate !== undefined && rawCandidate !== null
    ? String(rawCandidate).trim()
    : '';

  if (!trimmedCandidate) {
    return bqJoinTableParts([idPart, fallbackNamePart]);
  }

  const normalizedInput = bqNormalizeTableIdentifier(trimmedCandidate);
  if (!normalizedInput) {
    return bqJoinTableParts([idPart, fallbackNamePart]);
  }

  let aliasCandidate = normalizedInput;
  if (normalizedInput.includes(BQ_TABLE_PART_SEPARATOR)) {
    const segments = normalizedInput.split(BQ_TABLE_PART_SEPARATOR).filter(Boolean);
    if (!segments.length) {
      aliasCandidate = fallbackNamePart;
    } else if (segments[0] === idPart && segments.length > 1) {
      aliasCandidate = segments.slice(1).join(BQ_TABLE_PART_SEPARATOR);
    } else {
      aliasCandidate = segments[segments.length - 1];
    }
  }

  const aliasPart = bqSanitizeTablePart(aliasCandidate, fallbackNamePart);
  return bqJoinTableParts([idPart, aliasPart]);
}

function bqExtractAliasPart(tableName, formId) {
  const normalizedTable = bqComputeTableName(formId, '', tableName);
  if (!normalizedTable) return '';
  const idPart = bqSanitizeTablePart(formId, 'form');
  const segments = normalizedTable.split(BQ_TABLE_PART_SEPARATOR);
  if (segments.length <= 1) {
    return normalizedTable;
  }
  if (segments[0] === idPart) {
    return segments.slice(1).join(BQ_TABLE_PART_SEPARATOR);
  }
  return segments[segments.length - 1];
}

function bqEnsureUniqueName(baseName, usedNames) {
  let candidate = baseName;
  let index = 1;
  while (usedNames.has(candidate)) {
    candidate = `${baseName}_${index}`;
    index++;
  }
  usedNames.add(candidate);
  return candidate;
}

function bqParentTableId(formulaire) {
  if (!formulaire) {
    return bqJoinTableParts(['form', 'sheet']);
  }
  const candidate = formulaire.tableName || formulaire.alias || formulaire.nom;
  return bqComputeTableName(formulaire.id, formulaire.nom || '', candidate);
}

function bqSubTableId(formulaire, subformName) {
  const parentId = bqParentTableId(formulaire);
  const subPart = bqSanitizeTablePart(subformName, 'subform');
  return bqJoinTableParts([parentId, subPart]);
}

function bqMediaTableId(formulaire) {
  const parentId = bqParentTableId(formulaire);
  return bqJoinTableParts([parentId, 'media']);
}

function bqEnsureParentTable(config, tableId) {
  const tableRef = `${config.projectId}.${config.datasetId}.${tableId}`;
  console.log(`bqEnsureParentTable: vérification table ${tableRef}`);
  try {
    BigQuery.Tables.get(config.projectId, config.datasetId, tableId);
    console.log(`Table parent déjà existante: ${tableRef}`);
    return;
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureParentTable: erreur inattendue lors du get -> ${err}`);
      throw err;
    }
  }

  const tableResource = {
    tableReference: {
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId
    },
    schema: {
      fields: BQ_PARENT_BASE_COLUMNS
    },
    timePartitioning: bqBuildTimePartitioning('ingestion_time'),
    clustering: {
      fields: ['form_id', 'data_id']
    }
  };

  try {
    BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
    console.log(`Table parent créée: ${tableRef}`);
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureParentTable: erreur inattendue lors de l'insertion -> ${err}`);
      throw err;
    }
    console.log(`bqEnsureParentTable: dataset introuvable lors de la création (${tableRef}), tentative de création du dataset.`);
    bqEnsureDataset(config);
    BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
    console.log(`Table parent créée après création dataset: ${tableRef}`);
  }
}

function bqEnsureSubTable(config, tableId) {
  const tableRef = `${config.projectId}.${config.datasetId}.${tableId}`;
  console.log(`bqEnsureSubTable: vérification table ${tableRef}`);
  try {
    BigQuery.Tables.get(config.projectId, config.datasetId, tableId);
    console.log(`Table fille déjà existante: ${tableRef}`);
    return;
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureSubTable: erreur inattendue lors du get -> ${err}`);
      throw err;
    }
  }

  const tableResource = {
    tableReference: {
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId
    },
    schema: {
      fields: BQ_SUBTABLE_BASE_COLUMNS
    },
    timePartitioning: bqBuildTimePartitioning('ingestion_time'),
    clustering: {
      fields: ['form_id', 'parent_data_id']
    }
  };

  try {
    BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
    console.log(`Table fille créée: ${tableRef}`);
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureSubTable: erreur inattendue lors de l'insertion -> ${err}`);
      throw err;
    }
    console.log(`bqEnsureSubTable: dataset introuvable lors de la création (${tableRef}), tentative de création du dataset.`);
    bqEnsureDataset(config);
    BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
    console.log(`Table fille créée après création dataset: ${tableRef}`);
  }
}

function bqEnsureMediaTable(config, tableId) {
  const tableRef = `${config.projectId}.${config.datasetId}.${tableId}`;
  console.log(`bqEnsureMediaTable: vérification table ${tableRef}`);
  try {
    const table = BigQuery.Tables.get(config.projectId, config.datasetId, tableId);
    const existingFields = table && table.schema && Array.isArray(table.schema.fields) ? table.schema.fields : [];
    const existingNames = new Set(existingFields.map((field) => field.name));
    const missingColumns = BQ_MEDIA_BASE_COLUMNS.filter((column) => !existingNames.has(column.name));
    if (missingColumns.length) {
      const mergedFields = existingFields.concat(missingColumns);
      BigQuery.Tables.patch(
        {
          schema: {
            fields: mergedFields
          }
        },
        config.projectId,
        config.datasetId,
        tableId
      );
      console.log(
        `Table média mise à jour (${tableRef}) -> colonnes ajoutées: ${missingColumns
          .map((column) => column.name)
          .join(', ')}`
      );
    } else {
      console.log(`Table média déjà existante: ${tableRef}`);
    }
    return;
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureMediaTable: erreur inattendue lors du get -> ${err}`);
      throw err;
    }
  }

  const tableResource = {
    tableReference: {
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId
    },
    schema: {
      fields: BQ_MEDIA_BASE_COLUMNS
    },
    timePartitioning: bqBuildTimePartitioning('ingestion_time'),
    clustering: {
      fields: ['form_id', 'data_id']
    }
  };

  try {
    BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
    console.log(`Table média créée: ${tableRef}`);
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureMediaTable: erreur inattendue lors de l'insertion -> ${err}`);
      throw err;
    }
    console.log(`bqEnsureMediaTable: dataset introuvable lors de la création (${tableRef}), tentative de création du dataset.`);
    bqEnsureDataset(config);
    BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
    console.log(`Table média créée après création dataset: ${tableRef}`);
  }
}

function bqTableExists(config, tableId) {
  if (!config || !tableId) return false;
  try {
    BigQuery.Tables.get(config.projectId, config.datasetId, tableId);
    return true;
  } catch (err) {
    if (bqIsNotFound(err)) {
      return false;
    }
    console.log(`bqTableExists: erreur inattendue lors du get ${config.datasetId}.${tableId} -> ${err}`);
    throw err;
  }
}

function bqListFormSubTables(config, formulaire, parentTableId) {
  if (!config || !parentTableId) return [];
  const prefix = `${parentTableId}${BQ_TABLE_PART_SEPARATOR}`;
  const subTables = [];
  const seen = new Set();
  const mediaTableId = formulaire ? bqMediaTableId(formulaire) : '';
  let pageToken = null;

  try {
    do {
      const options = pageToken ? { pageToken } : {};
      const response = BigQuery.Tables.list(config.projectId, config.datasetId, options);
      const tables = Array.isArray(response.tables) ? response.tables : [];

      tables.forEach((table) => {
        const tableId = table && table.tableReference ? table.tableReference.tableId : '';
        if (!tableId) return;
        if (tableId === parentTableId) return;
        if (!tableId.startsWith(prefix)) return;
        if (mediaTableId && tableId === mediaTableId) return;
        if (seen.has(tableId)) return;
        seen.add(tableId);
        subTables.push(tableId);
      });

      pageToken = response.nextPageToken || null;
    } while (pageToken);
  } catch (err) {
    console.log(`bqListFormSubTables: échec listage tables (${config.datasetId}) -> ${err}`);
    return subTables;
  }

  return subTables;
}

function bqRunDeduplicationForForm(formulaire, options) {
  const config = getBigQueryConfig();
  if (!config) {
    console.log('bqRunDeduplicationForForm: configuration BigQuery indisponible, déduplication annulée.');
    return null;
  }

  const waitOptions = options && options.waitOptions ? options.waitOptions : null;
  const parentTableId = bqParentTableId(formulaire);
  const summary = {
    parent: { tableId: parentTableId, deleted: 0, skipped: false, reason: null, message: null },
    subTables: []
  };

  console.log(`bqRunDeduplicationForForm: lancement pour ${config.datasetId}.${parentTableId}`);

  if (bqTableExists(config, parentTableId)) {
    const parentResult = bqPurgeDuplicateParentRows(config, formulaire, parentTableId, waitOptions);
    summary.parent = Object.assign({ tableId: parentTableId }, parentResult);
  } else {
    summary.parent.skipped = true;
    summary.parent.reason = 'TABLE_NOT_FOUND';
    summary.parent.message = 'Table BigQuery introuvable';
    console.log(`bqRunDeduplicationForForm: table parent absente, purge ignorée (${config.datasetId}.${parentTableId}).`);
  }

  const subTableIds = bqListFormSubTables(config, formulaire, parentTableId);
  subTableIds.forEach((tableId) => {
    const subResult = bqPurgeDuplicateSubTableRows(config, formulaire, tableId, waitOptions);
    summary.subTables.push(Object.assign({ tableId }, subResult));
  });

  console.log(
    `bqRunDeduplicationForForm: terminé -> parentDeleted=${summary.parent.deleted}, subTables=${summary.subTables.length}`
  );
  return summary;
}

function bqEnsureColumns(config, tableId, columns, baseColumns) {
  if (!columns || !columns.length) return;
  const originColumns = Array.isArray(baseColumns) && baseColumns.length
    ? baseColumns
    : BQ_PARENT_BASE_COLUMNS;
  const existing = new Set(originColumns.map((c) => c.name));
  const newColumns = [];

  columns.forEach((col) => {
    if (!col || !col.name) return;
    if (existing.has(col.name)) return;
    existing.add(col.name);
    const definition = bqBuildColumnDefinition(col);
    newColumns.push(`ADD COLUMN IF NOT EXISTS ${definition}`);
  });

  if (!newColumns.length) return;

  const ddl = `ALTER TABLE \`${config.projectId}.${config.datasetId}.${tableId}\` ${newColumns.join(', ')}`;
  console.log(ddl);
  BigQuery.Jobs.query({ query: ddl, useLegacySql: false }, config.projectId);
}

function bqSafeInsertAll(requestBody, config, tableId, ensureFn) {
  const tableRef = `${config.projectId}.${config.datasetId}.${tableId}`;
  const maxAttempts = 3;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return BigQuery.Tabledata.insertAll(requestBody, config.projectId, config.datasetId, tableId);
    } catch (err) {
      lastError = err;
      if (!bqIsNotFound(err)) {
        throw err;
      }

      if (attempt === maxAttempts) {
        break;
      }

      console.log(
        `bqSafeInsertAll: ${tableRef} introuvable (${err.message}). Tentative de recréation (${attempt}/${maxAttempts - 1}).`
      );

      try {
        bqEnsureDataset(config);
      } catch (datasetErr) {
        console.log(`bqSafeInsertAll: échec ensure dataset -> ${datasetErr}`);
        throw err;
      }

      if (typeof ensureFn === 'function') {
        try {
          ensureFn();
        } catch (ensureError) {
          console.log(`bqSafeInsertAll: échec ensureFn -> ${ensureError}`);
          throw err;
        }
      }

      if (typeof Utilities === 'object' && Utilities.sleep) {
        Utilities.sleep(Math.min(500 * attempt, 2000));
      }
    }
  }

  console.log(`bqSafeInsertAll: échec insert ${tableRef} après ${maxAttempts} tentatives -> ${lastError}`);
  throw lastError;
}

function bqBuildColumnDefinition(column) {
  const name = column.name;
  const type = column.type || 'STRING';
  if (column.mode === 'REPEATED') {
    return `${name} ARRAY<${type}>`;
  }
  return `${name} ${type}`;
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

  const startTime = Date.now();
  let rows = [];

  try {
    bqEnsureRawTable(config);
    console.log(`Ingestion raw vers ${BQ_RAW_TABLE_ID} - lignes=${records.length}`);
    rows = records
      .map((record) => bqBuildRawRow(formulaire, record))
      .filter((row) => row !== null);
    if (!rows.length) return;

    const requestBody = {
      kind: 'bigquery#tableDataInsertAllRequest',
      rows: rows.map((row) => ({ json: row.payload, insertId: row.insertId })),
      skipInvalidRows: false,
      ignoreUnknownValues: false
    };

    const response = bqSafeInsertAll(
      requestBody,
      config,
      BQ_RAW_TABLE_ID,
      () => bqEnsureRawTable(config)
    );

    if (response.insertErrors && response.insertErrors.length) {
      throw new Error('BigQuery insertErrors: ' + JSON.stringify(response.insertErrors));
    }

    bqRecordAudit(config, formulaire, {
      targetTable: BQ_RAW_TABLE_ID,
      rowCount: rows.length,
      status: 'SUCCESS',
      durationMs: Date.now() - startTime,
      action: 'raw_insert'
    });
    if (rows.length) {
      console.log(`Exemple raw row: ${JSON.stringify(rows[0].payload).substring(0, 500)}...`);
    }
    console.log(`Ingestion raw réussie -> ${BQ_RAW_TABLE_ID}`);
  } catch (e) {
    try {
      bqRecordAudit(config, formulaire, {
        targetTable: BQ_RAW_TABLE_ID,
        rowCount: rows.length,
        status: 'FAILURE',
        durationMs: Date.now() - startTime,
        action: 'raw_insert',
        errorMessage: e && e.message ? e.message : String(e)
      });
    } catch (auditError) {
      console.log('bqIngestRawKizeoBatch: audit failure -> ' + auditError);
    }
    handleException('bqIngestRawKizeoBatch', e, {
      formId: formulaire?.id,
      dataset: config.datasetId,
      table: BQ_RAW_TABLE_ID
    });
  }
}

function bqPrepareParentRow(formulaire, data) {
  if (!data) return null;
  const usedNames = new Set(BQ_PARENT_BASE_COLUMNS.map((c) => c.name));
  const dynamicColumns = [];
  const subforms = [];
  const row = {
    form_id: String(data.form_id || formulaire?.id || ''),
    form_name: formulaire?.nom || '',
    data_id: String(data.id || data._id || ''),
    form_unique_id: data.form_unique_id || '',
    user_id: data.user_id || '',
    user_last_name: data.last_name || '',
    user_first_name: data.first_name || '',
    answer_time: bqNormalizeTimestamp(data.answer_time || data._answer_time),
    update_time: bqNormalizeTimestamp(data.update_time || data._update_time),
    origin_answer: data.origin_answer || '',
    ingestion_time: new Date().toISOString()
  };

  const parentContext = {
    form_id: row.form_id,
    form_name: row.form_name,
    data_id: row.data_id,
    form_unique_id: row.form_unique_id,
    answer_time: row.answer_time,
    update_time: row.update_time,
    ingestion_time: row.ingestion_time
  };

  const fields = data.fields || {};
  for (const rawName in fields) {
    if (!Object.prototype.hasOwnProperty.call(fields, rawName)) continue;
    const field = fields[rawName];
    if (!field) continue;

    if (isSubformField(field.type, field.value)) {
      const subformRows = normalizeSubformRows(field.value);
      const baseName = bqSlugifyIdentifier(rawName || 'subform');
      const tableColumnName = bqEnsureUniqueName(`table_${baseName}`, usedNames);
      const countColumnName = bqEnsureUniqueName(`table_${baseName}_row_count`, usedNames);
      const subTableId = bqSubTableId(formulaire, rawName);

      row[tableColumnName] = subformRows.length ? subTableId : null;
      row[countColumnName] = subformRows.length ? subformRows.length : 0;

      dynamicColumns.push({ name: tableColumnName, type: 'STRING', mode: 'NULLABLE' });
      dynamicColumns.push({ name: countColumnName, type: 'INT64', mode: 'NULLABLE' });

      if (subformRows.length) {
        const preparedSubform = bqPrepareSubformRows(formulaire, parentContext, rawName, field.value, subformRows);
        if (preparedSubform) {
          subforms.push(preparedSubform);
        }
      }
      continue;
    }

    const baseName = bqSlugifyIdentifier(rawName || 'field');
    const columnName = bqEnsureUniqueName(baseName, usedNames);
    const conversion = bqConvertFieldValue(field);

    row[columnName] = conversion.value;
    dynamicColumns.push({ name: columnName, type: conversion.type, mode: conversion.mode });
  }

  return { row, columns: dynamicColumns, subforms };
}

function bqConvertFieldValue(field) {
  const result = { type: 'STRING', mode: 'NULLABLE', value: null };
  if (!field) return result;

  const rawValue = field.value;
  const fieldType = (field.type || '').toLowerCase();

  if (rawValue === '' || rawValue === null || rawValue === undefined) {
    return result;
  }

  const arrayValue = Array.isArray(rawValue);

  switch (fieldType) {
    case 'number':
    case 'numeric':
    case 'float':
    case 'decimal':
      result.type = 'FLOAT64';
      result.value = arrayValue ? rawValue.map((v) => bqToNumber(v)) : bqToNumber(rawValue);
      if (arrayValue) result.mode = 'REPEATED';
      break;
    case 'integer':
      result.type = 'INT64';
      result.value = arrayValue ? rawValue.map((v) => bqToInteger(v)) : bqToInteger(rawValue);
      if (arrayValue) result.mode = 'REPEATED';
      break;
    case 'boolean':
    case 'yesno':
    case 'checkbox':
      result.type = 'BOOL';
      if (arrayValue) {
        result.mode = 'REPEATED';
        result.value = rawValue.map((v) => bqToBoolean(v));
      } else {
        result.value = bqToBoolean(rawValue);
      }
      break;
    case 'date':
      result.type = 'DATE';
      result.value = arrayValue
        ? rawValue.map((v) => bqNormalizeDate(v))
        : bqNormalizeDate(rawValue);
      if (arrayValue) result.mode = 'REPEATED';
      break;
    case 'time':
      result.type = 'TIME';
      result.value = arrayValue
        ? rawValue.map((v) => bqNormalizeTime(v))
        : bqNormalizeTime(rawValue);
      if (arrayValue) result.mode = 'REPEATED';
      break;
    case 'datetime':
    case 'timestamp':
      result.type = 'TIMESTAMP';
      result.value = arrayValue
        ? rawValue.map((v) => bqNormalizeTimestamp(v))
        : bqNormalizeTimestamp(rawValue);
      if (arrayValue) result.mode = 'REPEATED';
      break;
    default:
      if (arrayValue) {
        result.type = 'STRING';
        result.mode = 'REPEATED';
        result.value = rawValue.map((v) => (v === null || v === undefined ? null : String(v)));
      } else if (typeof rawValue === 'object') {
        result.type = 'JSON';
        result.value = rawValue;
      } else {
        result.type = 'STRING';
        result.value = String(rawValue);
      }
  }

  return result;
}

function bqPrepareSubformRows(formulaire, parentContext, fieldName, rawValue, normalizedRows) {
  const rows = Array.isArray(normalizedRows) ? normalizedRows : normalizeSubformRows(rawValue);
  if (!rows || !rows.length) return null;

  const tableId = bqSubTableId(formulaire, fieldName);
  const usedNames = new Set(BQ_SUBTABLE_BASE_COLUMNS.map((c) => c.name));
  const columnMap = {};
  const columnNameByKey = {};

  const ensureColumnName = (rawKey) => {
    const key = rawKey || 'field';
    if (columnNameByKey[key]) {
      return columnNameByKey[key];
    }
    const baseName = bqSlugifyIdentifier(key);
    const columnName = bqEnsureUniqueName(baseName, usedNames);
    columnNameByKey[key] = columnName;
    return columnName;
  };

  const preparedRows = rows.map((rowValues, index) => {
    const payload = {
      form_id: parentContext.form_id,
      form_name: parentContext.form_name,
      parent_data_id: parentContext.data_id,
      parent_form_unique_id: parentContext.form_unique_id,
      parent_answer_time: parentContext.answer_time,
      parent_update_time: parentContext.update_time,
      sub_row_index: index,
      ingestion_time: parentContext.ingestion_time
    };

    if (rowValues && typeof rowValues === 'object') {
      Object.keys(rowValues).forEach((rawKey) => {
        const columnName = ensureColumnName(rawKey);
        payload[columnName] = bqSerializeSubformValue(rowValues[rawKey]);
        columnMap[columnName] = { name: columnName, type: 'STRING', mode: 'NULLABLE' };
      });
    }

    return payload;
  });

  const columnDefs = Object.keys(columnMap).map((name) => columnMap[name]);

  return {
    tableId,
    rows: preparedRows,
    columns: columnDefs
  };
}

function bqSerializeSubformValue(value) {
  if (value === null || value === undefined) return null;
  if (Array.isArray(value)) {
    const serialized = value
      .map((item) => (item === null || item === undefined ? '' : String(item)))
      .filter((item) => item !== '');
    return serialized.length ? serialized.join(', ') : null;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (e) {
      return String(value);
    }
  }
  return String(value);
}

function bqToNumber(value) {
  const n = parseFloat(value);
  return isNaN(n) ? null : n;
}

function bqToInteger(value) {
  const n = parseInt(value, 10);
  return isNaN(n) ? null : n;
}

function bqToBoolean(value) {
  if (typeof value === 'boolean') return value;
  const normalized = String(value).toLowerCase();
  if (['true', 'yes', '1', 'on'].includes(normalized)) return true;
  if (['false', 'no', '0', 'off'].includes(normalized)) return false;
  return null;
}

function bqNormalizeDate(value) {
  if (!value) return null;
  try {
    const date = new Date(value);
    if (isNaN(date.getTime())) return null;
    return Utilities.formatDate(date, 'UTC', 'yyyy-MM-dd');
  } catch (e) {
    return null;
  }
}

function bqNormalizeTime(value) {
  if (!value) return null;
  const match = String(value).match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;
  const hours = match[1].padStart(2, '0');
  const minutes = match[2];
  const seconds = (match[3] || '00').padStart(2, '0');
  return `${hours}:${minutes}:${seconds}`;
}

function bqIngestParentBatch(formulaire, rows, columnDefs) {
  if (!rows || !rows.length) return;
  const config = getBigQueryConfig();
  if (!config) {
    console.log('Configuration BigQuery absente, ingestion parent ignorée.');
    return;
  }

  const tableId = bqParentTableId(formulaire);
  console.log(`Ingestion parent vers ${tableId} - lignes=${rows.length}`);

  const startTime = Date.now();

  try {
    bqEnsureParentTable(config, tableId);
    bqEnsureColumns(config, tableId, columnDefs);

    const requestBody = {
      kind: 'bigquery#tableDataInsertAllRequest',
      rows: rows.map((payload) => ({
        json: payload,
        insertId: `${payload.form_id || formulaire?.id || ''}|${payload.data_id || ''}|${payload.update_time || ''}`
      })),
      skipInvalidRows: false,
      ignoreUnknownValues: false
    };

    const response = bqSafeInsertAll(
      requestBody,
      config,
      tableId,
      () => {
        bqEnsureParentTable(config, tableId);
        bqEnsureColumns(config, tableId, columnDefs);
      }
    );

    if (response.insertErrors && response.insertErrors.length) {
      throw new Error('BigQuery parent insertErrors: ' + JSON.stringify(response.insertErrors));
    }
    if (rows.length) {
      console.log(`Exemple parent row: ${JSON.stringify(rows[0]).substring(0, 500)}...`);
    }
    console.log(`Ingestion parent réussie -> ${tableId}`);

    bqRecordAudit(config, formulaire, {
      targetTable: tableId,
      rowCount: rows.length,
      status: 'SUCCESS',
      durationMs: Date.now() - startTime,
      action: 'parent_insert'
    });
  } catch (e) {
    try {
      bqRecordAudit(config, formulaire, {
        targetTable: tableId,
        rowCount: rows.length,
        status: 'FAILURE',
        durationMs: Date.now() - startTime,
        action: 'parent_insert',
        errorMessage: e && e.message ? e.message : String(e)
      });
    } catch (auditError) {
      console.log('bqIngestParentBatch: audit failure -> ' + auditError);
    }
    handleException('bqIngestParentBatch', e, {
      table: tableId,
      rowCount: rows.length
    });
  }
}

function bqIngestSubTablesBatch(formulaire, tablesDefinition) {
  if (!tablesDefinition) return;
  const tableIds = Object.keys(tablesDefinition);
  if (!tableIds.length) return;

  const config = getBigQueryConfig();
  if (!config) {
    console.log('Configuration BigQuery absente, ingestion des tables filles ignorée.');
    return;
  }

  tableIds.forEach((tableId) => {
    const definition = tablesDefinition[tableId];
    if (!definition || !definition.rows || !definition.rows.length) return;
    const columnDefs = definition.columns
      ? Object.keys(definition.columns).map((name) => definition.columns[name])
      : [];

    const startTime = Date.now();
    const rowCount = definition.rows.length;

    try {
      bqEnsureSubTable(config, tableId);
      bqEnsureColumns(config, tableId, columnDefs, BQ_SUBTABLE_BASE_COLUMNS);

      const requestBody = {
        kind: 'bigquery#tableDataInsertAllRequest',
        rows: definition.rows.map((payload) => ({
          json: payload,
          insertId: `${payload.parent_data_id || ''}|${payload.sub_row_index || 0}|${payload.parent_update_time || ''}`
        })),
        skipInvalidRows: false,
        ignoreUnknownValues: false
      };

      const response = bqSafeInsertAll(
        requestBody,
        config,
        tableId,
        () => {
          bqEnsureSubTable(config, tableId);
          bqEnsureColumns(config, tableId, columnDefs, BQ_SUBTABLE_BASE_COLUMNS);
        }
      );

      if (response.insertErrors && response.insertErrors.length) {
        throw new Error('BigQuery sub-table insertErrors: ' + JSON.stringify(response.insertErrors));
      }
      if (definition.rows.length) {
        console.log(
          `Exemple sub row (${tableId}): ${JSON.stringify(definition.rows[0]).substring(0, 500)}...`
        );
      }
      console.log(`Ingestion table fille réussie -> ${tableId} (rows=${definition.rows.length})`);

      bqRecordAudit(config, formulaire, {
        targetTable: tableId,
        rowCount: rowCount,
        status: 'SUCCESS',
        durationMs: Date.now() - startTime,
        action: 'subform_insert'
      });
    } catch (e) {
      try {
        bqRecordAudit(config, formulaire, {
          targetTable: tableId,
          rowCount: rowCount,
          status: 'FAILURE',
          durationMs: Date.now() - startTime,
          action: 'subform_insert',
          errorMessage: e && e.message ? e.message : String(e)
        });
      } catch (auditError) {
        console.log('bqIngestSubTablesBatch: audit failure -> ' + auditError);
      }
      handleException('bqIngestSubTablesBatch', e, {
        table: tableId,
        rowCount: definition.rows ? definition.rows.length : 0
      });
    }
  });
}

function bqGetStreamingBufferInfo(config, tableId) {
  if (!config || !config.projectId || !config.datasetId || !tableId) return null;
  try {
    const table = BigQuery.Tables.get(config.projectId, config.datasetId, tableId);
    const buffer = table && table.streamingBuffer;
    if (!buffer) return null;

    const estimatedRows = Number(buffer.estimatedRows || 0);
    if (!estimatedRows) return null;

    const rawOldest = buffer.oldestEntryTime;
    const oldestMs = rawOldest !== undefined && rawOldest !== null ? Number(rawOldest) : NaN;
    const hasOldest = !Number.isNaN(oldestMs) && oldestMs > 0;

    return {
      rows: estimatedRows,
      oldestMs: hasOldest ? oldestMs : null,
      oldestIso: hasOldest ? new Date(oldestMs).toISOString() : 'unknown'
    };
  } catch (e) {
    console.log(`bqGetStreamingBufferInfo: échec récupération buffer ${config.datasetId}.${tableId} -> ${e}`);
    return null;
  }
}

function bqTableHasActiveStreamingBuffer(config, tableId) {
  const info = bqGetStreamingBufferInfo(config, tableId);
  if (!info) return false;
  console.log(
    `bqTableHasActiveStreamingBuffer: ${config.datasetId}.${tableId} -> rows=${info.rows}, oldest=${info.oldestIso}`
  );
  return true;
}

function bqWaitForStreamingBufferClear(config, tableId, options) {
  const retries = options && typeof options.retries === 'number' && options.retries >= 0 ? Math.floor(options.retries) : 3;
  const baseDelay = options && typeof options.delayMs === 'number' && options.delayMs >= 0 ? Math.floor(options.delayMs) : 5000;
  const minQuietMs = options && typeof options.minQuietMs === 'number' && options.minQuietMs >= 0 ? Math.floor(options.minQuietMs) : 90000;
  const maxTotalMs = options && typeof options.maxTotalMs === 'number' && options.maxTotalMs >= 0
    ? Math.floor(options.maxTotalMs)
    : (retries + 1) * Math.max(baseDelay, minQuietMs);

  let totalWait = 0;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const info = bqGetStreamingBufferInfo(config, tableId);
    if (!info) {
      return true;
    }

    const ageMs = info.oldestMs ? Date.now() - info.oldestMs : null;
    let sleepMs = baseDelay * Math.max(1, attempt + 1);

    if (ageMs !== null) {
      const quietGap = minQuietMs - ageMs;
      if (quietGap > 0) {
        sleepMs = Math.max(sleepMs, quietGap);
      }

      if (quietGap > 0 && quietGap > baseDelay * (attempt + 1)) {
        Utilities.sleep(quietGap);
        totalWait += quietGap;
        continue;
      }
    }

    if (totalWait + sleepMs > maxTotalMs) {
      sleepMs = Math.max(0, maxTotalMs - totalWait);
    }

    if (sleepMs <= 0) {
      break;
    }

    console.log(
      `bqWaitForStreamingBufferClear: buffer actif sur ${config.datasetId}.${tableId} (rows=${info.rows}, oldest=${info.oldestIso}), attente ${sleepMs} ms (tentative ${attempt + 1}/${retries + 1})`
    );
    Utilities.sleep(sleepMs);
    totalWait += sleepMs;
  }

  const stillActive = bqGetStreamingBufferInfo(config, tableId);
  if (stillActive) {
    console.log(
      `bqWaitForStreamingBufferClear: buffer encore actif après ${totalWait} ms (${config.datasetId}.${tableId}), rows=${stillActive.rows}, oldest=${stillActive.oldestIso}`
    );
    return false;
  }
  return true;
}

function bqPurgeDuplicateParentRows(config, formulaire, tableId, waitOptions) {
  const result = { deleted: 0, skipped: false, reason: null, message: null };
  if (!config || !tableId) {
    console.log('bqPurgeDuplicateParentRows: paramètres manquants, abandon.');
    result.skipped = true;
    result.reason = 'INVALID_PARAMS';
    result.message = 'Paramètres BigQuery manquants';
    return result;
  }
  const waitConfig = waitOptions || { retries: 2, delayMs: 5000, minQuietMs: 90000 };
  if (!bqWaitForStreamingBufferClear(config, tableId, waitConfig)) {
    console.log(`bqPurgeDuplicateParentRows: streaming buffer actif, purge annulée (${config.datasetId}.${tableId}).`);
    result.skipped = true;
    result.reason = 'STREAMING_BUFFER_ACTIVE';
    result.message = 'Buffer de streaming actif';
    return result;
  }
  const tableRef = `${config.projectId}.${config.datasetId}.${tableId}`;
  const qualifiedTable = `\`${tableRef}\``;
  const sql = `
DELETE FROM ${qualifiedTable}
WHERE STRUCT(form_unique_id, data_id, ingestion_time) IN (
  SELECT AS STRUCT form_unique_id, data_id, ingestion_time
  FROM (
    SELECT
      form_unique_id,
      data_id,
      ingestion_time,
      ROW_NUMBER() OVER (
        PARTITION BY form_unique_id
        ORDER BY update_time DESC, ingestion_time DESC
      ) AS row_num
    FROM ${qualifiedTable}
  )
  WHERE row_num > 1
)`;

  const startTime = Date.now();
  try {
    const response = BigQuery.Jobs.query({
      query: sql,
      useLegacySql: false,
      location: config.location || BQ_DEFAULT_CONFIG.location
    }, config.projectId);
    const deleted = Number(response.numDmlAffectedRows || 0);
    console.log(`bqPurgeDuplicateParentRows: ${deleted} doublons supprimés (${tableRef}).`);
    result.deleted = deleted;
    bqRecordAudit(config, formulaire, {
      targetTable: tableId,
      rowCount: deleted,
      status: 'SUCCESS',
      durationMs: Date.now() - startTime,
      action: 'parent_dedupe'
    });
    return result;
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    const isStreamingBufferError = typeof message === 'string' && message.toLowerCase().indexOf('streaming buffer') !== -1;
    if (isStreamingBufferError) {
      console.log(`bqPurgeDuplicateParentRows: streaming buffer actif, déduplication reportée (${tableRef}).`);
      try {
        bqRecordAudit(config, formulaire, {
          targetTable: tableId,
          rowCount: 0,
          status: 'SKIPPED',
          durationMs: Date.now() - startTime,
          action: 'parent_dedupe',
          errorMessage: message
        });
      } catch (auditError) {
        console.log('bqPurgeDuplicateParentRows: audit failure -> ' + auditError);
      }
      result.skipped = true;
      result.reason = 'STREAMING_BUFFER_ERROR';
      result.message = message;
      return result;
    }
    try {
      bqRecordAudit(config, formulaire, {
        targetTable: tableId,
        rowCount: 0,
        status: 'FAILURE',
        durationMs: Date.now() - startTime,
        action: 'parent_dedupe',
        errorMessage: message
      });
    } catch (auditError) {
      console.log('bqPurgeDuplicateParentRows: audit failure -> ' + auditError);
    }
    handleException('bqPurgeDuplicateParentRows', e, {
      table: tableId,
      project: config.projectId,
      dataset: config.datasetId
    });
    result.skipped = true;
    result.reason = 'ERROR';
    result.message = message;
    return result;
  }
}

function bqPurgeDuplicateSubTableRows(config, formulaire, tableId, waitOptions) {
  const result = { deleted: 0, skipped: false, reason: null, message: null };
  if (!config || !tableId) {
    console.log('bqPurgeDuplicateSubTableRows: paramètres manquants, abandon.');
    result.skipped = true;
    result.reason = 'INVALID_PARAMS';
    result.message = 'Paramètres BigQuery manquants';
    return result;
  }
  const waitConfig = waitOptions || { retries: 2, delayMs: 5000, minQuietMs: 90000 };
  if (!bqWaitForStreamingBufferClear(config, tableId, waitConfig)) {
    console.log(`bqPurgeDuplicateSubTableRows: streaming buffer actif, purge annulée (${config.datasetId}.${tableId}).`);
    result.skipped = true;
    result.reason = 'STREAMING_BUFFER_ACTIVE';
    result.message = 'Buffer de streaming actif';
    return result;
  }
  const tableRef = `${config.projectId}.${config.datasetId}.${tableId}`;
  const qualifiedTable = `\`${tableRef}\``;
  const sql = `
DELETE FROM ${qualifiedTable}
WHERE STRUCT(parent_data_id, sub_row_index, ingestion_time) IN (
  SELECT AS STRUCT parent_data_id, sub_row_index, ingestion_time
  FROM (
    SELECT
      parent_data_id,
      sub_row_index,
      ingestion_time,
      parent_update_time,
      ROW_NUMBER() OVER (
        PARTITION BY parent_data_id, sub_row_index
        ORDER BY parent_update_time DESC, ingestion_time DESC
      ) AS row_num
    FROM ${qualifiedTable}
  )
  WHERE row_num > 1
)`;

  const startTime = Date.now();
  try {
    const response = BigQuery.Jobs.query({
      query: sql,
      useLegacySql: false,
      location: config.location || BQ_DEFAULT_CONFIG.location
    }, config.projectId);
    const deleted = Number(response.numDmlAffectedRows || 0);
    console.log(`bqPurgeDuplicateSubTableRows: ${deleted} doublons supprimés (${tableRef}).`);
    result.deleted = deleted;
    bqRecordAudit(config, formulaire, {
      targetTable: tableId,
      rowCount: deleted,
      status: 'SUCCESS',
      durationMs: Date.now() - startTime,
      action: 'subform_dedupe'
    });
    return result;
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    const isStreamingBufferError = typeof message === 'string' && message.toLowerCase().indexOf('streaming buffer') !== -1;
    if (isStreamingBufferError) {
      console.log(`bqPurgeDuplicateSubTableRows: streaming buffer actif, déduplication reportée (${tableRef}).`);
      try {
        bqRecordAudit(config, formulaire, {
          targetTable: tableId,
          rowCount: 0,
          status: 'SKIPPED',
          durationMs: Date.now() - startTime,
          action: 'subform_dedupe',
          errorMessage: message
        });
      } catch (auditError) {
        console.log('bqPurgeDuplicateSubTableRows: audit failure -> ' + auditError);
      }
      result.skipped = true;
      result.reason = 'STREAMING_BUFFER_ERROR';
      result.message = message;
      return result;
    }
    try {
      bqRecordAudit(config, formulaire, {
        targetTable: tableId,
        rowCount: 0,
        status: 'FAILURE',
        durationMs: Date.now() - startTime,
        action: 'subform_dedupe',
        errorMessage: message
      });
    } catch (auditError) {
      console.log('bqPurgeDuplicateSubTableRows: audit failure -> ' + auditError);
    }
    handleException('bqPurgeDuplicateSubTableRows', e, {
      table: tableId,
      project: config.projectId,
      dataset: config.datasetId
    });
    result.skipped = true;
    result.reason = 'ERROR';
    result.message = message;
    return result;
  }
}

function bqPrepareMediaRows(formulaire, medias) {
  if (!medias || !medias.length) return [];
  const ingestionTime = new Date().toISOString();
  return medias
    .map((media) => {
      if (!media) return null;
      const driveFileId = String(media.driveFileId || '').trim();
      if (!driveFileId) return null;

      const rawPublicUrl =
        media.drivePublicUrl ||
        media.driveUrl ||
        media.driveViewUrl ||
        media.lh3Url ||
        '';
      let drivePublicUrl = rawPublicUrl ? normalizeDrivePublicUrl(rawPublicUrl) : '';
      const isNormalized =
        drivePublicUrl &&
        (drivePublicUrl.indexOf('https://drive.google.com/uc?export=view&id=') === 0 ||
          drivePublicUrl.indexOf('https://lh3.googleusercontent.com/d/') === 0);
      if (!isNormalized && driveFileId) {
        const sanitizedDriveFileId = driveFileId.replace(/[^A-Za-z0-9_-]/g, '');
        if (sanitizedDriveFileId) {
          drivePublicUrl = 'https://drive.google.com/uc?export=view&id=' + sanitizedDriveFileId;
        }
      }

      return {
        form_id: String(media.formId || formulaire?.id || ''),
        form_name: media.formName || formulaire?.nom || '',
        data_id: String(media.dataId || ''),
        form_unique_id: media.formUniqueId || '',
        field_name: media.fieldName || '',
        media_id: media.mediaId || '',
        media_type: media.fieldType || '',
        file_name: media.fileName || media.name || '',
        drive_file_id: driveFileId,
        drive_url: media.driveUrl || '',
        drive_view_url: media.driveViewUrl || '',
        drive_public_url: drivePublicUrl || '',
        folder_id: media.folderId || '',
        folder_url: media.folderUrl || '',
        parent_answer_time: bqNormalizeTimestamp(media.parentAnswerTime || ''),
        parent_update_time: bqNormalizeTimestamp(media.parentUpdateTime || ''),
        ingestion_time: ingestionTime
      };
    })
    .filter((row) => row && row.data_id && row.drive_file_id);
}

function bqIngestMediaBatch(formulaire, medias) {
  const rows = bqPrepareMediaRows(formulaire, medias);
  if (!rows.length) {
    console.log('Aucun média à ingérer dans BigQuery.');
    return;
  }

  const config = getBigQueryConfig();
  if (!config) {
    console.log('Configuration BigQuery absente, ingestion médias ignorée.');
    return;
  }

  const tableId = bqMediaTableId(formulaire);
  console.log(`Ingestion médias vers ${tableId} - lignes=${rows.length}`);

  const startTime = Date.now();

  try {
    bqEnsureMediaTable(config, tableId);

    const requestBody = {
      kind: 'bigquery#tableDataInsertAllRequest',
      rows: rows.map((payload) => ({
        json: payload,
        insertId: `${payload.form_id || formulaire?.id || ''}|${payload.data_id || ''}|${payload.drive_file_id || payload.file_name || ''}`
      })),
      skipInvalidRows: false,
      ignoreUnknownValues: false
    };

    const response = bqSafeInsertAll(
      requestBody,
      config,
      tableId,
      () => bqEnsureMediaTable(config, tableId)
    );

    if (response.insertErrors && response.insertErrors.length) {
      throw new Error('BigQuery media insertErrors: ' + JSON.stringify(response.insertErrors));
    }
    if (rows.length) {
      console.log(`Exemple média row: ${JSON.stringify(rows[0]).substring(0, 500)}...`);
    }
    console.log(`Ingestion médias réussie -> ${tableId}`);

    bqRecordAudit(config, formulaire, {
      targetTable: tableId,
      rowCount: rows.length,
      status: 'SUCCESS',
      durationMs: Date.now() - startTime,
      action: 'media_insert'
    });
  } catch (e) {
    try {
      bqRecordAudit(config, formulaire, {
        targetTable: tableId,
        rowCount: rows.length,
        status: 'FAILURE',
        durationMs: Date.now() - startTime,
        action: 'media_insert',
        errorMessage: e && e.message ? e.message : String(e)
      });
    } catch (auditError) {
      console.log('bqIngestMediaBatch: audit failure -> ' + auditError);
    }
    handleException('bqIngestMediaBatch', e, {
      table: tableId,
      rowCount: rows.length
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

  const rawPayload = bqBuildRawPayload(formulaire, record, {
    answerTime,
    updateTime,
    ingestionTime
  });

  let payloadJson = null;
  if (rawPayload !== null && rawPayload !== undefined) {
    try {
      payloadJson = JSON.stringify(rawPayload);
    } catch (e) {
      console.log(`bqBuildRawRow: échec stringify payload -> ${e}`);
      payloadJson = null;
    }
  }

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
    payload: payloadJson
  };

  return { insertId, payload };
}

function bqBuildRawPayload(formulaire, record, context) {
  const sanitizedRecord = bqCloneForJson(record || {});
  if (!sanitizedRecord || typeof sanitizedRecord !== 'object') {
    return sanitizedRecord || null;
  }

  sanitizedRecord.form_id = String(formulaire?.id || sanitizedRecord.form_id || sanitizedRecord.formId || '');
  sanitizedRecord.form_name = formulaire?.nom || sanitizedRecord.form_name || sanitizedRecord.formName || '';
  sanitizedRecord.data_id = String(record?.id || record?._id || sanitizedRecord.data_id || sanitizedRecord.dataId || '');

  if (context?.answerTime !== undefined) {
    sanitizedRecord.answer_time = context.answerTime;
  }
  if (context?.updateTime !== undefined) {
    sanitizedRecord.update_time = context.updateTime;
  }
  if (context?.ingestionTime !== undefined) {
    sanitizedRecord.ingestion_time = context.ingestionTime;
  }

  if (!sanitizedRecord.fields && record?.fields) {
    sanitizedRecord.fields = bqCloneForJson(record.fields);
  }

  return sanitizedRecord;
}

function bqCloneForJson(value) {
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) {
    return value.map((item) => {
      const cloned = bqCloneForJson(item);
      return cloned === undefined ? null : cloned;
    });
  }
  if (typeof value === 'object') {
    const clone = {};
    Object.keys(value).forEach((key) => {
      const cloned = bqCloneForJson(value[key]);
      if (cloned !== undefined) {
        clone[key] = cloned;
      }
    });
    return clone;
  }
  return value;
}

/**
 * Garantit l'existence de la table brute.
 * @param {{projectId:string, datasetId:string}} config
 */
function bqEnsureRawTable(config) {
  const tableRef = `${config.projectId}.${config.datasetId}.${BQ_RAW_TABLE_ID}`;
  console.log(`bqEnsureRawTable: vérification table ${tableRef}`);
  try {
    const table = BigQuery.Tables.get(config.projectId, config.datasetId, BQ_RAW_TABLE_ID);
    bqEnsureRawTableSchema(config, table);
    console.log(`Table raw déjà existante: ${tableRef}`);
    return;
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureRawTable: erreur inattendue lors du get -> ${err}`);
      throw err;
    }
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
        { name: 'payload', type: 'STRING', mode: 'NULLABLE' }
      ]
    },
    timePartitioning: bqBuildTimePartitioning('ingestion_time'),
    clustering: {
      fields: ['form_id']
    }
  };

  try {
    BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
    console.log(`Table raw créée: ${tableRef}`);
    bqEnsureRawTableSchema(config, tableResource);
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqEnsureRawTable: erreur inattendue lors de l'insertion -> ${err}`);
      throw err;
    }
    console.log(`bqEnsureRawTable: dataset introuvable lors de la création (${tableRef}), tentative de création du dataset.`);
    bqEnsureDataset(config);
    BigQuery.Tables.insert(tableResource, config.projectId, config.datasetId);
    console.log(`Table raw créée après création dataset: ${tableRef}`);
  }
}

function bqEnsureRawTableSchema(config, tableMetadata) {
  if (!tableMetadata || !tableMetadata.schema || !Array.isArray(tableMetadata.schema.fields)) return;
  const payloadField = tableMetadata.schema.fields.find((field) => field.name === 'payload');
  if (!payloadField) return;
  if (payloadField.type === 'STRING') return;

  console.log(`bqEnsureRawTableSchema: conversion de la colonne payload (${payloadField.type}) vers STRING.`);
  const tableId = `\`${config.projectId}.${config.datasetId}.${BQ_RAW_TABLE_ID}\``;
  const ddl = `ALTER TABLE ${tableId} ALTER COLUMN payload SET DATA TYPE STRING`;
  try {
    BigQuery.Jobs.query({
      query: ddl,
      useLegacySql: false,
      location: config.location || BQ_DEFAULT_CONFIG.location
    }, config.projectId);
    console.log('bqEnsureRawTableSchema: colonne payload convertie en STRING.');
  } catch (err) {
    console.log(`bqEnsureRawTableSchema: impossible de convertir la colonne payload -> ${err}`);
  }
}

/**
 * Enregistre un point d'audit minimal dans les logs BigQuery (table `etl_audit`).
 * La création de la table est automatique si elle n'existe pas.
 * @param {{projectId:string, datasetId:string}} config
 * @param {{id:string, nom:string}} formulaire
 * @param {number} rowCount
 */
function bqRecordAudit(config, formulaire, details) {
  const auditTableId = 'etl_audit';
  const payload = details || {};
  const now = new Date();
  const errorMessage = payload.errorMessage ? String(payload.errorMessage).substring(0, 500) : '';
  const rowCount = typeof payload.rowCount === 'number' ? payload.rowCount : 0;
  const durationMs = typeof payload.durationMs === 'number' ? payload.durationMs : 0;
  const row = {
    run_id: payload.runId || `${now.getTime()}_${formulaire?.id || payload.formId || 'unknown'}`,
    form_id: payload.formId || formulaire?.id || '',
    form_name: payload.formName || formulaire?.nom || '',
    target_table: payload.targetTable || '',
    action: payload.action || '',
    row_count: rowCount,
    status: payload.status || 'SUCCESS',
    duration_ms: durationMs,
    error_message: errorMessage,
    run_at: now.toISOString()
  };

  try {
    bqEnsureAuditTable(config, auditTableId);

    const requestBody = {
      kind: 'bigquery#tableDataInsertAllRequest',
      rows: [{ json: row, insertId: row.run_id }],
      skipInvalidRows: false,
      ignoreUnknownValues: false
    };
    const response = bqSafeInsertAll(
      requestBody,
      config,
      auditTableId,
      () => bqEnsureAuditTable(config, auditTableId)
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
    const table = BigQuery.Tables.get(config.projectId, config.datasetId, tableId);
    const existingFields = table?.schema?.fields || [];
    const additionalFields = [
      { name: 'target_table', type: 'STRING', mode: 'NULLABLE' },
      { name: 'action', type: 'STRING', mode: 'NULLABLE' },
      { name: 'duration_ms', type: 'INT64', mode: 'NULLABLE' },
      { name: 'error_message', type: 'STRING', mode: 'NULLABLE' }
    ];
    bqEnsureColumns(config, tableId, additionalFields, existingFields);
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
        { name: 'form_id', type: 'STRING', mode: 'NULLABLE' },
        { name: 'form_name', type: 'STRING', mode: 'NULLABLE' },
        { name: 'target_table', type: 'STRING', mode: 'NULLABLE' },
        { name: 'action', type: 'STRING', mode: 'NULLABLE' },
        { name: 'row_count', type: 'INT64', mode: 'NULLABLE' },
        { name: 'status', type: 'STRING', mode: 'NULLABLE' },
        { name: 'duration_ms', type: 'INT64', mode: 'NULLABLE' },
        { name: 'error_message', type: 'STRING', mode: 'NULLABLE' },
        { name: 'run_at', type: 'TIMESTAMP', mode: 'NULLABLE' }
      ]
    },
    timePartitioning: bqBuildTimePartitioning('run_at'),
    clustering: {
      fields: ['form_id']
    }
  };

  try {
    BigQuery.Tables.insert(resource, config.projectId, config.datasetId);
    console.log(`Table audit créée: ${config.projectId}.${config.datasetId}.${tableId}`);
  } catch (err) {
    if (!bqIsNotFound(err)) {
      throw err;
    }
    console.log(`bqEnsureAuditTable: dataset introuvable pour ${config.projectId}.${config.datasetId}, tentative de recréation.`);
    bqEnsureDataset(config);
    BigQuery.Tables.insert(resource, config.projectId, config.datasetId);
    console.log(`Table audit créée après création dataset: ${config.projectId}.${config.datasetId}.${tableId}`);
  }
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
  const normalized = message.toLowerCase();
  return (
    normalized.indexOf('not found') !== -1 ||
    normalized.indexOf('404') !== -1 ||
    normalized.indexOf('is deleted') !== -1 ||
    normalized.indexOf('does not exist') !== -1
  );
}
