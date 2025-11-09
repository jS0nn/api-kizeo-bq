/**
 * Fonctions utilitaires pour initialiser et alimenter BigQuery.
 * Les identifiants de projet et dataset sont lus dans les ScriptProperties:
 *   - BQ_PROJECT_ID
 *   - BQ_DATASET
 *   - BQ_LOCATION (optionnel, utilisé pour la journalisation)
 */
const BQ_RAW_TABLE_ID = 'kizeo_raw_events';
const BQ_FIELD_DICTIONARY_TABLE_ID = 'etl_field_dictionary';
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
const KIZEO_LOCAL_TIMEZONE = 'Europe/Paris';

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
  { name: 'form_id', type: 'STRING', mode: 'REQUIRED', label: 'form_id', sourceType: 'base' },
  { name: 'form_name', type: 'STRING', mode: 'NULLABLE', label: 'form_name', sourceType: 'base' },
  { name: 'data_id', type: 'STRING', mode: 'REQUIRED', label: 'data_id', sourceType: 'base' },
  { name: 'form_unique_id', type: 'STRING', mode: 'NULLABLE', label: 'form_unique_id', sourceType: 'base' },
  { name: 'user_id', type: 'STRING', mode: 'NULLABLE', label: 'user_id', sourceType: 'base' },
  { name: 'user_last_name', type: 'STRING', mode: 'NULLABLE', label: 'user_last_name', sourceType: 'base' },
  { name: 'user_first_name', type: 'STRING', mode: 'NULLABLE', label: 'user_first_name', sourceType: 'base' },
  { name: 'answer_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'answer_time', sourceType: 'base' },
  { name: 'answer_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'answer_time_cet', sourceType: 'base' },
  { name: 'update_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'update_time', sourceType: 'base' },
  { name: 'update_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'update_time_cet', sourceType: 'base' },
  { name: 'origin_answer', type: 'STRING', mode: 'NULLABLE', label: 'origin_answer', sourceType: 'base' },
  { name: 'ingestion_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'ingestion_time', sourceType: 'base' },
  { name: 'ingestion_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'ingestion_time_cet', sourceType: 'base' }
];

const BQ_SUBTABLE_BASE_COLUMNS = [
  { name: 'form_id', type: 'STRING', mode: 'REQUIRED', label: 'form_id', sourceType: 'base' },
  { name: 'form_name', type: 'STRING', mode: 'NULLABLE', label: 'form_name', sourceType: 'base' },
  { name: 'parent_data_id', type: 'STRING', mode: 'REQUIRED', label: 'parent_data_id', sourceType: 'base' },
  { name: 'parent_form_unique_id', type: 'STRING', mode: 'NULLABLE', label: 'parent_form_unique_id', sourceType: 'base' },
  { name: 'parent_answer_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'parent_answer_time', sourceType: 'base' },
  { name: 'parent_answer_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'parent_answer_time_cet', sourceType: 'base' },
  { name: 'parent_update_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'parent_update_time', sourceType: 'base' },
  { name: 'parent_update_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'parent_update_time_cet', sourceType: 'base' },
  { name: 'sub_row_index', type: 'INT64', mode: 'NULLABLE', label: 'sub_row_index', sourceType: 'base' },
  { name: 'ingestion_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'ingestion_time', sourceType: 'base' },
  { name: 'ingestion_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'ingestion_time_cet', sourceType: 'base' }
];

const BQ_MEDIA_BASE_COLUMNS = [
  { name: 'form_id', type: 'STRING', mode: 'REQUIRED', label: 'form_id', sourceType: 'base' },
  { name: 'form_name', type: 'STRING', mode: 'NULLABLE', label: 'form_name', sourceType: 'base' },
  { name: 'data_id', type: 'STRING', mode: 'REQUIRED', label: 'data_id', sourceType: 'base' },
  { name: 'form_unique_id', type: 'STRING', mode: 'NULLABLE', label: 'form_unique_id', sourceType: 'base' },
  { name: 'field_name', type: 'STRING', mode: 'NULLABLE', label: 'field_name', sourceType: 'base' },
  { name: 'media_id', type: 'STRING', mode: 'NULLABLE', label: 'media_id', sourceType: 'base' },
  { name: 'media_type', type: 'STRING', mode: 'NULLABLE', label: 'media_type', sourceType: 'base' },
  { name: 'file_name', type: 'STRING', mode: 'NULLABLE', label: 'file_name', sourceType: 'base' },
  { name: 'drive_file_id', type: 'STRING', mode: 'NULLABLE', label: 'drive_file_id', sourceType: 'base' },
  { name: 'drive_url', type: 'STRING', mode: 'NULLABLE', label: 'drive_url', sourceType: 'base' },
  { name: 'drive_view_url', type: 'STRING', mode: 'NULLABLE', label: 'drive_view_url', sourceType: 'base' },
  { name: 'drive_public_url', type: 'STRING', mode: 'NULLABLE', label: 'drive_public_url', sourceType: 'base' },
  { name: 'folder_id', type: 'STRING', mode: 'NULLABLE', label: 'folder_id', sourceType: 'base' },
  { name: 'folder_url', type: 'STRING', mode: 'NULLABLE', label: 'folder_url', sourceType: 'base' },
  { name: 'parent_answer_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'parent_answer_time', sourceType: 'base' },
  { name: 'parent_update_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'parent_update_time', sourceType: 'base' },
  { name: 'parent_answer_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'parent_answer_time_cet', sourceType: 'base' },
  { name: 'parent_update_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'parent_update_time_cet', sourceType: 'base' },
  { name: 'ingestion_time', type: 'TIMESTAMP', mode: 'NULLABLE', label: 'ingestion_time', sourceType: 'base' },
  { name: 'ingestion_time_cet', type: 'DATETIME', mode: 'NULLABLE', label: 'ingestion_time_cet', sourceType: 'base' }
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
    bqEnsureFieldDictionaryTable(config);
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
function describeMissingBigQueryConfig(missingKeys) {
  const keys = missingKeys.join(', ');
  return (
    `Configuration BigQuery incomplète (${keys}). ` +
    'Initialisez-la via le menu "Configurer BigQuery" ou exécutez initBigQueryConfig().'
  );
}

function getBigQueryConfig(options) {
  const opts = options || {};
  const props = getConfigStore();
  const projectIdRaw = props.getProperty(BQ_CONFIG_KEYS.project) || '';
  const datasetIdRaw = props.getProperty(BQ_CONFIG_KEYS.dataset) || '';
  const locationRaw = props.getProperty(BQ_CONFIG_KEYS.location) || '';

  const projectId = projectIdRaw.trim();
  const datasetId = datasetIdRaw.trim();
  const location = locationRaw.trim() || BQ_DEFAULT_CONFIG.location || '';

  const missingKeys = [];
  if (!projectId) {
    missingKeys.push(BQ_CONFIG_KEYS.project);
  }
  if (!datasetId) {
    missingKeys.push(BQ_CONFIG_KEYS.dataset);
  }

  if (missingKeys.length) {
    const message = describeMissingBigQueryConfig(missingKeys);
    if (opts.throwOnMissing) {
      const error = new Error(message);
      error.name = 'BigQueryConfigError';
      error.missingKeys = missingKeys;
      throw error;
    }
    console.log(message);
    return null;
  }

  const config = {
    projectId: projectId,
    datasetId: datasetId,
    location: location || null
  };

  console.log(
    `BQ Config -> project=${config.projectId}, dataset=${config.datasetId}, location=${config.location || 'default'}`
  );
  return config;
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
    bqEnsureFieldDictionaryTable(config);
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
    .replace(BQ_IDENTIFIER_REGEX, '_')
    .replace(/_+/g, '_');
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
    .replace(/_{3,}/g, '__')
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

function bqFetchExistingColumns(config, tableId, baseColumns) {
  if (Array.isArray(baseColumns) && baseColumns.length) {
    return baseColumns;
  }
  try {
    const table = BigQuery.Tables.get(config.projectId, config.datasetId, tableId);
    const fields = table && table.schema && Array.isArray(table.schema.fields) ? table.schema.fields : [];
    if (fields && fields.length) {
      return fields;
    }
  } catch (err) {
    if (!bqIsNotFound(err)) {
      console.log(`bqFetchExistingColumns: impossible de récupérer le schéma actuel (${tableId}) -> ${err}`);
    }
  }
  return [];
}

function bqEnsureColumns(config, tableId, columns, baseColumns) {
  if (!columns || !columns.length) {
    return { added: [], convertedToString: [] };
  }

  const tableRef = `\`${config.projectId}.${config.datasetId}.${tableId}\``;
  const existingFields = bqFetchExistingColumns(config, tableId, baseColumns);
  const existingMap = {};
  const addedColumns = [];
  const alterStatements = [];
  const dropNotNullStatements = [];
  const convertedToString = [];

  existingFields.forEach((field) => {
    if (!field || !field.name) return;
    existingMap[field.name] = {
      type: (field.type || 'STRING').toUpperCase(),
      mode: (field.mode || 'NULLABLE').toUpperCase()
    };
  });

  columns.forEach((col) => {
    if (!col || !col.name) return;
    const name = col.name;
    const desiredType = (col.type || 'STRING').toUpperCase();
    const desiredMode = (col.mode || 'NULLABLE').toUpperCase();
    const existing = existingMap[name];

    if (!existing) {
      const definition = bqBuildColumnDefinition(col);
      addedColumns.push(`ADD COLUMN IF NOT EXISTS ${definition}`);
      existingMap[name] = { type: desiredType, mode: desiredMode };
      return;
    }

    const existingType = existing.type;
    const existingMode = existing.mode;

    if (existingMode === 'REQUIRED' && desiredMode !== 'REQUIRED') {
      dropNotNullStatements.push(`ALTER COLUMN ${name} DROP NOT NULL`);
      existing.mode = 'NULLABLE';
    }

    if (desiredType === 'STRING' && existingType !== 'STRING') {
      const targetTypeExpression =
        existingMode === 'REPEATED' || desiredMode === 'REPEATED' ? 'ARRAY<STRING>' : 'STRING';
      alterStatements.push(`ALTER COLUMN ${name} SET DATA TYPE ${targetTypeExpression}`);
      convertedToString.push({
        name: name,
        repeated: existingMode === 'REPEATED' || desiredMode === 'REPEATED'
      });
      existing.type = 'STRING';
      col.type = 'STRING';
      col.mode = existingMode === 'REPEATED' || desiredMode === 'REPEATED' ? 'REPEATED' : 'NULLABLE';
      return;
    }

    if (desiredType === 'JSON' && existingType !== 'JSON') {
      alterStatements.push(`ALTER COLUMN ${name} SET DATA TYPE JSON`);
      existing.type = 'JSON';
      return;
    }

    if (desiredType === 'FLOAT64' && existingType === 'INT64') {
      alterStatements.push(`ALTER COLUMN ${name} SET DATA TYPE FLOAT64`);
      existing.type = 'FLOAT64';
      return;
    }
  });

  if (addedColumns.length) {
    const ddl = `ALTER TABLE ${tableRef} ${addedColumns.join(', ')}`;
    console.log(ddl);
    BigQuery.Jobs.query({ query: ddl, useLegacySql: false }, config.projectId);
  }

  dropNotNullStatements.forEach((ddl) => {
    console.log(`ALTER TABLE ${tableRef} ${ddl}`);
    BigQuery.Jobs.query({ query: `ALTER TABLE ${tableRef} ${ddl}`, useLegacySql: false }, config.projectId);
  });

  alterStatements.forEach((ddl) => {
    console.log(`ALTER TABLE ${tableRef} ${ddl}`);
    BigQuery.Jobs.query({ query: `ALTER TABLE ${tableRef} ${ddl}`, useLegacySql: false }, config.projectId);
  });

  return {
    added: addedColumns,
    convertedToString,
    droppedNotNull: dropNotNullStatements,
    altered: alterStatements
  };
}

function bqApplySchemaAdjustmentsToRows(rows, adjustments) {
  if (!rows || !rows.length || !adjustments) {
    return rows;
  }
  const convertEntries = (adjustments.convertedToString || [])
    .map((entry) => {
      if (!entry) return null;
      if (typeof entry === 'string') {
        return { name: entry, repeated: false };
      }
      if (typeof entry.name === 'string' && entry.name) {
        return { name: entry.name, repeated: !!entry.repeated };
      }
      return null;
    })
    .filter((entry) => entry && entry.name);
  if (!convertEntries.length) {
    return rows;
  }
  rows.forEach((row) => {
    if (!row) return;
    convertEntries.forEach((entry) => {
      if (!Object.prototype.hasOwnProperty.call(row, entry.name)) return;
      row[entry.name] = entry.repeated
        ? bqCoerceValueToStringArray(row[entry.name])
        : bqCoerceValueToString(row[entry.name]);
    });
  });
  return rows;
}

function bqCoerceValueToString(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (Array.isArray(value)) {
    const normalized = value
      .map((item) => (item === null || item === undefined ? '' : String(item)))
      .filter((item) => item !== '');
    return normalized.length ? normalized.join(', ') : null;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (err) {
      return String(value);
    }
  }
  return String(value);
}

function bqCoerceValueToStringArray(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const sourceArray = Array.isArray(value) ? value : [value];
  const normalized = sourceArray
    .map((item) => (item === null || item === undefined ? '' : String(item)))
    .filter((item) => item !== '');
  return normalized.length ? normalized : null;
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
  const answerTimeRaw = data.answer_time || data._answer_time || null;
  const updateTimeRaw = data.update_time || data._update_time || null;
  const ingestionTime = new Date().toISOString();
  const row = {
    form_id: String(data.form_id || formulaire?.id || ''),
    form_name: formulaire?.nom || '',
    data_id: String(data.id || data._id || ''),
    form_unique_id: data.form_unique_id || '',
    user_id: data.user_id || '',
    user_last_name: data.last_name || '',
    user_first_name: data.first_name || '',
    answer_time: bqNormalizeTimestamp(answerTimeRaw),
    answer_time_cet: bqFormatCetDatetime(answerTimeRaw),
    update_time: bqNormalizeTimestamp(updateTimeRaw),
    update_time_cet: bqFormatCetDatetime(updateTimeRaw),
    origin_answer: data.origin_answer || '',
    ingestion_time: ingestionTime,
    ingestion_time_cet: bqFormatCetDatetime(ingestionTime)
  };

  const parentContext = {
    form_id: row.form_id,
    form_name: row.form_name,
    data_id: row.data_id,
    form_unique_id: row.form_unique_id,
    answer_time: row.answer_time,
    answer_time_cet: row.answer_time_cet,
    update_time: row.update_time,
    update_time_cet: row.update_time_cet,
    ingestion_time: row.ingestion_time,
    ingestion_time_cet: row.ingestion_time_cet
  };

  const fields = data.fields || {};
  for (const rawName in fields) {
    if (!Object.prototype.hasOwnProperty.call(fields, rawName)) continue;
    const field = fields[rawName];
    if (!field) continue;

    const fieldTypeRaw = field.type || '';
    const fieldTypeNormalized = fieldTypeRaw.toString().toLowerCase();

    if (isSubformField(fieldTypeRaw, field.value)) {
      const subformRows = normalizeSubformRows(field.value);
      const baseName = bqSlugifyIdentifier(rawName || 'subform');
      const tableColumnName = bqEnsureUniqueName(`table_${baseName}`, usedNames);
      const countColumnName = bqEnsureUniqueName(`table_${baseName}_row_count`, usedNames);
      const subTableId = bqSubTableId(formulaire, rawName);

      row[tableColumnName] = subformRows.length ? subTableId : null;
      row[countColumnName] = subformRows.length ? subformRows.length : 0;

      dynamicColumns.push({
        name: tableColumnName,
        type: 'STRING',
        mode: 'NULLABLE',
        label: rawName,
        sourceType: 'subform_reference'
      });
      dynamicColumns.push({
        name: countColumnName,
        type: 'INT64',
        mode: 'NULLABLE',
        label: rawName,
        sourceType: 'subform_row_count'
      });

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
    dynamicColumns.push({
      name: columnName,
      type: conversion.type,
      mode: conversion.mode,
      label: rawName,
      sourceType: fieldTypeNormalized || 'field'
    });

    if (Array.isArray(conversion.extraColumns) && conversion.extraColumns.length) {
      conversion.extraColumns.forEach((extraColumn) => {
        if (!extraColumn) return;
        const suffix = typeof extraColumn.suffix === 'string' && extraColumn.suffix.length
          ? extraColumn.suffix
          : '_cet';
        const extraBaseName = `${baseName}${suffix}`;
        const extraColumnName = bqEnsureUniqueName(extraBaseName, usedNames);
        const labelSuffix = typeof extraColumn.labelSuffix === 'string' && extraColumn.labelSuffix.length
          ? extraColumn.labelSuffix
          : ' (CET)';
        const columnLabel = extraColumn.label || `${rawName || baseName}${labelSuffix}`;
        const columnSourceType = extraColumn.sourceType || `${(fieldTypeNormalized || 'field')}_cet`;
        row[extraColumnName] = typeof extraColumn.value === 'undefined' ? null : extraColumn.value;
        dynamicColumns.push({
          name: extraColumnName,
          type: extraColumn.type || 'STRING',
          mode: extraColumn.mode || conversion.mode || 'NULLABLE',
          label: columnLabel,
          sourceType: columnSourceType
        });
      });
    }
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
    case 'timestamp': {
      const normalizedTimestamp = arrayValue
        ? rawValue.map((v) => bqNormalizeTimestamp(v))
        : bqNormalizeTimestamp(rawValue);
      result.type = 'TIMESTAMP';
      result.value = normalizedTimestamp;
      if (arrayValue) {
        result.mode = 'REPEATED';
      }

      const cetValue = arrayValue
        ? rawValue.map((v) => bqFormatCetDatetime(v))
        : bqFormatCetDatetime(rawValue);
      result.extraColumns = [
        {
          suffix: '_cet',
          type: 'DATETIME',
          mode: arrayValue ? 'REPEATED' : 'NULLABLE',
          value: cetValue,
          labelSuffix: ' (CET)',
          sourceType: 'datetime_cet'
        }
      ];
      break;
    }
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
    columnMap[columnName] = {
      name: columnName,
      type: 'STRING',
      mode: 'NULLABLE',
      label: rawKey || key,
      sourceType: 'subform_field'
    };
    return columnName;
  };

  const preparedRows = rows.map((rowValues, index) => {
    const payload = {
      form_id: parentContext.form_id,
      form_name: parentContext.form_name,
      parent_data_id: parentContext.data_id,
      parent_form_unique_id: parentContext.form_unique_id,
      parent_answer_time: parentContext.answer_time,
      parent_answer_time_cet: parentContext.answer_time_cet,
      parent_update_time: parentContext.update_time,
      parent_update_time_cet: parentContext.update_time_cet,
      sub_row_index: index,
      ingestion_time: parentContext.ingestion_time,
      ingestion_time_cet: parentContext.ingestion_time_cet
    };

    if (rowValues && typeof rowValues === 'object') {
      Object.keys(rowValues).forEach((rawKey) => {
        const columnName = ensureColumnName(rawKey);
        payload[columnName] = bqSerializeSubformValue(rowValues[rawKey]);
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
    const schemaAdjustments = bqEnsureColumns(config, tableId, columnDefs);
    const normalizedColumns = (columnDefs || []).map((col) => {
      const clone = Object.assign({}, col);
      const conversions = schemaAdjustments && Array.isArray(schemaAdjustments.convertedToString)
        ? schemaAdjustments.convertedToString
        : [];
      const matchedConversion = conversions.find((entry) => {
        if (!entry) return false;
        if (typeof entry === 'string') return entry === col.name;
        return entry.name === col.name;
      });
      if (matchedConversion) {
        clone.type = 'STRING';
        clone.mode = matchedConversion.repeated ? 'REPEATED' : 'NULLABLE';
      }
      return clone;
    });
    bqRecordFieldDictionaryEntries(config, formulaire, tableId, normalizedColumns, BQ_PARENT_BASE_COLUMNS);
    const preparedRows = bqApplySchemaAdjustmentsToRows(rows, schemaAdjustments);

    const requestBody = {
      kind: 'bigquery#tableDataInsertAllRequest',
      rows: preparedRows.map((payload) => ({
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
        bqRecordFieldDictionaryEntries(config, formulaire, tableId, normalizedColumns, BQ_PARENT_BASE_COLUMNS);
      }
    );

    if (response.insertErrors && response.insertErrors.length) {
      throw new Error('BigQuery parent insertErrors: ' + JSON.stringify(response.insertErrors));
    }
    if (preparedRows.length) {
      console.log(`Exemple parent row: ${JSON.stringify(preparedRows[0]).substring(0, 500)}...`);
    }
    console.log(`Ingestion parent réussie -> ${tableId}`);

    bqRecordAudit(config, formulaire, {
      targetTable: tableId,
      rowCount: preparedRows.length,
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
      const schemaAdjustments = bqEnsureColumns(config, tableId, columnDefs, BQ_SUBTABLE_BASE_COLUMNS);
      const normalizedColumns = columnDefs.map((col) => {
        const clone = Object.assign({}, col);
        const conversions = schemaAdjustments && Array.isArray(schemaAdjustments.convertedToString)
          ? schemaAdjustments.convertedToString
          : [];
        const matchedConversion = conversions.find((entry) => {
          if (!entry) return false;
          if (typeof entry === 'string') return entry === col.name;
          return entry.name === col.name;
        });
        if (matchedConversion) {
          clone.type = 'STRING';
          clone.mode = matchedConversion.repeated ? 'REPEATED' : 'NULLABLE';
        }
        return clone;
      });
      bqRecordFieldDictionaryEntries(config, formulaire, tableId, normalizedColumns, BQ_SUBTABLE_BASE_COLUMNS);
      const preparedRows = bqApplySchemaAdjustmentsToRows(definition.rows, schemaAdjustments);
      const insertedCount = preparedRows.length;

      const requestBody = {
        kind: 'bigquery#tableDataInsertAllRequest',
        rows: preparedRows.map((payload) => ({
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
          bqRecordFieldDictionaryEntries(config, formulaire, tableId, normalizedColumns, BQ_SUBTABLE_BASE_COLUMNS);
        }
      );

      if (response.insertErrors && response.insertErrors.length) {
        throw new Error('BigQuery sub-table insertErrors: ' + JSON.stringify(response.insertErrors));
      }
      if (insertedCount) {
        console.log(
          `Exemple sub row (${tableId}): ${JSON.stringify(preparedRows[0]).substring(0, 500)}...`
        );
      }
      console.log(`Ingestion table fille réussie -> ${tableId} (rows=${insertedCount})`);

      bqRecordAudit(config, formulaire, {
        targetTable: tableId,
        rowCount: insertedCount,
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
  const ingestionTimeCet = bqFormatCetDatetime(ingestionTime);
  const driveService =
    typeof DriveMediaService !== 'undefined' &&
    DriveMediaService &&
    typeof DriveMediaService.getDefault === 'function'
      ? DriveMediaService.getDefault()
      : null;
  const normalizePublicUrl =
    driveService && typeof driveService.normalizeDrivePublicUrl === 'function'
      ? driveService.normalizeDrivePublicUrl.bind(driveService)
      : typeof normalizeDrivePublicUrl === 'function'
      ? normalizeDrivePublicUrl
      : function (url) {
          return url || '';
        };
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
      let drivePublicUrl = rawPublicUrl ? normalizePublicUrl(rawPublicUrl) : '';
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

      const parentAnswerTime = bqNormalizeTimestamp(media.parentAnswerTime || '');
      const parentUpdateTime = bqNormalizeTimestamp(media.parentUpdateTime || '');
      const parentAnswerTimeCet = bqFormatCetDatetime(media.parentAnswerTime || '');
      const parentUpdateTimeCet = bqFormatCetDatetime(media.parentUpdateTime || '');

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
        parent_answer_time: parentAnswerTime,
        parent_answer_time_cet: parentAnswerTimeCet,
        parent_update_time: parentUpdateTime,
        parent_update_time_cet: parentUpdateTimeCet,
        ingestion_time: ingestionTime,
        ingestion_time_cet: ingestionTimeCet
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
    bqRecordFieldDictionaryEntries(config, formulaire, tableId, [], BQ_MEDIA_BASE_COLUMNS);

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
      () => {
        bqEnsureMediaTable(config, tableId);
        bqRecordFieldDictionaryEntries(config, formulaire, tableId, [], BQ_MEDIA_BASE_COLUMNS);
      }
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

function bqEnsureFieldDictionaryTable(config) {
  try {
    BigQuery.Tables.get(config.projectId, config.datasetId, BQ_FIELD_DICTIONARY_TABLE_ID);
    return;
  } catch (err) {
    if (!bqIsNotFound(err)) {
      throw err;
    }
  }

  const resource = {
    tableReference: {
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId: BQ_FIELD_DICTIONARY_TABLE_ID
    },
    schema: {
      fields: [
        { name: 'form_id', type: 'STRING', mode: 'NULLABLE' },
        { name: 'form_name', type: 'STRING', mode: 'NULLABLE' },
        { name: 'table_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'field_slug', type: 'STRING', mode: 'REQUIRED' },
        { name: 'field_label', type: 'STRING', mode: 'NULLABLE' },
        { name: 'field_type', type: 'STRING', mode: 'NULLABLE' },
        { name: 'field_mode', type: 'STRING', mode: 'NULLABLE' },
        { name: 'source_type', type: 'STRING', mode: 'NULLABLE' },
        { name: 'last_seen_at', type: 'TIMESTAMP', mode: 'NULLABLE' }
      ]
    },
    timePartitioning: bqBuildTimePartitioning('last_seen_at'),
    clustering: {
      fields: ['table_id', 'field_slug']
    }
  };

  try {
    BigQuery.Tables.insert(resource, config.projectId, config.datasetId);
    console.log(`Table dictionnaire créée: ${config.projectId}.${config.datasetId}.${BQ_FIELD_DICTIONARY_TABLE_ID}`);
  } catch (err) {
    if (!bqIsNotFound(err)) {
      throw err;
    }
    console.log(
      `bqEnsureFieldDictionaryTable: dataset introuvable pour ${config.projectId}.${config.datasetId}, tentative de recréation.`
    );
    bqEnsureDataset(config);
    BigQuery.Tables.insert(resource, config.projectId, config.datasetId);
    console.log(
      `Table dictionnaire créée après création dataset: ${config.projectId}.${config.datasetId}.${BQ_FIELD_DICTIONARY_TABLE_ID}`
    );
  }
}

function bqRecordFieldDictionaryEntries(config, formulaire, tableId, columns, baseColumns) {
  if (!config || !config.projectId || !config.datasetId || !tableId) return;
  const entries = [];
  const seen = new Set();

  const appendEntry = (column, fallbackSource) => {
    if (!column || !column.name) return;
    const slug = column.name;
    if (seen.has(slug)) return;
    seen.add(slug);
    entries.push({
      form_id: formulaire?.id || '',
      form_name: formulaire?.nom || '',
      table_id: tableId,
      field_slug: slug,
      field_label: column.label || slug,
      field_type: column.type || 'STRING',
      field_mode: column.mode || 'NULLABLE',
      source_type: column.sourceType || fallbackSource || 'dynamic'
    });
  };

  if (Array.isArray(baseColumns)) {
    baseColumns.forEach((column) => appendEntry(column, 'base'));
  }
  if (Array.isArray(columns)) {
    columns.forEach((column) => appendEntry(column, 'dynamic'));
  }

  if (!entries.length) return;

  bqEnsureFieldDictionaryTable(config);

  const nowIso = new Date().toISOString();
  const nowMillis = Date.now();
  const rows = entries.map((entry, index) => {
    const payload = Object.assign({}, entry, { last_seen_at: nowIso });
    return {
      json: payload,
      insertId: `${tableId}|${entry.field_slug}|${nowMillis + index}`
    };
  });

  const requestBody = {
    kind: 'bigquery#tableDataInsertAllRequest',
    rows,
    skipInvalidRows: false,
    ignoreUnknownValues: false
  };

  try {
    bqSafeInsertAll(
      requestBody,
      config,
      BQ_FIELD_DICTIONARY_TABLE_ID,
      () => bqEnsureFieldDictionaryTable(config)
    );
  } catch (err) {
    handleException('bqRecordFieldDictionaryEntries', err, {
      table: BQ_FIELD_DICTIONARY_TABLE_ID,
      targetTable: tableId
    });
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

function bqFormatCetDatetime(value) {
  if (!value) return null;
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (isNaN(date.getTime())) return null;
    if (typeof Utilities === 'object' && Utilities.formatDate) {
      return Utilities.formatDate(date, KIZEO_LOCAL_TIMEZONE, 'yyyy-MM-dd HH:mm:ss');
    }
    const pad = (num) => String(num).padStart(2, '0');
    return (
      date.getUTCFullYear() +
      '-' +
      pad(date.getUTCMonth() + 1) +
      '-' +
      pad(date.getUTCDate()) +
      ' ' +
      pad(date.getUTCHours()) +
      ':' +
      pad(date.getUTCMinutes()) +
      ':' +
      pad(date.getUTCSeconds())
    );
  } catch (e) {
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
