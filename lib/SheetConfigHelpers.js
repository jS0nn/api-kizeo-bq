// SheetConfigHelpers Version 0.1.0

function sheetConfigCreateService(options) {
  if (!options || typeof options !== 'object') {
    throw new Error('SheetConfigHelpers.create requiert un objet options.');
  }

  var requiredKeys = Array.isArray(options.requiredKeys) ? options.requiredKeys.slice() : [];
  var configHeaders = Array.isArray(options.configHeaders) ? options.configHeaders.slice() : [];
  var batchLimitKey = options.batchLimitKey || 'batch_limit';
  var ingestFlagKey = options.ingestFlagKey || 'ingest_bigquery';
  var defaultBatchLimit =
    typeof options.defaultBatchLimit === 'number' && Number.isFinite(options.defaultBatchLimit)
      ? options.defaultBatchLimit
      : 30;
  var sanitizeBatchLimitValue =
    typeof options.sanitizeBatchLimitValue === 'function'
      ? options.sanitizeBatchLimitValue
      : function (value) {
          return value;
        };
  var sanitizeBooleanFlag =
    typeof options.sanitizeBooleanFlag === 'function'
      ? options.sanitizeBooleanFlag
      : function (value) {
          return value;
        };
  var computeTableName =
    typeof options.computeTableName === 'function'
      ? options.computeTableName
      : function () {
          return null;
        };
  var maxTableNameLength =
    typeof options.maxTableNameLength === 'number' && Number.isFinite(options.maxTableNameLength)
      ? options.maxTableNameLength
      : 128;
  var applyLayout =
    typeof options.applyLayout === 'function'
      ? options.applyLayout
      : function () {};
  var getConfiguredBatchLimit =
    typeof options.getConfiguredBatchLimit === 'function'
      ? options.getConfiguredBatchLimit
      : function () {
          return defaultBatchLimit;
        };

  function readConfigFromSheet(sheet) {
    if (!sheet) return {};
    var lastRow = sheet.getLastRow();
    if (lastRow < 2) {
      return {};
    }
    var values = sheet.getRange(2, 1, lastRow - 1, 2).getValues();
    var config = {};
    for (var index = 0; index < values.length; index++) {
      var row = values[index];
      var key = row[0];
      if (!key) continue;
      config[String(key).trim()] = row[1];
    }
    return config;
  }

  function writeConfigToSheet(sheet, config) {
    if (!sheet) return;
    var existingRowCount = Math.max(sheet.getLastRow() - 1, 0);
    if (existingRowCount > 0) {
      sheet.getRange(2, 1, existingRowCount, 2).clearContent();
    }

    var entries = new Map();
    if (config && typeof config === 'object') {
      Object.keys(config).forEach(function (key) {
        var trimmedKey = String(key || '').trim();
        if (!trimmedKey) return;
        entries.set(trimmedKey, config[key]);
      });
    }

    entries.delete('bq_alias');

    var rows = [];
    var rowIndexMap = {};

    configHeaders.forEach(function (header) {
      var value = entries.has(header) ? entries.get(header) : '';
      rowIndexMap[header] = rows.length;
      rows.push([header, value]);
      entries.delete(header);
    });

    entries.forEach(function (value, key) {
      rowIndexMap[key] = rows.length;
      rows.push([key, value]);
    });

    if (rows.length) {
      sheet.getRange(2, 1, rows.length, 2).setValues(rows);
    }

    if (typeof applyLayout === 'function') {
      applyLayout(sheet, rows.length, rowIndexMap);
    }
  }

  function validateConfig(rawConfig, sheet) {
    var config = rawConfig || {};
    var errors = [];
    var sanitized = {};

    requiredKeys.forEach(function (key) {
      var rawValue = config[key];
      var value = rawValue !== undefined && rawValue !== null ? String(rawValue).trim() : '';
      if (!value) {
        errors.push({ key: key, message: 'Champ ' + key + ' manquant ou vide.' });
      } else {
        sanitized[key] = value;
      }
    });

    var rawBatchLimit = config[batchLimitKey];
    var sanitizedBatchLimit = sanitizeBatchLimitValue(rawBatchLimit);
    if (
      rawBatchLimit !== undefined &&
      rawBatchLimit !== null &&
      rawBatchLimit !== '' &&
      sanitizedBatchLimit === null
    ) {
      errors.push({ key: batchLimitKey, message: batchLimitKey + ' doit être un entier positif.' });
    } else {
      sanitized[batchLimitKey] =
        sanitizedBatchLimit !== null ? sanitizedBatchLimit : defaultBatchLimit;
    }

    sanitized[ingestFlagKey] = sanitizeBooleanFlag(config[ingestFlagKey], true);

    var tableCandidate =
      config.bq_table_name !== undefined && config.bq_table_name !== null
        ? String(config.bq_table_name).trim()
        : config.bq_alias !== undefined && config.bq_alias !== null
        ? String(config.bq_alias).trim()
        : '';

    var formIdForTable = sanitized.form_id || (config.form_id ? String(config.form_id).trim() : '');
    var formNameForTable = sanitized.form_name || (config.form_name ? String(config.form_name).trim() : '');

    var computedTableName = '';
    try {
      computedTableName = computeTableName(formIdForTable, formNameForTable, tableCandidate);
    } catch (computeError) {
      console.log('SheetConfigHelpers.validateConfig: échec calcul table -> ' + computeError);
    }

    if (!computedTableName) {
      errors.push({ key: 'bq_table_name', message: 'bq_table_name manquant ou invalide.' });
    } else if (computedTableName.length > maxTableNameLength) {
      errors.push({
        key: 'bq_table_name',
        message: 'bq_table_name doit contenir ' + maxTableNameLength + ' caractères maximum.'
      });
    } else {
      sanitized.bq_table_name = computedTableName;
    }

    return {
      isValid: errors.length === 0,
      config: sanitized,
      errors: errors,
      sheetName: sheet ? sheet.getName() : ''
    };
  }

  function notifyConfigErrors(validation) {
    var lines = validation.errors
      .map(function (error) {
        return '• ' + error.message;
      })
      .join('\n');
    var message = (validation.sheetName ? validation.sheetName + '\n' : '') + lines;

    try {
      var ui = SpreadsheetApp.getUi();
      ui.alert('Configuration invalide', message, ui.ButtonSet.OK);
    } catch (uiError) {
      try {
        var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
        spreadsheet.toast(message, 'Configuration invalide', 10);
      } catch (toastError) {
        console.log('SheetConfigHelpers.notifyConfigErrors: affichage impossible -> ' + toastError);
      }
    }

    console.log('Configuration invalide détectée: ' + message);
  }

  function resolveFormContext(spreadsheet) {
    if (!spreadsheet) {
      return null;
    }
    var sheet = spreadsheet.getActiveSheet();
    if (!sheet) {
      return null;
    }
    var config = readConfigFromSheet(sheet) || {};
    return {
      sheet: sheet,
      config: config,
      batchLimit: getConfiguredBatchLimit(config)
    };
  }

  return {
    readConfigFromSheet: readConfigFromSheet,
    writeConfigToSheet: writeConfigToSheet,
    validateConfig: validateConfig,
    notifyConfigErrors: notifyConfigErrors,
    resolveFormContext: resolveFormContext
  };
}

function sheetConfigNormalizeFormId(formId) {
  return String(formId || '').trim();
}

function sheetConfigFindConfigSheet(spreadsheet, normalizedId) {
  if (!spreadsheet || !normalizedId) {
    return null;
  }
  var suffix = ' || ' + normalizedId;
  var sheets = spreadsheet.getSheets();
  for (var i = 0; i < sheets.length; i++) {
    var name = sheets[i].getName();
    if (typeof name === 'string' && name.indexOf(suffix, Math.max(0, name.length - suffix.length)) !== -1) {
      return sheets[i];
    }
  }
  return null;
}

function sheetConfigReadKeyValueSheet(sheet) {
  if (!sheet) {
    return {};
  }
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return {};
  }
  var values = sheet.getRange(2, 1, lastRow - 1, 2).getValues();
  var config = {};
  for (var index = 0; index < values.length; index++) {
    var row = values[index];
    var key = row && row.length ? row[0] : '';
    if (!key) continue;
    config[String(key).trim()] = row[1];
  }
  return config;
}

function sheetConfigReadStoredConfig(spreadsheet, formId) {
  if (!spreadsheet) {
    return null;
  }
  var normalizedId = sheetConfigNormalizeFormId(formId);
  if (!normalizedId) {
    return null;
  }
  var sheet = sheetConfigFindConfigSheet(spreadsheet, normalizedId);
  if (!sheet) {
    return null;
  }
  var config = sheetConfigReadKeyValueSheet(sheet);
  if (!config || Object.keys(config).length === 0) {
    return null;
  }
  if (!config.form_id) {
    config.form_id = normalizedId;
  }
  if (!config.bq_table_name && config.bq_alias && typeof bqComputeTableName === 'function') {
    try {
      config.bq_table_name = bqComputeTableName(config.form_id, config.form_name || '', config.bq_alias);
    } catch (error) {
      console.log('SheetConfigHelpers.readStoredConfig: calcul table échoué -> ' + error);
    }
  }
  if (config.bq_alias) {
    delete config.bq_alias;
  }
  return config;
}

var SheetConfigHelpers = {
  create: sheetConfigCreateService,
  readStoredConfig: sheetConfigReadStoredConfig
};
