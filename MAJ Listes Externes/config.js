// config Version 0.1.0

var majConfig =
  typeof majConfig !== 'undefined'
    ? majConfig
    : (function () {
        var DEFAULT_KIZEO_BATCH_LIMIT = 30;
        var CONFIG_BATCH_LIMIT_KEY = 'batch_limit';
        var CONFIG_INGEST_BIGQUERY_KEY = 'ingest_bigquery';
        var CONFIG_HEADERS = [
          'form_id',
          'form_name',
          'bq_table_name',
          'action',
          CONFIG_BATCH_LIMIT_KEY,
          CONFIG_INGEST_BIGQUERY_KEY,
          'last_data_id',
          'last_update_time',
          'last_answer_time',
          'last_run_at',
          'last_saved_row_count',
          'last_run_duration_s',
          'trigger_frequency'
        ];
        var REQUIRED_CONFIG_KEYS = ['form_id', 'form_name', 'action'];
        var MAX_BQ_TABLE_NAME_LENGTH = 128;

        function resolveSymbol(symbolName) {
          if (typeof requireMajSymbol === 'function') {
            return requireMajSymbol(symbolName);
          }
          if (typeof majBootstrap !== 'undefined' && majBootstrap) {
            if (typeof majBootstrap.require === 'function') {
              return majBootstrap.require(symbolName);
            }
            if (typeof majBootstrap.requireMany === 'function') {
              var resolved = majBootstrap.requireMany([symbolName]);
              if (resolved && Object.prototype.hasOwnProperty.call(resolved, symbolName)) {
                return resolved[symbolName];
              }
            }
          }
          if (typeof libKizeo === 'undefined' || libKizeo === null) {
            throw new Error('libKizeo indisponible (accès ' + symbolName + ')');
          }
          var value = libKizeo[symbolName];
          if (value === undefined || value === null) {
            throw new Error('libKizeo.' + symbolName + ' indisponible');
          }
          return value;
        }

        var sheetInterfaceHelpers = resolveSymbol('SheetInterfaceHelpers');
        var sheetConfigHelpers = resolveSymbol('SheetConfigHelpers');

        function sanitizeBatchLimitValue(raw) {
          if (raw === null || raw === undefined) return null;
          var numeric = Number(raw);
          if (!Number.isFinite(numeric)) return null;
          var floored = Math.floor(numeric);
          if (floored <= 0) return null;
          return floored;
        }

        function getConfiguredBatchLimit(config) {
          var raw =
            config && typeof config === 'object' && CONFIG_BATCH_LIMIT_KEY in config
              ? config[CONFIG_BATCH_LIMIT_KEY]
              : null;
          var sanitized = sanitizeBatchLimitValue(raw);
          if (sanitized !== null) return sanitized;
          return DEFAULT_KIZEO_BATCH_LIMIT;
        }

        function normalizeBooleanConfigValue(raw, defaultValue) {
          if (raw === null || raw === undefined || raw === '') {
            return !!defaultValue;
          }
          if (typeof raw === 'boolean') {
            return raw;
          }
          var normalized = raw.toString().trim().toLowerCase();
          if (!normalized) {
            return !!defaultValue;
          }
          if (['true', '1', 'yes', 'y', 'oui'].indexOf(normalized) !== -1) {
            return true;
          }
          if (['false', '0', 'no', 'n', 'non'].indexOf(normalized) !== -1) {
            return false;
          }
          return !!defaultValue;
        }

        function sanitizeBooleanConfigFlag(raw, defaultValue) {
          return normalizeBooleanConfigValue(raw, defaultValue) ? 'true' : 'false';
        }

        var SHEET_CONFIG_SERVICE = sheetConfigHelpers.create({
          requiredKeys: REQUIRED_CONFIG_KEYS,
          configHeaders: CONFIG_HEADERS,
          batchLimitKey: CONFIG_BATCH_LIMIT_KEY,
          ingestFlagKey: CONFIG_INGEST_BIGQUERY_KEY,
          defaultBatchLimit: DEFAULT_KIZEO_BATCH_LIMIT,
          sanitizeBatchLimitValue: sanitizeBatchLimitValue,
          sanitizeBooleanFlag: sanitizeBooleanConfigFlag,
          getConfiguredBatchLimit: getConfiguredBatchLimit,
          computeTableName: function (formId, formName, candidate) {
            var computeTableNameFn = resolveSymbol('bqComputeTableName');
            return computeTableNameFn(formId, formName, candidate);
          },
          maxTableNameLength: MAX_BQ_TABLE_NAME_LENGTH,
          applyLayout: function (sheet, rowCount, rowIndexMap) {
            if (sheetInterfaceHelpers && typeof sheetInterfaceHelpers.applyConfigLayout === 'function') {
              sheetInterfaceHelpers.applyConfigLayout(sheet, rowCount, {
                headerLabels: ['Paramètre', 'Valeur'],
                rowIndexMap: rowIndexMap,
                triggerOptions:
                  typeof majTriggers !== 'undefined' && majTriggers.getTriggerOptions
                    ? majTriggers.getTriggerOptions()
                    : {},
                batchLimitKey: CONFIG_BATCH_LIMIT_KEY,
                ingestFlagKey: CONFIG_INGEST_BIGQUERY_KEY
              });
            }
          }
        });

        function readFormConfigFromSheet(sheet) {
          return SHEET_CONFIG_SERVICE.readConfigFromSheet(sheet);
        }

        function writeFormConfigToSheet(sheet, config) {
          SHEET_CONFIG_SERVICE.writeConfigToSheet(sheet, config);
        }

        function resolveFormulaireContext(spreadsheetBdD) {
          return SHEET_CONFIG_SERVICE.resolveFormContext(spreadsheetBdD);
        }

        function validateFormConfig(rawConfig, sheet) {
          return SHEET_CONFIG_SERVICE.validateConfig(rawConfig, sheet);
        }

        function notifyConfigErrors(validation) {
          SHEET_CONFIG_SERVICE.notifyConfigErrors(validation);
        }

        function createActionCode() {
          var timestamp = new Date().getTime().toString(36);
          var randomSegment = Utilities.getUuid().replace(/-/g, '').substring(0, 12);
          return ('act_' + timestamp + randomSegment).substring(0, 30);
        }

        return {
          DEFAULT_KIZEO_BATCH_LIMIT: DEFAULT_KIZEO_BATCH_LIMIT,
          CONFIG_BATCH_LIMIT_KEY: CONFIG_BATCH_LIMIT_KEY,
          CONFIG_INGEST_BIGQUERY_KEY: CONFIG_INGEST_BIGQUERY_KEY,
          CONFIG_HEADERS: CONFIG_HEADERS,
          REQUIRED_CONFIG_KEYS: REQUIRED_CONFIG_KEYS,
          sanitizeBatchLimitValue: sanitizeBatchLimitValue,
          getConfiguredBatchLimit: getConfiguredBatchLimit,
          sanitizeBooleanConfigFlag: sanitizeBooleanConfigFlag,
          readFormConfigFromSheet: readFormConfigFromSheet,
          writeFormConfigToSheet: writeFormConfigToSheet,
          resolveFormulaireContext: resolveFormulaireContext,
          validateFormConfig: validateFormConfig,
          notifyConfigErrors: notifyConfigErrors,
          createActionCode: createActionCode,
          getService: function () {
            return SHEET_CONFIG_SERVICE;
          }
        };
      })();

var DEFAULT_KIZEO_BATCH_LIMIT = majConfig.DEFAULT_KIZEO_BATCH_LIMIT;
var CONFIG_BATCH_LIMIT_KEY = majConfig.CONFIG_BATCH_LIMIT_KEY;
var CONFIG_INGEST_BIGQUERY_KEY = majConfig.CONFIG_INGEST_BIGQUERY_KEY;
var CONFIG_HEADERS = majConfig.CONFIG_HEADERS;
var REQUIRED_CONFIG_KEYS = majConfig.REQUIRED_CONFIG_KEYS;
