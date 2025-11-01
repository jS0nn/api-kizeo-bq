// pipeline Version 0.1.0

var majPipeline =
  typeof majPipeline !== 'undefined'
    ? majPipeline
    : (function () {
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

        var reportException = resolveSymbol('handleException');
        var ingestProcessData = resolveSymbol('processData');
        var sheetInterfaceHelpers = resolveSymbol('SheetInterfaceHelpers');
        var extractAliasPart = resolveSymbol('bqExtractAliasPart');
        var runDeduplication = resolveSymbol('bqRunDeduplicationForForm');
        var initBigQueryConfig = resolveSymbol('initBigQueryConfig');
        var ensureBigQueryCoreTables = resolveSymbol('ensureBigQueryCoreTables');

        function onOpen() {
          afficheMenu();
          console.log('Fin de onOpen');
          var props = PropertiesService.getDocumentProperties();
          var projectId = props.getProperty('BQ_PROJECT_ID');
          var dataset = props.getProperty('BQ_DATASET');
          var location = props.getProperty('BQ_LOCATION');
          console.log(
            'ScriptProperties BQ -> project=' +
              (projectId || 'NULL') +
              ', dataset=' +
              (dataset || 'NULL') +
              ', location=' +
              (location || 'NULL')
          );
          try {
            majTriggers.getStoredTriggerFrequency();
          } catch (e) {
            uiHandleException('onOpen.triggerFrequency', e);
          }
          ensureDeduplicationTrigger();
        }

        function setScriptProperties(etat) {
          PropertiesService.getScriptProperties().setProperty('etatExecution', etat);
        }

        function getEtatExecution() {
          return PropertiesService.getScriptProperties().getProperty('etatExecution');
        }

        function initBigQueryConfigFromSheet() {
          try {
            var ui = SpreadsheetApp.getUi();
            var defaults = initBigQueryConfig();
            var refreshedProps = PropertiesService.getDocumentProperties();
            refreshedProps.setProperties(
              {
                BQ_PROJECT_ID: defaults.projectId,
                BQ_DATASET: defaults.datasetId,
                BQ_LOCATION: defaults.location || ''
              },
              true
            );
            try {
              ensureBigQueryCoreTables();
            } catch (ensureError) {
              reportException('initBigQueryConfigFromSheet.ensureCore', ensureError);
            }
            var finalProject = refreshedProps.getProperty('BQ_PROJECT_ID');
            var finalDataset = refreshedProps.getProperty('BQ_DATASET');
            var finalLocation = refreshedProps.getProperty('BQ_LOCATION');
            Logger.log(
              'initBigQueryConfigFromSheet -> project=' +
                finalProject +
                ', dataset=' +
                finalDataset +
                ', location=' +
                finalLocation
            );
            ui.alert(
              'Configuration BigQuery initialisée :\nProjet=' +
                finalProject +
                '\nDataset=' +
                finalDataset +
                '\nLocation=' +
                (finalLocation || 'default')
            );
          } catch (e) {
            reportException('initBigQueryConfigFromSheet', e);
          }
        }

        function handleProcessSummary(formulaire, context, summary, runStart, ingestFlag) {
          if (!summary) return;
          var runEnd = Date.now();
          var runDurationSeconds = Math.round((runEnd - runStart) / 1000);
          var lastRecord = summary.latestRecord || null;
          var rowCount = summary.rowCount || 0;
          var ingestionStatus = summary.status || 'UNKNOWN';
          Logger.log(
            'main: ingestion terminée -> status=' +
              ingestionStatus +
              ', rowCount=' +
              rowCount +
              ', latestRecord=' +
              (lastRecord && lastRecord.data_id ? lastRecord.data_id : 'aucun')
          );
          if (sheetInterfaceHelpers && summary.metadataUpdateStatus === 'UPDATED') {
            try {
              sheetInterfaceHelpers.persistExecutionMetadata({
                sheet: context.sheet,
                configService: majConfig.getService(),
                formulaire: formulaire,
                summary: summary,
                runDurationSeconds: runDurationSeconds
              });
            } catch (metaError) {
              reportException('main.persistExecutionMetadata', metaError);
            }
          }
          if (ingestFlag !== 'false' && summary.medias && summary.medias.length && context.config.driveFolderId) {
            try {
              majExports.exportMedias(summary.medias, context.config.driveFolderId);
            } catch (mediaError) {
              reportException('main.exportMedias', mediaError);
            }
          }
          if (summary.pdf && context.config.driveFolderId) {
            try {
              majExports.exportPdfBlob(
                formulaire.nom,
                summary.pdf.dataId,
                summary.pdf.blob,
                context.config.driveFolderId
              );
            } catch (pdfError) {
              reportException('main.exportPdfBlob', pdfError);
            }
          }
          setScriptProperties('termine');
          if (sheetInterfaceHelpers) {
            sheetInterfaceHelpers.notifyExecutionCompleted({
              runDurationSeconds: runDurationSeconds,
              rowCount: rowCount,
              latestRecord: lastRecord
            });
          }
        }

        function main(options) {
          var opts = options || {};
          var origin = opts.origin || 'unknown';
          var skipLockCheck = opts.skipLockCheck === true;
          var spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
          var context = majConfig.resolveFormulaireContext(spreadsheetBdD);
          if (!context) {
            console.log('main: aucun formulaire configuré pour ce classeur.');
            return;
          }
          var validation = majConfig.validateFormConfig(context.config, context.sheet);
          if (!validation.isValid) {
            majConfig.notifyConfigErrors(validation);
            return;
          }
          context.config = Object.assign({}, context.config, validation.config);
          if (!skipLockCheck && getEtatExecution() === 'enCours') {
            var notifyOptions = {};
            if (origin === 'menu') {
              notifyOptions.toastMessage =
                'Une mise à jour est déjà en cours. Relancez depuis le menu lorsqu\'elle sera terminée.';
              notifyOptions.alertMessage =
                'Une mise à jour est déjà en cours. Patientez avant de relancer depuis le menu.';
            } else if (origin === 'ui_dialog') {
              notifyOptions.toastMessage =
                'Une mise à jour est déjà en cours. Patientez avant de relancer depuis la fenêtre de sélection.';
              notifyOptions.showAlert = false;
              notifyOptions.showToast = false;
              notifyOptions.shouldThrow = true;
              notifyOptions.errorMessage = 'EXECUTION_EN_COURS';
            }
            if (sheetInterfaceHelpers) {
              sheetInterfaceHelpers.notifyExecutionAlreadyRunning(notifyOptions);
            }
            console.log('Exécution précédente toujours en cours.');
            console.log("En cas de blocage, réinitialisez l'état manuellement ou exécutez setScriptProperties('termine').");
            return;
          }
          setScriptProperties('enCours');
          var runStart = Date.now();
          try {
            var tableName = validation.config.bq_table_name;
            var aliasPart = tableName;
            try {
              aliasPart = extractAliasPart(tableName, validation.config.form_id);
            } catch (aliasError) {
              console.log("main: impossible d'extraire l'alias -> " + aliasError);
            }
            var formulaire = {
              nom: validation.config.form_name,
              id: validation.config.form_id,
              tableName: tableName,
              alias: aliasPart,
              action: validation.config.action
            };
            var ingestFlag = validation.config[CONFIG_INGEST_BIGQUERY_KEY];
            if (
              !sheetInterfaceHelpers.ensureBigQueryConfigAvailability(
                ingestFlag,
                context.sheet ? context.sheet.getName() : ''
              )
            ) {
              reportException('main.ensureBigQueryConfig', new Error('Configuration BigQuery manquante'));
              setScriptProperties('termine');
              return;
            }
            var services = resolveSymbol('createIngestionServices')({
              logger: console
            });
            var summary = ingestProcessData({
              formulaire: formulaire,
              config: context.config,
              spreadsheet: spreadsheetBdD,
              services: services,
              runOptions: {
                ingestBigQuery: ingestFlag !== 'false',
                batchLimit: majConfig.getConfiguredBatchLimit(context.config),
                origin: origin
              }
            });
            handleProcessSummary(formulaire, context, summary, runStart, ingestFlag);
          } catch (error) {
            reportException('main', error, { origin: origin });
            setScriptProperties('termine');
            if (sheetInterfaceHelpers) {
              sheetInterfaceHelpers.notifyExecutionFailed(error);
            }
          }
        }

        function runBigQueryDeduplication() {
          if (getEtatExecution() === 'enCours') {
            console.log('runBigQueryDeduplication: exécution principale en cours, déduplication reportée.');
            return {
              status: 'SKIPPED',
              reason: 'RUN_IN_PROGRESS',
              message: 'Une mise à jour est déjà en cours.'
            };
          }
          var spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
          var context = majConfig.resolveFormulaireContext(spreadsheetBdD);
          if (!context) {
            console.log('runBigQueryDeduplication: aucun formulaire configuré, déduplication ignorée.');
            return {
              status: 'SKIPPED',
              reason: 'NO_FORM_CONFIG',
              message: 'Aucun formulaire sélectionné.'
            };
          }
          var validation = majConfig.validateFormConfig(context.config, context.sheet);
          if (!validation.isValid) {
            console.log('runBigQueryDeduplication: configuration invalide, déduplication ignorée.');
            return {
              status: 'SKIPPED',
              reason: 'INVALID_CONFIG',
              message: 'Configuration invalide.',
              errors: validation.errors
            };
          }
          var tableName = validation.config.bq_table_name;
          var aliasPart = tableName;
          try {
            aliasPart = extractAliasPart(tableName, validation.config.form_id);
          } catch (aliasError) {
            console.log("runBigQueryDeduplication: impossible d'extraire l'alias -> " + aliasError);
          }
          var formulaire = {
            nom: validation.config.form_name,
            id: validation.config.form_id,
            tableName: tableName,
            alias: aliasPart
          };
          var ingestFlag = validation.config[CONFIG_INGEST_BIGQUERY_KEY];
          if (ingestFlag === 'false') {
            return {
              status: 'SKIPPED',
              reason: 'BIGQUERY_DISABLED',
              message: "L'ingestion BigQuery est désactivée pour ce formulaire."
            };
          }
          if (
            !sheetInterfaceHelpers.ensureBigQueryConfigAvailability(
              ingestFlag,
              context.sheet ? context.sheet.getName() : ''
            )
          ) {
            return {
              status: 'ERROR',
              reason: 'MISSING_BIGQUERY_CONFIG',
              message: 'Configuration BigQuery manquante.'
            };
          }
          Logger.log('runBigQueryDeduplication: lancement pour ' + formulaire.id + ' (' + tableName + ')');
          try {
            var summary = runDeduplication(formulaire);
            if (summary) {
              Logger.log(
                'runBigQueryDeduplication: terminé -> parent supprimé=' +
                  summary.parent.deleted +
                  ', tables filles traitées=' +
                  summary.subTables.length
              );
              return {
                status: 'DONE',
                parent: summary.parent,
                subTables: summary.subTables
              };
            }
            return {
              status: 'DONE',
              parent: null,
              subTables: []
            };
          } catch (e) {
            reportException('runBigQueryDeduplication', e, {
              formId: formulaire.id,
              tableName: tableName
            });
            return {
              status: 'ERROR',
              reason: 'EXCEPTION',
              message: e && e.message ? e.message : String(e)
            };
          }
        }

        function launchManualDeduplication() {
          var ui = SpreadsheetApp.getUi();
          var result;
          try {
            result = runBigQueryDeduplication();
          } catch (e) {
            var message = e && e.message ? e.message : String(e);
            ui.alert('Déduplication BigQuery', 'Erreur inattendue: ' + message, ui.ButtonSet.OK);
            return;
          }
          if (!result) {
            ui.alert('Déduplication BigQuery', 'Aucun résultat retourné par la déduplication.', ui.ButtonSet.OK);
            return;
          }
          if (result.status === 'SKIPPED') {
            var reason = result.reason || 'UNKNOWN';
            var message =
              reason === 'RUN_IN_PROGRESS'
                ? 'Une mise à jour est déjà en cours. Réessayez après sa fin.'
                : reason === 'NO_FORM_CONFIG'
                ? 'Aucun formulaire n’est configuré pour ce classeur. Sélectionnez un formulaire avant de lancer la déduplication.'
                : reason === 'INVALID_CONFIG'
                ? ('Configuration invalide.\n' + ((result.errors && result.errors.length && result.errors[0].message) || '')).trim()
                : result.message || 'Opération ignorée.';
            ui.alert('Déduplication BigQuery', message, ui.ButtonSet.OK);
            return;
          }
          if (result.status === 'ERROR') {
            ui.alert(
              'Déduplication BigQuery',
              'La déduplication a échoué: ' + (result.message || 'Erreur inconnue'),
              ui.ButtonSet.OK
            );
            return;
          }
          var parentInfo = result.parent
            ? 'Entrées dédupliquées: ' + result.parent.deleted
            : 'Aucune entrée supprimée';
          ui.alert(
            'Déduplication BigQuery',
            parentInfo + '\nTables filles traitées: ' + (result.subTables || []).length,
            ui.ButtonSet.OK
          );
        }

        return {
          onOpen: onOpen,
          setScriptProperties: setScriptProperties,
          getEtatExecution: getEtatExecution,
          initBigQueryConfigFromSheet: initBigQueryConfigFromSheet,
          main: main,
          runBigQueryDeduplication: runBigQueryDeduplication,
          launchManualDeduplication: launchManualDeduplication
        };
      })();
