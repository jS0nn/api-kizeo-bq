#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');

function readFile(relativePath) {
  const absolute = path.join(__dirname, '..', relativePath);
  return fs.readFileSync(absolute, 'utf8');
}

function createContext(additionalGlobals = {}) {
  const base = {
    console,
    JSON,
    Date,
    Error,
    Math,
    Number,
    String,
    Boolean,
    RegExp,
    Object,
    Array,
    Set,
    Map,
    parseFloat,
    parseInt,
    isNaN,
    encodeURIComponent,
    decodeURIComponent,
    Utilities: {
      sleep: () => {}
    }
  };
  return vm.createContext(Object.assign(base, additionalGlobals));
}

function runFile(relativePath, context) {
  const code = readFile(relativePath);
  vm.runInContext(code, context, { filename: relativePath });
}

function loadProcessManagerModules(context) {
  runFile('lib/process/manager/logging.js', context);
  runFile('lib/process/manager/preparation.js', context);
  runFile('lib/process/manager/ingestion.js', context);
  runFile('lib/process/manager/finalize.js', context);
  runFile('lib/ProcessManager.js', context);
}

function buildPublicApiContext(overrides = {}) {
  const functionStubs = {
    handleException: function handleException() {},
    requeteAPIDonnees: function requeteAPIDonnees() {},
    processData: function processData() {},
    handleResponses: function handleResponses() {},
    markResponsesAsRead: function markResponsesAsRead() {},
    createIngestionServices: function createIngestionServices() {},
    buildExecutionTargets: function buildExecutionTargets() {},
    resolveBatchLimit: function resolveBatchLimit() {},
    resolveIsoTimestamp: function resolveIsoTimestamp() {},
    resolveUnreadDataset: function resolveUnreadDataset() {},
    collectResponseArtifacts: function collectResponseArtifacts() {},
    ingestBigQueryPayloads: function ingestBigQueryPayloads() {},
    runExternalListsSync: function runExternalListsSync() {},
    initBigQueryConfig: function initBigQueryConfig() {},
    ensureBigQueryCoreTables: function ensureBigQueryCoreTables() {},
    getBigQueryConfig: function getBigQueryConfig() {},
    bqComputeTableName: function bqComputeTableName() {},
    bqExtractAliasPart: function bqExtractAliasPart() {},
    bqRunDeduplicationForForm: function bqRunDeduplicationForForm() {},
    bqParentTableId: function bqParentTableId() {},
    bqBackfillForm: function bqBackfillForm() {},
    gestionFeuilles: function gestionFeuilles() {},
    isNumeric: function isNumeric() {},
    fetchUnreadResponses: function fetchUnreadResponses() {},
    ingestResponsesBatch: function ingestResponsesBatch() {},
    finalizeIngestionRun: function finalizeIngestionRun() {}
  };

  const objectExports = {
    DriveMediaService: { getDefault: () => ({}) },
    ExternalListsService: { updateFromSnapshot: () => {} },
    SheetInterfaceHelpers: { applyConfigLayout: () => {} },
    SheetConfigHelpers: { create: () => {}, readStoredConfig: () => ({}) },
    SheetDriveExports: { exportMedias: () => {} },
    FormResponseSnapshot: { buildRowSnapshot: () => {} }
  };

  return createContext(Object.assign({}, functionStubs, objectExports, overrides));
}

function testFormResponseSnapshotUsesDriveService() {
  const driveService = {
    calls: [],
    processField(formId, dataId, fieldName, fieldValue, spreadsheet, options) {
      this.calls.push({ formId, dataId, fieldName, fieldValue, options });
      return {
        formula: '=HYPERLINK("https://drive.example/file","media")',
        files: [
          {
            mediaId: 'm-1',
            fileName: 'media.png',
            fileId: 'file-1',
            driveUrl: 'https://drive.example/file',
            driveViewUrl: 'https://drive.example/view',
            drivePublicUrl: '',
            folderId: 'folder-1',
            folderUrl: 'https://drive.example/folder'
          }
        ]
      };
    },
    normalizeDrivePublicUrl(url) {
      return `public:${url}`;
    }
  };

  const errors = [];
  const context = createContext({
    DriveMediaService: {
      getDefault: () => driveService
    },
    handleException: (name, error) => {
      errors.push({ name, message: error && error.message ? error.message : String(error) });
    },
    isNumeric: (value) => !Number.isNaN(Number(value)),
    normalizeSubformRows: (value) => value,
    isSubformField: (type) => type === 'subform',
    formSnapshotLegacyLog: () => {}
  });

  runFile('lib/FormResponseSnapshot.js', context);

  const snapshot = context.FormResponseSnapshot.buildRowSnapshot(
    { getId: () => 'SPREAD' },
    { id: 'FORM-1', nom: 'Formulaire Test' },
    {
      id: 'REC-1',
      form_id: 'FORM-1',
      form_unique_id: 'FORM-1::REC-1',
      answer_time: '2024-01-01T10:00:00Z',
      update_time: '2024-01-01T11:00:00Z',
      fields: {
        photo_field: { type: 'photo', value: 'MEDIA-123' },
        notes: { type: 'text', value: 'RAS' }
      }
    },
    []
  );

  assert.deepStrictEqual(errors, [], 'aucune erreur ne doit être remontée');
  assert.strictEqual(driveService.calls.length, 1, 'processField doit être appelé une fois');
  assert.ok(
    snapshot &&
      Array.isArray(snapshot.rowEnCours) &&
      snapshot.rowEnCours.some((value) => typeof value === 'string' && value.includes('HYPERLINK')),
    'le snapshot doit contenir la formule Drive'
  );
}

function testCreateIngestionServicesUsesGlobalFactories() {
  const snapshotStub = {};
  const context = createContext({
    KizeoClient: { requeteAPIDonnees: () => {} },
    FormResponseSnapshot: snapshotStub,
    ExternalSnapshot: snapshotStub,
    bqEnsureDataset: () => {},
    bqEnsureRawTable: () => {},
    bqEnsureParentTable: () => {},
    bqEnsureSubTable: () => {},
    bqEnsureMediaTable: () => {},
    bqEnsureColumns: () => {},
    bqIngestRawKizeoBatch: () => {},
    bqIngestParentBatch: () => {},
    bqIngestSubTablesBatch: () => {},
    bqIngestMediaBatch: () => {},
    bqRecordAudit: () => {},
    bqRunDeduplicationForForm: function runDedup() {},
    getBigQueryConfig: () => ({ projectId: 'p', datasetId: 'd', location: 'europe-west1' })
  });

  runFile('lib/process/utils.js', context);

  const services = context.createIngestionServices();
  assert.strictEqual(services.snapshot, snapshotStub, 'le snapshot par défaut doit être FormResponseSnapshot');
  assert.strictEqual(
    services.bigQuery.runDeduplicationForForm,
    context.bqRunDeduplicationForForm,
    'les fonctions BigQuery doivent être reliées directement'
  );

  const customSnapshot = {};
  const overrideServices = context.createIngestionServices({ snapshot: customSnapshot });
  assert.strictEqual(overrideServices.snapshot, customSnapshot, 'l’override snapshot doit être honoré');
}

function testCollectResponseArtifactsWithoutLegacySheetSync() {
  const driveService = {
    processField: () => ({
      formula: '=HYPERLINK("https://drive","media")',
      files: []
    }),
    normalizeDrivePublicUrl: () => ''
  };

  const context = createContext({
    DriveMediaService: { getDefault: () => driveService },
    handleException: () => {},
    isNumeric: (value) => !Number.isNaN(Number(value)),
    normalizeSubformRows: (value) => value,
    formSnapshotLegacyLog: () => {},
    bqPrepareParentRow: () => ({
      row: { data_id: 'REC-1' },
      columns: [{ name: 'dynamic_col', type: 'STRING', mode: 'NULLABLE' }],
      subforms: []
    })
  });

  runFile('lib/FormResponseSnapshot.js', context);
  runFile('lib/process/utils.js', context);
  runFile('lib/process/collector.js', context);

  const detailedRecord = {
    id: 'REC-1',
    form_id: 'FORM-1',
    answer_time: '2024-01-01T10:00:00Z',
    update_time: '2024-01-01T11:00:00Z',
    fields: {}
  };

  const fetchCalls = [];
  const result = context.collectResponseArtifacts(
    [{ _id: 'REC-1' }],
    {
      fetchFn: (method, path) => {
        fetchCalls.push({ method, path });
        if (path.indexOf('/data/REC-1') !== -1) {
          return { data: { data: detailedRecord }, responseCode: 200 };
        }
        return { data: { status: 'ok', data: [{ _id: 'REC-1' }] }, responseCode: 200 };
      },
      formulaire: { id: 'FORM-1', nom: 'Formulaire Test' },
      apiPath: '/forms/FORM-1/data/unread/ACTION/10?includeupdated',
      log: () => {},
      spreadsheetBdD: { getId: () => 'SPREAD' },
      medias: [],
      snapshotService: context.FormResponseSnapshot,
      bigQuery: {
        prepareParentRow: context.bqPrepareParentRow
      }
    }
  );

  assert.ok(result, 'un résultat doit être renvoyé');
  assert.ok(result.processedDataIds.has('REC-1'), 'l’identifiant traité doit être renvoyé');
  assert.strictEqual(result.lastSnapshot && result.lastSnapshot.rowEnCours.length > 0, true, 'un snapshot doit être présent');
  assert.ok(fetchCalls.some((call) => call.path.indexOf('/data/REC-1') !== -1), 'le détail de la réponse doit être récupéré');
}

function testCollectMediasForRecordUsesSnapshotService() {
  const calls = [];
  const context = createContext({
    FormResponseSnapshot: {
      buildRowSnapshot: (spreadsheet, formulaire, detail, medias) => {
        calls.push({ spreadsheet, formulaire, detail, medias });
      }
    },
    handleException: () => {}
  });

  runFile('lib/backfill.js', context);

  const mediasCollector = [];
  context.collectMediasForRecord(
    { id: 'FORM-1', nom: 'Formulaire test' },
    { id: 'DATA-1' },
    mediasCollector,
    { getName: () => 'Sheet' }
  );

  assert.strictEqual(calls.length, 1, 'buildRowSnapshot doit être invoqué');
  assert.strictEqual(mediasCollector.length, 0, 'collectMediasForRecord ne doit pas modifier le tableau directement');

  delete context.FormResponseSnapshot;
  context.collectMediasForRecord(
    { id: 'FORM-1', nom: 'Formulaire test' },
    { id: 'DATA-2' },
    mediasCollector,
    { getName: () => 'Sheet' }
  );
  assert.strictEqual(calls.length, 1, 'sans snapshot service, aucun appel supplémentaire ne doit être effectué');
}

function testLibPublicSymbolsExports() {
  const context = buildPublicApiContext();

  runFile('lib/0_Data.js', context);

  assert.strictEqual(typeof context.libKizeo, 'undefined', 'aucun alias libKizeo ne doit être créé');
  const dataLogPrefix = vm.runInContext(
    'typeof DATA_LOG_PREFIX === "undefined" ? undefined : DATA_LOG_PREFIX',
    context
  );
  assert.strictEqual(dataLogPrefix, 'lib:Data', 'DATA_LOG_PREFIX doit rester global');
  assert.strictEqual(
    typeof context.getLibPublicSymbols,
    'function',
    'getLibPublicSymbols doit être exposée'
  );

  const symbols = context.getLibPublicSymbols();
  assert.ok(Array.isArray(symbols), 'getLibPublicSymbols doit renvoyer un tableau');
  assert.ok(symbols.length > 0, 'la liste des symboles publics ne doit pas être vide');

  const expectedSymbols = [
    'DATA_LOG_PREFIX',
    'handleException',
    'requeteAPIDonnees',
    'processData',
    'handleResponses',
    'markResponsesAsRead',
    'createIngestionServices',
    'buildExecutionTargets',
    'resolveBatchLimit',
    'resolveIsoTimestamp',
    'resolveUnreadDataset',
    'collectResponseArtifacts',
    'ingestBigQueryPayloads',
    'runExternalListsSync',
    'initBigQueryConfig',
    'ensureBigQueryCoreTables',
    'getBigQueryConfig',
    'bqComputeTableName',
    'bqExtractAliasPart',
    'bqRunDeduplicationForForm',
    'bqParentTableId',
    'bqBackfillForm',
    'DriveMediaService',
    'ExternalListsService',
    'SheetInterfaceHelpers',
    'SheetConfigHelpers',
    'SheetDriveExports',
    'FormResponseSnapshot',
    'gestionFeuilles',
    'isNumeric',
    'fetchUnreadResponses',
    'ingestResponsesBatch',
    'finalizeIngestionRun'
  ];

  assert.strictEqual(
    symbols.length,
    expectedSymbols.length,
    'la liste des symboles publics doit conserver le même nombre d’entrées'
  );
  expectedSymbols.forEach((symbol) => {
    assert.ok(symbols.indexOf(symbol) !== -1, `le symbole ${symbol} doit rester exposé`);
  });
  const unexpected = symbols.filter((symbol) => expectedSymbols.indexOf(symbol) === -1);
  assert.strictEqual(unexpected.length, 0, 'aucun symbole inattendu ne doit être exposé');
}

function testGetLibPublicApiFreezesExports() {
  const context = buildPublicApiContext();

  runFile('lib/0_Data.js', context);
  runFile('lib/zz_PublicApi.js', context);

  assert.strictEqual(
    typeof context.getLibPublicApi,
    'function',
    'getLibPublicApi doit être exposée par la librairie'
  );

  const api = context.getLibPublicApi();
  assert.ok(api, 'getLibPublicApi doit retourner un objet');
  assert.ok(Object.isFrozen(api), 'l’API publique doit être figée');

  const symbols = context.getLibPublicSymbols();
  const apiKeys = Object.keys(api).sort();
  const sortedSymbols = symbols.slice().sort();
  const missing = sortedSymbols.filter((symbol) => apiKeys.indexOf(symbol) === -1);
  const extra = apiKeys.filter((key) => sortedSymbols.indexOf(key) === -1);
  assert.strictEqual(
    missing.length,
    0,
    'tous les symboles documentés doivent être présents dans l’API publique figée'
  );
  assert.strictEqual(
    extra.length,
    0,
    'l’API publique ne doit exposer aucun symbole non déclaré dans LIB_PUBLIC_SYMBOLS'
  );

  const repeatCall = context.getLibPublicApi();
  assert.strictEqual(repeatCall, api, 'getLibPublicApi doit retourner la même instance figée');

  symbols.forEach((symbol) => {
    assert.strictEqual(api[symbol], context[symbol], `l’API doit exposer la référence globale ${symbol}`);
  });
}

function testRunExternalListsSyncWithoutService() {
  const logs = [];
  const context = createContext({
    handleException: () => {},
    requeteAPIDonnees: (method, path) => {
      if (method === 'GET' && path.indexOf('/data/unread/') !== -1) {
        return { data: { data: [] }, responseCode: 200 };
      }
      return { data: { lists: [] }, responseCode: 200 };
    }
  });

  runFile('lib/process/utils.js', context);
  runFile('lib/process/external-lists.js', context);

  const result = context.runExternalListsSync(
    { id: 'FORM-1', nom: 'Formulaire test' },
    { existingHeaders: ['champ'], rowEnCours: ['valeur'] },
    context.requeteAPIDonnees,
    '/forms/FORM-1/data/unread/ACTION/10?includeupdated',
    (message) => logs.push(message)
  );

  assert.strictEqual(result.metadataUpdateStatus, 'SKIPPED', 'sans service, la synchronisation doit être ignorée');
  assert.ok(
    logs.some((entry) => entry.indexOf('ExternalListsService indisponible') !== -1),
    'un message doit être journalisé pour indiquer l’absence de service'
  );
}

function testProcessDataBuildsSummary() {
  const ensureCalls = [];
  const ingestionCalls = [];
  const markCalls = [];
  const context = createContext({
    resolveBatchLimit: (value) => Number(value),
    buildExecutionTargets: (existingConfig, overrideTargets) =>
      Object.assign({ bigQuery: true, externalLists: true }, overrideTargets || {}),
    SheetConfigHelpers: {
      readStoredConfig: () => ({ last_data_id: null })
    },
    createIngestionServices: () => ({
      fetch: () => {},
      now: () => '2024-02-01T00:00:00.000Z',
      logger: { log: () => {} },
      bigQuery: {
        getConfig: () => ({ projectId: 'p', datasetId: 'd', location: 'loc' }),
        ensureDataset: () => ensureCalls.push('dataset'),
        ensureRawTable: () => ensureCalls.push('raw'),
        ensureParentTable: () => ensureCalls.push('parent'),
        ensureSubTable: () => {},
        ensureMediaTable: () => {},
        ensureColumns: () => {},
        ingestRawBatch: () => {},
        ingestParentBatch: () => {},
        ingestSubTablesBatch: () => {},
        ingestMediaBatch: () => {},
        recordAudit: () => {},
        computeTableName: () => 'dataset_form',
        extractAliasPart: () => 'dataset_form_alias',
        parentTableId: () => 'dataset_form_parent',
        prepareParentRow: () => ({ row: { id: 'REC-1' }, columns: [], subforms: [] }),
        prepareSubformRows: () => [],
        prepareMediaRows: () => []
      }
    }),
    resolveLogFunction: (logger) => logger.log.bind(logger),
    resolveIsoTimestamp: (nowProvider) =>
      typeof nowProvider === 'function' ? nowProvider() : '2024-02-01T00:00:00.000Z',
    collectResponseArtifacts: () => ({
      bigQueryContext: { rawRows: [{ id: 'REC-1' }], parentRows: [], parentColumns: {}, subTables: {} },
      processedDataIds: new Set(['REC-1']),
      latestRecord: { id: 'REC-1' },
      lastSnapshot: { fake: true }
    }),
    ingestBigQueryPayloads: () => ingestionCalls.push('pipeline'),
    markResponsesAsRead: () => markCalls.push('mark'),
    runExternalListsSync: () => ({ metadataUpdateStatus: 'OK' }),
    requeteAPIDonnees: () => {},
    handleException: () => {},
    resolveUnreadDataset: () => ({ type: 'OK', payload: { data: [{ _id: 'REC-1' }] } }),
    processGetLogPrefix: () => 'lib:Data'
  });

  loadProcessManagerModules(context);

  const originalMark = context.markResponsesAsRead;
  context.markResponsesAsRead = function () {
    markCalls.push('mark');
    if (typeof originalMark === 'function') {
      return originalMark.apply(this, arguments);
    }
  };

  const formulaire = { id: 'FORM-1', nom: 'Formulaire test' };
  const result = context.processData({}, formulaire, 'ACTION', 5, {});

  assert.deepStrictEqual(
    ensureCalls,
    ['dataset', 'raw', 'parent'],
    'les étapes de préparation BigQuery doivent être exécutées'
  );
  assert.deepStrictEqual(ingestionCalls, ['pipeline'], 'l’ingestion BigQuery doit être déclenchée');
  assert.strictEqual(markCalls.length, 1, 'les réponses doivent être marquées comme lues');
  assert.strictEqual(formulaire.tableName, 'dataset_form_parent', 'le formulaire doit hériter du nom de table parent');
  assert.strictEqual(formulaire.alias, 'dataset_form_alias', "l'alias doit être calculé par le service BigQuery");
  assert.strictEqual(result.status, 'INGESTED', 'le statut de retour doit refléter le succès');
  assert.strictEqual(result.rowCount, 1, 'une ligne doit être comptabilisée');
  assert.strictEqual(result.metadataUpdateStatus, 'OK', 'la synchronisation des listes doit être validée');
  assert.strictEqual(result.runTimestamp, '2024-02-01T00:00:00.000Z', 'le timestamp doit provenir du service');
}

function testProcessDataWithBigQueryDisabled() {
  const ensureCalls = [];
  const ingestionCalls = [];
  const externalCalls = [];
  const markCalls = [];
  const context = createContext({
    resolveBatchLimit: (value) => Number(value),
    buildExecutionTargets: (existingConfig, overrideTargets) =>
      Object.assign({ bigQuery: true, externalLists: true }, overrideTargets || {}),
    SheetConfigHelpers: {
      readStoredConfig: () => ({ last_data_id: null })
    },
    createIngestionServices: () => ({
      fetch: () => {},
      now: () => '2024-03-01T00:00:00.000Z',
      logger: { log: () => {} },
      bigQuery: {
        getConfig: () => ({ projectId: 'p', datasetId: 'd', location: 'loc' }),
        ensureDataset: () => ensureCalls.push('dataset'),
        ensureRawTable: () => ensureCalls.push('raw'),
        ensureParentTable: () => ensureCalls.push('parent'),
        ensureSubTable: () => {},
        ensureMediaTable: () => {},
        ensureColumns: () => {},
        ingestRawBatch: () => {},
        ingestParentBatch: () => {},
        ingestSubTablesBatch: () => {},
        ingestMediaBatch: () => {},
        recordAudit: () => {},
        computeTableName: () => 'dataset_form',
        extractAliasPart: () => 'dataset_form_alias',
        parentTableId: () => 'dataset_form_parent',
        prepareParentRow: () => ({ row: { id: 'REC-2' }, columns: [], subforms: [] }),
        prepareSubformRows: () => [],
        prepareMediaRows: () => []
      }
    }),
    resolveLogFunction: (logger) => logger.log.bind(logger),
    resolveIsoTimestamp: (nowProvider) =>
      typeof nowProvider === 'function' ? nowProvider() : '2024-03-01T00:00:00.000Z',
    collectResponseArtifacts: () => ({
      bigQueryContext: { rawRows: [{ id: 'REC-2' }], parentRows: [], parentColumns: {}, subTables: {} },
      processedDataIds: new Set(['REC-2']),
      latestRecord: { id: 'REC-2' },
      lastSnapshot: { fake: true }
    }),
    ingestBigQueryPayloads: () => ingestionCalls.push('pipeline'),
    markResponsesAsRead: () => markCalls.push('mark'),
    runExternalListsSync: () => {
      externalCalls.push('external');
      return { metadataUpdateStatus: 'OK' };
    },
    requeteAPIDonnees: () => {},
    handleException: () => {},
    resolveUnreadDataset: () => ({ type: 'OK', payload: { data: [{ _id: 'REC-2' }] } }),
    processGetLogPrefix: () => 'lib:Data'
  });

  loadProcessManagerModules(context);

  const originalMark = context.markResponsesAsRead;
  context.markResponsesAsRead = function () {
    markCalls.push('mark');
    if (typeof originalMark === 'function') {
      return originalMark.apply(this, arguments);
    }
  };

  const formulaire = { id: 'FORM-2', nom: 'Formulaire test 2' };
  const result = context.processData({}, formulaire, 'ACTION', 5, {
    targets: { bigQuery: false, externalLists: false }
  });

  assert.deepStrictEqual(ensureCalls, [], 'aucune préparation BigQuery ne doit avoir lieu');
  assert.deepStrictEqual(ingestionCalls, [], 'l’ingestion BigQuery doit être court-circuitée');
  assert.strictEqual(markCalls.length, 1, 'les réponses doivent toujours être marquées comme lues');
  assert.strictEqual(externalCalls.length, 0, 'la synchronisation des listes doit être ignorée');
  assert.strictEqual(result.metadataUpdateStatus, 'SKIPPED', 'le statut liste doit refléter la désactivation');
  assert.strictEqual(result.status, 'INGESTED', 'le traitement doit rester positif');
  assert.strictEqual(result.rowCount, 1, 'la taille du lot doit être conservée');
}

function testSheetInterfaceHelpersApplyConfigLayout() {
  const actions = {
    headerValues: null,
    filterCreated: false,
    validations: []
  };
  const rowCount = 3;

  function createRangeStub(type, row) {
    return {
      setValues: function (values) {
        if (type === 'header') {
          actions.headerValues = values;
        }
        return this;
      },
      getValues: function () {
        if (type === 'header') {
          return [['', '']];
        }
        return [[null]];
      },
      setFontWeight: function () {
        return this;
      },
      setFontColor: function () {
        return this;
      },
      setWrap: function () {
        return this;
      },
      setHorizontalAlignment: function () {
        return this;
      },
      setVerticalAlignment: function () {
        return this;
      },
      setDataValidation: function () {
        actions.validations.push({ row: row });
        return this;
      },
      setNumberFormat: function () {
        return this;
      },
      setNote: function () {
        return this;
      }
    };
  }

  const tableRangeStub = {
    applyRowBanding: function () {
      return {
        setHeaderRowColor: function () {
          return this;
        },
        setFirstRowColor: function () {
          return this;
        },
        setSecondRowColor: function () {
          return this;
        }
      };
    },
    setBorder: function () {
      return this;
    },
    createFilter: function () {
      actions.filterCreated = true;
      return this;
    }
  };

  const sheet = {
    getSheetId: function () {
      return 1;
    },
    setFrozenRows: function () {
      return this;
    },
    setFrozenColumns: function () {
      return this;
    },
    setColumnWidth: function () {
      return this;
    },
    getBandings: function () {
      return [];
    },
    getFilter: function () {
      return null;
    },
    getRange: function (row, column, numRows, numCols) {
      if (row === 1 && column === 1 && numRows === 1 && numCols === 2) {
        return createRangeStub('header', row);
      }
      if (row === 1 && column === 1 && numRows === rowCount + 1 && numCols === 2) {
        return tableRangeStub;
      }
      return createRangeStub('data', row);
    }
  };

  function createValidationBuilder() {
    return {
      requireNumberGreaterThan: function () {
        return this;
      },
      requireValueInList: function () {
        return this;
      },
      requireTextMatchesPattern: function () {
        return this;
      },
      setAllowInvalid: function () {
        return this;
      },
      setHelpText: function () {
        return this;
      },
      setNumberFormat: function () {
        return this;
      },
      build: function () {
        return {};
      }
    };
  }

  const context = createContext({
    SpreadsheetApp: {
      BandingTheme: { LIGHT_GREY: 'LIGHT_GREY' },
      BorderStyle: { SOLID: 'SOLID' },
      newDataValidation: function () {
        return createValidationBuilder();
      }
    }
  });

  runFile('lib/SheetInterfaceHelpers.js', context);

  context.SheetInterfaceHelpers.applyConfigLayout(sheet, rowCount, {
    headerLabels: ['Colonne', 'Valeur'],
    rowIndexMap: {
      batch_limit: 0,
      ingest_bigquery: 1,
      trigger_frequency: 2
    },
    triggerOptions: { H1: {} },
    batchLimitKey: 'batch_limit',
    ingestFlagKey: 'ingest_bigquery'
  });

  assert.ok(Array.isArray(actions.headerValues), 'les entêtes doivent être définies');
  assert.strictEqual(actions.headerValues[0][0], 'Colonne', 'la première colonne doit être renommée');
  assert.strictEqual(actions.headerValues[0][1], 'Valeur', 'la seconde colonne doit être renommée');
  assert.strictEqual(actions.filterCreated, true, 'un filtre doit être créé');
  const validationRows = actions.validations.map((entry) => entry.row).sort();
  assert.deepStrictEqual(validationRows, [2, 3, 4], 'les validations doivent couvrir batch, ingest et trigger');
}

function testSheetInterfaceHelpersNotifyExecutionAlreadyRunning() {
  const toastCalls = [];
  const context = createContext({
    SpreadsheetApp: {
      BandingTheme: { LIGHT_GREY: 'LIGHT_GREY' },
      BorderStyle: { SOLID: 'SOLID' },
      newDataValidation: function () {
        return {
          requireNumberGreaterThan: function () {
            return this;
          },
          requireValueInList: function () {
            return this;
          },
          setAllowInvalid: function () {
            return this;
          },
          setHelpText: function () {
            return this;
          },
          build: function () {
            return {};
          }
        };
      },
      getActiveSpreadsheet: function () {
        return {
          toast: function (message, title, duration) {
            toastCalls.push({ message, title, duration });
          }
        };
      }
    }
  });

  runFile('lib/SheetInterfaceHelpers.js', context);

  context.SheetInterfaceHelpers.notifyExecutionAlreadyRunning({ showAlert: false, toastSeconds: 5 });

  assert.strictEqual(toastCalls.length, 1, 'un toast doit être émis');
  assert.strictEqual(toastCalls[0].duration, 5, 'la durée du toast doit respecter la configuration');
}

function testSheetInterfaceHelpersEnsureBigQueryConfigAvailability() {
  let handledContext = null;

  const context = createContext({
    SpreadsheetApp: {
      getUi: function () {
        return {
          alert: function () {
            /* ignore */
          }
        };
      },
      getActiveSpreadsheet: function () {
        return {
          toast: function () {
            /* ignore */
          }
        };
      },
      BandingTheme: { LIGHT_GREY: 'LIGHT_GREY' },
      BorderStyle: { SOLID: 'SOLID' },
      newDataValidation: function () {
        return {
          requireNumberGreaterThan: function () {
            return this;
          },
          requireValueInList: function () {
            return this;
          },
          setAllowInvalid: function () {
            return this;
          },
          setHelpText: function () {
            return this;
          },
          build: function () {
            return {};
          }
        };
      }
    },
    getBigQueryConfig: function () {
      const error = new Error('Missing BQ');
      error.missingKeys = ['BQ_PROJECT_ID'];
      throw error;
    },
    handleException: function (name, error, contextInfo) {
      handledContext = contextInfo;
    }
  });

  runFile('lib/SheetInterfaceHelpers.js', context);

  const result = context.SheetInterfaceHelpers.ensureBigQueryConfigAvailability('true', 'Config');
  assert.strictEqual(result, false, 'la fonction doit indiquer une configuration manquante');
  assert.ok(handledContext && handledContext.sheet === 'Config', 'le contexte doit inclure le nom de la feuille');
}

function testFetchUnreadResponsesStates() {
  const context = createContext({
    resolveUnreadDataset: function () {
      return { type: 'NO_UNREAD' };
    }
  });

  loadProcessManagerModules(context);

  const resultNoData = context.fetchUnreadResponses(() => {}, { id: 'FORM' }, '/api', true, null, () => {});
  assert.strictEqual(resultNoData.state, 'NO_DATA', 'NO_UNREAD doit être converti en NO_DATA');

  context.resolveUnreadDataset = function () {
    return { type: 'FALLBACK_EMPTY' };
  };
  const resultFallback = context.fetchUnreadResponses(() => {}, { id: 'FORM' }, '/api', false, null, () => {});
  assert.strictEqual(resultFallback.status, 'FALLBACK_EMPTY', 'FALLBACK_EMPTY doit être renvoyé tel quel');

  context.resolveUnreadDataset = function (_, __, ___, ____, payload) {
    return { type: 'OK', payload: payload };
  };
  const okPayload = { data: [{ _id: 'REC-1' }] };
  const resultOk = context.fetchUnreadResponses(() => {}, { id: 'FORM' }, '/api', false, okPayload, () => {});
  assert.strictEqual(resultOk.state, 'OK', 'Un type OK doit renvoyer state OK');
  assert.deepStrictEqual(resultOk.payload, okPayload, 'Le payload doit être renvoyé sans modification');
}

function testIngestResponsesBatchSkipsBigQuery() {
  const ingestionCalls = [];
  const artifacts = {
    bigQueryContext: { rawRows: [{ id: 'REC-1' }], parentRows: [], parentColumns: {}, subTables: {} },
    processedDataIds: new Set(['REC-1']),
    latestRecord: { id: 'REC-1' },
    lastSnapshot: { fake: true }
  };

  const context = createContext({
    collectResponseArtifacts: () => artifacts,
    ingestBigQueryPayloads: () => ingestionCalls.push('ingest'),
    processGetLogPrefix: () => 'lib:Data'
  });

  loadProcessManagerModules(context);

  const services = { fetch: () => {} };
  const result = context.ingestResponsesBatch(
    { id: 'FORM' },
    { data: [{ _id: 'REC-1' }] },
    services,
    [],
    () => {},
    { bigQuery: false },
    '/api',
    'ACTION'
  );

  assert.deepStrictEqual(result, artifacts, 'Le batch doit renvoyer les artefacts collectés');
  assert.strictEqual(ingestionCalls.length, 0, 'L ingestion ne doit pas être déclenchée quand bigQuery=false');
}

function testHandleResponsesPropagatesSnapshot() {
  const snapshot = { foo: 'bar' };
  const finalizeSnapshots = [];
  const services = { fetch: () => {}, logger: { log: () => {} }, now: () => new Date('2024-01-01T00:00:00Z') };

  const context = createContext({
    processReportException: () => {},
    resolveIsoTimestamp: () => '2024-01-01T00:00:00Z',
    createIngestionServices: () => services,
    resolveLogFunction: (logger) =>
      logger && typeof logger.log === 'function' ? logger.log.bind(logger) : () => {}
  });

  loadProcessManagerModules(context);

  context.fetchUnreadResponses = function () {
    return { state: 'OK', payload: { data: [] } };
  };

  context.ingestResponsesBatch = function () {
    return {
      bigQueryContext: { rawRows: [] },
      processedDataIds: new Set(),
      latestRecord: null,
      lastSnapshot: snapshot
    };
  };

  context.finalizeIngestionRun = function (
    formulaire,
    processingResult,
    innerServices,
    action,
    targets,
    apiPath,
    log,
    runTimestamp
  ) {
    finalizeSnapshots.push(processingResult.lastSnapshot);
    return Object.assign(
      {
        status: 'INGESTED',
        rowCount: processingResult.bigQueryContext.rawRows.length,
        metadataUpdateStatus: 'OK',
        runTimestamp: runTimestamp
      },
      { lastSnapshot: processingResult.lastSnapshot }
    );
  };

  const result = context.handleResponses(
    {},
    { id: 'FORM', nom: 'Formulaire' },
    '/forms/FORM/data',
    'ACTION',
    [],
    false,
    { services: services, targets: { bigQuery: false, externalLists: false } }
  );

  assert.strictEqual(finalizeSnapshots.length, 1, 'finalizeIngestionRun doit être invoqué');
  assert.strictEqual(finalizeSnapshots[0], snapshot, 'Le snapshot doit être transmis à finalizeIngestionRun');
  assert.strictEqual(result.lastSnapshot, snapshot, 'Le résultat doit contenir le snapshot final');
}

function testFinalizeIngestionRun() {
  const markCalls = [];
  const externalCalls = [];
  const context = createContext({ processGetLogPrefix: () => 'lib:Data' });

  loadProcessManagerModules(context);

  context.markResponsesAsRead = function (formulaire, action, ids, fetchFn) {
    markCalls.push({ formulaire, action, ids });
    if (typeof fetchFn === 'function') {
      fetchFn();
    }
  };
  context.runExternalListsSync = function () {
    externalCalls.push('external');
    return { metadataUpdateStatus: 'OK' };
  };

  const services = { fetch: () => {} };
  const processingResult = {
    bigQueryContext: { rawRows: [{ id: 'REC-1' }] },
    processedDataIds: new Set(['REC-1']),
    latestRecord: { id: 'REC-1' },
    lastSnapshot: { fake: true }
  };

  const result = context.finalizeIngestionRun(
    { id: 'FORM' },
    processingResult,
    services,
    'ACTION',
    { externalLists: true },
    '/api',
    () => {},
    '2024-01-01T00:00:00Z'
  );

  assert.strictEqual(markCalls.length, 1, 'Les réponses doivent être marquées comme lues');
  assert.strictEqual(externalCalls.length, 1, 'La synchronisation des listes doit être déclenchée');
  assert.strictEqual(result.status, 'INGESTED', 'Le statut de finalisation doit être INGESTED');
  assert.strictEqual(result.metadataUpdateStatus, 'OK', 'Le statut de métadonnées doit refléter la synchronisation');
}

function testSheetDriveExportsBuildDisplayName() {
  const context = createContext({
    DriveMediaService: {
      getDefault: () => ({ saveBlobToFolder: () => {} })
    },
    DriveApp: {
      getFolderById: () => ({
        getFoldersByName: () => {
          return {
            hasNext: () => false,
            next: () => ({ getId: () => 'child' })
          };
        },
        createFolder: () => ({ getId: () => 'child' })
      }),
      getFileById: () => ({ makeCopy: () => {} })
    }
  });

  runFile('lib/SheetDriveExports.js', context);

  const name = context.SheetDriveExports.buildMediaDisplayName({
    name: 'media',
    driveFileId: '1A2B-3C'
  });
  assert.strictEqual(name, 'media__1A2B-3C', 'L ID doit être suffixé au nom du média');

  const fallback = context.SheetDriveExports.buildMediaDisplayName({ fileName: 'fallback' });
  assert.strictEqual(fallback, 'fallback', 'Sans ID Drive, le nom doit être inchangé');
}

const tests = [
  { name: 'FormResponseSnapshot utilise DriveMediaService', fn: testFormResponseSnapshotUsesDriveService },
  { name: 'createIngestionServices expose les services globaux', fn: testCreateIngestionServicesUsesGlobalFactories },
  { name: 'collectResponseArtifacts fonctionne sans legacy Sheets', fn: testCollectResponseArtifactsWithoutLegacySheetSync },
  { name: 'collectMediasForRecord exploite le snapshot', fn: testCollectMediasForRecordUsesSnapshotService },
  { name: 'getLibPublicSymbols documente les exports globaux', fn: testLibPublicSymbolsExports },
  { name: 'getLibPublicApi fige l’API exposée', fn: testGetLibPublicApiFreezesExports },
  { name: 'runExternalListsSync ignore l’absence de service', fn: testRunExternalListsSyncWithoutService },
  { name: 'processData construit un résumé cohérent', fn: testProcessDataBuildsSummary },
  { name: 'processData respecte les cibles d’exécution désactivées', fn: testProcessDataWithBigQueryDisabled },
  { name: 'SheetInterfaceHelpers met en forme la feuille de configuration', fn: testSheetInterfaceHelpersApplyConfigLayout },
  { name: 'SheetInterfaceHelpers notifie une exécution en cours', fn: testSheetInterfaceHelpersNotifyExecutionAlreadyRunning },
  { name: 'SheetInterfaceHelpers signale une configuration BigQuery manquante', fn: testSheetInterfaceHelpersEnsureBigQueryConfigAvailability },
  { name: 'fetchUnreadResponses gère les différents états', fn: testFetchUnreadResponsesStates },
  { name: 'ingestResponsesBatch respecte les cibles BigQuery', fn: testIngestResponsesBatchSkipsBigQuery },
  { name: 'handleResponses propage le snapshot collecté', fn: testHandleResponsesPropagatesSnapshot },
  { name: 'finalizeIngestionRun marque les réponses et synchronise les listes', fn: testFinalizeIngestionRun },
  { name: 'SheetDriveExports construit un nom de média stable', fn: testSheetDriveExportsBuildDisplayName }
];

let failures = 0;

tests.forEach(({ name, fn }) => {
  try {
    fn();
    console.log(`✔ ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`✖ ${name}`);
    console.error(error);
  }
});

if (failures > 0) {
  console.error(`Tests échoués: ${failures}`);
  process.exit(1);
} else {
  console.log('Tous les tests sont passés.');
}
