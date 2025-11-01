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
      snapshotService: context.FormResponseSnapshot
    }
  );

  assert.ok(result, 'un résultat doit être renvoyé');
  assert.ok(result.processedDataIds.has('REC-1'), 'l’identifiant traité doit être renvoyé');
  assert.strictEqual(result.lastSnapshot && result.lastSnapshot.rowEnCours.length > 0, true, 'un snapshot doit être présent');
  assert.ok(fetchCalls.some((call) => call.path.indexOf('/data/REC-1') !== -1), 'le détail de la réponse doit être récupéré');
}

const tests = [
  { name: 'FormResponseSnapshot utilise DriveMediaService', fn: testFormResponseSnapshotUsesDriveService },
  { name: 'createIngestionServices expose les services globaux', fn: testCreateIngestionServicesUsesGlobalFactories },
  { name: 'collectResponseArtifacts fonctionne sans legacy Sheets', fn: testCollectResponseArtifactsWithoutLegacySheetSync }
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
