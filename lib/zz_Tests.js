/**
 * Main du fichier utilisant la bibliotheque id : 15F8a-5rU-4plaAPJuhyYC8-ndbEbWtqHw8RX94_C7cc
 * Met à jour les données pour chaque onglet de feuille.
 * Si de nouvelles réponses sont trouvées pour le formulaire correspondant à un onglet, les données sont enregistrées.
 * Sinon, un message de log est affiché.
 */

function runAllTests() {
  return TestSuite.run();
}

const __testsGlobalScope = (function () {
  return this;
})();

const TestSuite = (function () {
  const tests = [];

  function addTest(name, fn) {
    tests.push({ name, fn });
  }

  function createAssert(testName) {
    function formatMessage(message) {
      return message ? `${testName}: ${message}` : testName;
    }

    function fail(message) {
      throw new Error(formatMessage(message || 'Assertion échouée'));
    }

    function equals(actual, expected, message) {
      if (actual !== expected) {
        fail(`${message || 'Valeur inattendue'} -> attendu=${expected}, obtenu=${actual}`);
      }
    }

    function deepEquals(actual, expected, message) {
      const actualString = JSON.stringify(actual);
      const expectedString = JSON.stringify(expected);
      if (actualString !== expectedString) {
        fail(
          `${message || 'Objets différents'} -> attendu=${expectedString}, obtenu=${actualString}`
        );
      }
    }

    function truthy(value, message) {
      if (!value) {
        fail(message || 'Valeur non truthy');
      }
    }

    function falsy(value, message) {
      if (value) {
        fail(message || 'Valeur non falsy');
      }
    }

    return {
      equals,
      deepEquals,
      truthy,
      falsy,
      fail
    };
  }

  function withMockedGlobals(overrides, callback) {
    const previous = {};
    const scope = __testsGlobalScope;
    Object.keys(overrides).forEach((key) => {
      previous[key] = Object.prototype.hasOwnProperty.call(scope, key) ? scope[key] : undefined;
      scope[key] = overrides[key];
    });
    try {
      return callback();
    } finally {
      Object.keys(overrides).forEach((key) => {
        if (previous[key] === undefined) {
          delete scope[key];
        } else {
          scope[key] = previous[key];
        }
      });
    }
  }

  function run() {
    const results = [];
    tests.forEach((test) => {
      const assert = createAssert(test.name);
      const startedAt = Date.now();
      try {
        const maybePromise = test.fn(assert, withMockedGlobals);
        if (maybePromise && typeof maybePromise.then === 'function') {
          throw new Error('Tests asynchrones non supportés dans ce runner.');
        }
        results.push({
          name: test.name,
          status: 'PASS',
          durationMs: Date.now() - startedAt
        });
      } catch (error) {
        results.push({
          name: test.name,
          status: 'FAIL',
          durationMs: Date.now() - startedAt,
          error: error && error.stack ? String(error.stack) : String(error)
        });
      }
    });

    const summary = {
      total: results.length,
      passed: results.filter((item) => item.status === 'PASS').length,
      failed: results.filter((item) => item.status === 'FAIL').length,
      results
    };
    Logger.log(`Tests exécutés: ${summary.passed}/${summary.total} réussis.`);
    if (summary.failed) {
      results
        .filter((item) => item.status === 'FAIL')
        .forEach((failure, index) => {
          Logger.log(
            `Échec ${index + 1}/${summary.failed} -> ${failure.name} (${failure.durationMs} ms): ${failure.error}`
          );
        });
    } else {
      Logger.log('Tous les tests sont passés avec succès.');
    }
    return summary;
  }

  return {
    addTest,
    run,
    withMockedGlobals
  };
})();

function reduireJSON2(jsonObj, nbMaxTab) {
  function reduire(obj) {
    if (Array.isArray(obj)) {
      return obj.slice(0, nbMaxTab).map(reduire);
    }
    if (typeof obj === 'object' && obj !== null) {
      const newObj = {};
      Object.keys(obj).forEach((key) => {
        newObj[key] = reduire(obj[key]);
      });
      return newObj;
    }
    return obj;
  }
  return reduire(jsonObj);
}

function reduireJSON(jsonObj, limites) {
  function reduire(obj, key) {
    if (Array.isArray(obj)) {
      return obj.slice(0, limites.nbMaxTab).map((item) => reduire(item, null));
    }
    if (typeof obj === 'object' && obj !== null) {
      const newObj = {};
      let keys = Object.keys(obj);
      if (key && limites.listeObjAReduire && Object.prototype.hasOwnProperty.call(limites.listeObjAReduire, key)) {
        const maxProps = limites.listeObjAReduire[key];
        keys = keys.slice(0, maxProps);
      }
      keys.forEach((k) => {
        newObj[k] = reduire(obj[k], k);
      });
      return newObj;
    }
    return obj;
  }
  return reduire(jsonObj, null);
}

function createMockPropertiesStore(initialData) {
  const data = Object.assign({}, initialData || {});
  return {
    getProperty: (key) => (Object.prototype.hasOwnProperty.call(data, key) ? data[key] : null),
    setProperty: (key, value) => {
      data[key] = value;
    },
    deleteProperty: (key) => {
      delete data[key];
    },
    setProperties: (properties, deleteOthers) => {
      if (deleteOthers) {
        Object.keys(data).forEach((key) => delete data[key]);
      }
      Object.keys(properties || {}).forEach((key) => {
        data[key] = properties[key];
      });
    },
    getProperties: () => Object.assign({}, data),
    getAll: () => Object.assign({}, data)
  };
}

TestSuite.addTest('APIHandler utilise le token en cache', function (assert, withMockedGlobals) {
  const store = createMockPropertiesStore({
    KIZEO_API_TOKEN: 'cached-token'
  });
  let spreadsheetOpened = false;
  let fetchInvocationCount = 0;
  let usedToken = null;

  withMockedGlobals(
    {
      PropertiesService: {
        getDocumentProperties: () => store,
        getScriptProperties: () => store
      },
      SpreadsheetApp: {
        openById: () => {
          spreadsheetOpened = true;
          return null;
        }
      },
      UrlFetchApp: {
        fetch: (url, settings) => {
          fetchInvocationCount += 1;
          usedToken = settings.headers.authorization;
          return {
            getResponseCode: () => 200,
            getHeaders: () => ({ 'Content-Type': 'application/json' }),
            getContentText: () => JSON.stringify({ status: 'ok' })
          };
        }
      }
    },
    function () {
      const result = requeteAPIDonnees('GET', '/forms', null);
      assert.equals(fetchInvocationCount, 1, 'fetch doit être appelé une fois');
      assert.equals(usedToken, 'cached-token', 'doit utiliser le token en cache');
      assert.falsy(spreadsheetOpened, 'le classeur token ne doit pas être relu');
      assert.equals(result.responseCode, 200, 'code HTTP attendu');
      assert.deepEquals(result.data, { status: 'ok' }, 'payload JSON attendu');
      assert.equals(result.error, null, 'aucune erreur attendue');
    }
  );
});

TestSuite.addTest('APIHandler rafraîchit le token sur 401', function (assert, withMockedGlobals) {
  const store = createMockPropertiesStore({
    KIZEO_API_TOKEN: 'expired-token'
  });
  let spreadsheetReadCount = 0;
  const tokensByCall = [];
  let step = 0;

  withMockedGlobals(
    {
      PropertiesService: {
        getDocumentProperties: () => store,
        getScriptProperties: () => store
      },
      SpreadsheetApp: {
        openById: () => ({
          getSheetByName: () => ({
            getRange: () => ({
              getValue: () => {
                spreadsheetReadCount += 1;
                return 'fresh-token';
              }
            })
          })
        })
      },
      UrlFetchApp: {
        fetch: (url, settings) => {
          tokensByCall.push(settings.headers.authorization);
          if (step === 0) {
            step += 1;
            return {
              getResponseCode: () => 401,
              getHeaders: () => ({ 'Content-Type': 'application/json' }),
              getContentText: () => JSON.stringify({ status: 'unauthorized' })
            };
          }
          return {
            getResponseCode: () => 200,
            getHeaders: () => ({ 'Content-Type': 'application/json' }),
            getContentText: () => JSON.stringify({ status: 'ok' })
          };
        }
      }
    },
    function () {
      const result = requeteAPIDonnees('GET', '/forms', null);
      assert.equals(spreadsheetReadCount, 1, 'le classeur doit être consulté une seule fois');
      assert.deepEquals(tokensByCall, ['expired-token', 'fresh-token'], 'séquence de tokens incorrecte');
      assert.equals(store.getProperty('KIZEO_API_TOKEN'), 'fresh-token', 'le PropertiesService doit être mis à jour');
      assert.equals(result.responseCode, 200, 'la requête doit réussir après rafraîchissement');
      assert.equals(result.error, null, 'aucune erreur attendue après retry');
    }
  );
});

TestSuite.addTest('APIHandler retourne une erreur structurée sur exception fetch', function (assert, withMockedGlobals) {
  const store = createMockPropertiesStore({
    KIZEO_API_TOKEN: 'valid'
  });
  const uniqueSuffix = 'Network failure ' + Date.now();

  withMockedGlobals(
    {
      PropertiesService: {
        getDocumentProperties: () => store,
        getScriptProperties: () => store
      },
      SpreadsheetApp: {
        openById: () => ({
          getSheetByName: () => ({
            getRange: () => ({
              getValue: () => 'valid'
            })
          })
        })
      },
      UrlFetchApp: {
        fetch: () => {
          throw new Error(uniqueSuffix);
        }
      }
    },
    function () {
      const result = requeteAPIDonnees('GET', '/forms', null);
      assert.equals(result.responseCode, null, 'responseCode doit être null');
      assert.truthy(result.error, 'un objet error doit être renvoyé');
      assert.truthy(
        result.error.message && result.error.message.indexOf('Network failure') === 0,
        'le message doit contenir la cause réseau'
      );
      assert.truthy(
        result.error.cause && String(result.error.cause).indexOf(uniqueSuffix) !== -1,
        'la cause doit contenir le détail de l’erreur'
      );
    }
  );
});

TestSuite.addTest('handleException gère l’absence de classeur actif et throttle les e-mails', function (assert, withMockedGlobals) {
  const mailCalls = [];
  const cacheStore = {};

  function createCache() {
    return {
      get: (key) => cacheStore[key] || null,
      put: (key, value) => {
        cacheStore[key] = value;
      }
    };
  }

  withMockedGlobals(
    {
      SpreadsheetApp: {
        getActiveSpreadsheet: () => {
          throw new Error('no spreadsheet');
        }
      },
      Session: {
        getActiveUser: () => ({
          getEmail: () => 'tester@example.com'
        })
      },
      ScriptApp: {
        getScriptId: () => 'script-unit-test'
      },
      DriveApp: {
        getFileById: () => ({
          getName: () => 'Mock Project'
        })
      },
      CacheService: {
        getScriptCache: () => createCache()
      },
      MailApp: {
        sendEmail: (payload) => {
          mailCalls.push(payload);
        }
      }
    },
    function () {
      handleException('unitTest', new Error('boom'), { foo: 'bar' });
      handleException('unitTest', new Error('boom'), { foo: 'bar' });
      assert.equals(mailCalls.length, 1, 'un seul e-mail doit être envoyé grâce au throttle');
      const mail = mailCalls[0];
      assert.truthy(
        mail.subject.indexOf('[Mock Project]') === 0,
        'le sujet doit commencer par le nom du projet'
      );
      assert.truthy(mail.subject.indexOf('unitTest') !== -1, 'le sujet doit contenir le nom de la fonction');
      assert.truthy(mail.body.indexOf('foo: bar') !== -1, 'le contexte doit apparaître dans le corps du mail');
      assert.truthy(
        mail.to.indexOf('tester@example.com') !== -1,
        'le destinataire doit inclure l’utilisateur actif'
      );
      assert.truthy(
        mail.to.indexOf(ERROR_MAIL_RECIPIENT) !== -1,
        'le destinataire doit inclure le contact support'
      );
    }
  );
});

TestSuite.addTest('handleException envoie un mail réel à l’utilisateur actif', function (assert) {
  try {
    handleException(
      'automatedMailTest',
      new Error('Test mail automatique ' + new Date().toISOString()),
      { triggeredBy: 'runAllTests' }
    );
    assert.truthy(true, 'handleException doit s’exécuter sans erreur');
  } catch (e) {
    assert.fail('Erreur lors de l’envoi de mail réel: ' + e);
  }
});

TestSuite.addTest('handleResponses sans synchronisation Sheets legacy', function (assert, withMockedGlobals) {
  const propertyStore = createMockPropertiesStore({
    ENABLE_LEGACY_SHEETS_SYNC: 'false'
  });
  const bigQueryCapture = {
    raw: null,
    parent: null,
    parentColumns: null,
    subTables: null,
    media: null
  };
  const fetchCalls = [];

  const fakeUnreadPayload = {
    status: 'ok',
    data: [
      {
        _id: 'record-001'
      }
    ]
  };

  const detailedRecord = {
    form_id: 'FORM-1',
    form_unique_id: 'FORM-1-UID',
    id: 'record-001',
    user_id: 'user-1',
    last_name: 'Doe',
    first_name: 'Jane',
    answer_time: '2024-06-10T08:00:00Z',
    update_time: '2024-06-10T10:00:00Z',
    origin_answer: 'mobile',
    fields: {
      temperature_air: { type: 'number', value: 18.5 },
      commentaires: { type: 'text', value: 'RAS' }
    }
  };

  const services = createIngestionServices({
    fetch: (method, path) => {
      fetchCalls.push({ method, path });
      if (path.indexOf('/data/unread/') !== -1) {
        return { data: fakeUnreadPayload, responseCode: 200 };
      }
      if (path.indexOf('/data/record-001') !== -1) {
        return { data: { data: detailedRecord }, responseCode: 200 };
      }
      if (path.indexOf('/markasreadbyaction/') !== -1) {
        return { data: { status: 'ok' }, responseCode: 200 };
      }
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    },
    logger: {
      log: () => {}
    },
    now: () => new Date('2024-06-10T11:00:00Z'),
    bigQuery: {
      ingestRawBatch: (formulaire, rows) => {
        bigQueryCapture.raw = rows;
      },
      ingestParentBatch: (formulaire, rows, columns) => {
        bigQueryCapture.parent = rows;
        bigQueryCapture.parentColumns = columns;
      },
      ingestSubTablesBatch: (formulaire, subTables) => {
        bigQueryCapture.subTables = subTables;
      },
      ingestMediaBatch: (formulaire, medias) => {
        bigQueryCapture.media = medias;
      }
    }
  });

  withMockedGlobals(
    {
      PropertiesService: {
        getDocumentProperties: () => propertyStore,
        getScriptProperties: () => propertyStore
      }
    },
    function () {
      const formulaire = { id: 'FORM-1', nom: 'Formulaire test' };
      const medias = [];
      const result = handleResponses(
        { getId: () => 'MOCK-SPREADSHEET' },
        formulaire,
        '/forms/FORM-1/data/unread/action-test/10?includeupdated',
        'action-test',
        medias,
        false,
        {
          services,
          targets: {
            bigQuery: true,
            externalLists: false
          },
          unreadPayload: fakeUnreadPayload
        }
      );
      const legacyUsage = propertyStore.getAll();
      assert.equals(result.status, 'INGESTED', 'statut attendu');
      assert.truthy(Array.isArray(bigQueryCapture.raw) && bigQueryCapture.raw.length === 1, 'ligne raw attendue');
      assert.truthy(bigQueryCapture.parentColumns && bigQueryCapture.parentColumns.length >= 2, 'colonnes dynamiques attendues');
      assert.falsy(Object.keys(legacyUsage).some((key) => key.indexOf('LEGACY_USAGE_legacy_flag_enabled') !== -1), 'le flag legacy ne doit pas être activé');
      assert.truthy(
        fetchCalls.some((call) => call.path.indexOf('/markasreadbyaction/') !== -1),
        'la requête markasread doit être envoyée'
      );
    }
  );
});

TestSuite.addTest(
  'collectResponseArtifacts ignore la persistance Sheets quand legacy désactivé',
  function (assert, withMockedGlobals) {
    const persistCalls = [];
    const formulaire = { id: 'FORM-PR', nom: 'Formulaire Process' };
    const medias = [];
    const detailRecord = {
      id: 'rec-01',
      form_id: 'FORM-PR',
      update_time: '2025-01-10T10:00:00Z',
      fields: { champ: { type: 'text', value: 'ok' } }
    };

    withMockedGlobals(
      {
        bqPrepareParentRow: () => ({
          row: { id: 'rec-01' },
          columns: [],
          subforms: []
        }),
        recordLegacyUsage: () => {}
      },
      function () {
        const snapshot = { existingHeaders: ['A'], rowEnCours: ['B'] };
        const snapshotService = {
          buildRowSnapshot: () => snapshot,
          persistSnapshot: () => {
            persistCalls.push('called');
            return { persisted: true };
          }
        };
        const result = ProcessManager.collectResponseArtifacts(
          [{ _id: 'rec-01' }],
          {
            fetchFn: (method, path) => {
              if (path.indexOf('/data/rec-01') !== -1) {
                return { data: { data: detailRecord }, responseCode: 200 };
              }
              return { data: { status: 'ok', data: [] }, responseCode: 200 };
            },
            formulaire,
            log: () => {},
            spreadsheetBdD: { getId: () => 'SPREAD' },
            medias,
            legacySheetSyncEnabled: false,
            snapshotService
          }
        );

        assert.equals(persistCalls.length, 0, 'persistSnapshot ne doit pas être appelé');
        assert.equals(result.lastSnapshot, snapshot, 'le snapshot doit être renvoyé tel quel');
        assert.equals(result.processedDataIds.has('rec-01'), true, 'l’identifiant doit être collecté');
        assert.equals(result.bigQueryContext.rawRows.length, 1, 'une ligne raw attendue');
      }
    );
  }
);

TestSuite.addTest(
  'collectResponseArtifacts persiste via fallback global en mode legacy',
  function (assert, withMockedGlobals) {
    const persistCalls = [];
    const legacyMarkers = [];
    const formulaire = { id: 'FORM-L', nom: 'Formulaire Legacy' };

    withMockedGlobals(
      {
        bqPrepareParentRow: () => ({
          row: { id: 'rec-02' },
          columns: [],
          subforms: []
        }),
        persistLegacySnapshotToSheet: (spreadsheet, form, snapshot) => {
          persistCalls.push({ spreadsheet, form, snapshot });
          return Object.assign({ persisted: true }, snapshot);
        },
        recordLegacyUsage: (marker) => legacyMarkers.push(marker)
      },
      function () {
        const medias = [];
        const snapshot = { existingHeaders: ['X'], rowEnCours: ['Y'] };
        const snapshotService = {
          buildRowSnapshot: () => snapshot
        };
        const detailRecord = {
          id: 'rec-02',
          form_id: 'FORM-L',
          update_time: '2025-02-01T10:00:00Z',
          fields: { champ: { type: 'text', value: 'legacy' } }
        };

        const result = ProcessManager.collectResponseArtifacts(
          [{ _id: 'rec-02' }],
          {
            fetchFn: (method, path) => {
              if (path.indexOf('/data/rec-02') !== -1) {
                return { data: { data: detailRecord }, responseCode: 200 };
              }
              return { data: { status: 'ok', data: [] }, responseCode: 200 };
            },
            formulaire,
            log: () => {},
            spreadsheetBdD: { getId: () => 'SPREAD-LEG' },
            medias,
            legacySheetSyncEnabled: true,
            snapshotService
          }
        );

        assert.equals(persistCalls.length, 1, 'persistLegacySnapshotToSheet doit être invoqué');
        assert.truthy(result.lastSnapshot.persisted, 'le snapshot doit refléter la persistance');
        assert.truthy(legacyMarkers.indexOf('legacy_flag_enabled') !== -1, 'le flag legacy doit être enregistré');
        assert.equals(result.processedDataIds.has('rec-02'), true, 'l’identifiant doit être marqué');
      }
    );
  }
);

TestSuite.addTest('persistLegacySnapshotToSheet délègue au service snapshot', function (assert, withMockedGlobals) {
  const calls = [];
  withMockedGlobals(
    {
      SheetSnapshot: {
        persistSnapshot: (spreadsheet, formulaire, snapshot) => {
          calls.push({ spreadsheet, formulaire, snapshot });
          return Object.assign({ persisted: true }, snapshot);
        }
      },
      recordLegacyUsage: () => {}
    },
    function () {
      const snapshot = {
        existingHeaders: ['col1', 'col2'],
        rowEnCours: ['A', 'B']
      };
      const result = persistLegacySnapshotToSheet(
        { getId: () => 'SPREADSHEET' },
        { id: 'FORM-LEG', nom: 'Legacy' },
        snapshot
      );
      assert.equals(calls.length, 1, 'persistSnapshot doit être invoqué');
      assert.equals(result.persisted, true, 'le service doit contrôler le retour');
    }
  );
});

TestSuite.addTest('bqRecordFieldDictionaryEntries insère les mappings uniques', function (assert, withMockedGlobals) {
  const insertCalls = [];
  const tablesInserted = [];
  const mockConfig = {
    projectId: 'proj',
    datasetId: 'dataset'
  };

  const mockBigQuery = {
    Tables: {
      get: (projectId, datasetId, tableId) => {
        if (tableId === BQ_FIELD_DICTIONARY_TABLE_ID) {
          throw new Error('not found');
        }
        return {};
      },
      insert: (resource, projectId, datasetId) => {
        tablesInserted.push({ resource, projectId, datasetId });
      },
      patch: () => {}
    },
    Datasets: {
      get: () => ({})
    },
    Tabledata: {
      insertAll: (requestBody, projectId, datasetId, tableId) => {
        insertCalls.push({ requestBody, projectId, datasetId, tableId });
        return {};
      }
    }
  };

  withMockedGlobals(
    {
      BigQuery: mockBigQuery,
      Utilities: {
        sleep: () => {}
      }
    },
    function () {
      const dynamicColumns = [
        { name: 'temperature_air', type: 'FLOAT64', mode: 'NULLABLE', label: 'Température', sourceType: 'field' },
        { name: 'temperature_air', type: 'FLOAT64', mode: 'NULLABLE', label: 'Température Duplicat', sourceType: 'field' }
      ];
      bqRecordFieldDictionaryEntries(
        mockConfig,
        { id: 'FORM-1', nom: 'Form test' },
        'form_1_table',
        dynamicColumns,
        BQ_PARENT_BASE_COLUMNS
      );
      assert.truthy(tablesInserted.length >= 1, 'la table dictionnaire doit être créée au besoin');
      assert.equals(insertCalls.length, 1, 'un insertAll attendu');
      const rows = insertCalls[0].requestBody.rows;
      const uniqueSlugs = new Set(rows.map((row) => row.json.field_slug));
      assert.equals(rows.length, uniqueSlugs.size, 'pas de doublon attendu sur les slugs');
      const temperatureRow = rows.find((row) => row.json.field_slug === 'temperature_air');
      assert.truthy(temperatureRow, 'le champ dynamique doit être présent');
      assert.equals(
        temperatureRow.json.field_label,
        'Température',
        'le premier label rencontré doit être conservé'
      );
    }
  );
});

TestSuite.addTest('parseConfigBooleanFlag et buildExecutionTargets', function (assert) {
  assert.equals(parseConfigBooleanFlag('true', false), true, 'true string');
  assert.equals(parseConfigBooleanFlag('False', true), false, 'false string case insensitive');
  assert.equals(parseConfigBooleanFlag('', true), true, 'fallback sur default');
  assert.equals(parseConfigBooleanFlag(null, false), false, 'null => default');
  const targets = buildExecutionTargets(
    { ingest_bigquery: '0', form_id: 'abc' },
    { sheet: false, externalLists: false }
  );
  assert.equals(targets.bigQuery, false, 'override config via sheet value');
  assert.equals(targets.sheet, false, 'override explicite');
  assert.equals(targets.externalLists, false, 'override externalLists');
});

TestSuite.addTest('buildExecutionTargets fallback sans parseConfigBooleanFlag', function (assert, withMockedGlobals) {
  withMockedGlobals(
    {
      parseConfigBooleanFlag: undefined,
      CONFIG_INGEST_BIGQUERY_KEY: 'ingest_bigquery'
    },
    function () {
      const targets = ProcessManager.buildExecutionTargets(
        { ingest_bigquery: '0', form_id: 'FORM-TST' },
        { sheet: false }
      );
      assert.equals(targets.bigQuery, false, 'la valeur "0" doit désactiver BigQuery');
      assert.equals(targets.sheet, false, 'l’override doit être conservé');
      assert.equals(targets.externalLists, true, 'les listes externes restent actives par défaut');
    }
  );
});

TestSuite.addTest('resolveUnreadDataset couvre les différents chemins', function (assert, withMockedGlobals) {
  const formulaire = { id: 'FORM-TEST' };
  const logMessages = [];
  const log = (message) => logMessages.push(message);

  const baseUnread = {
    status: 'ok',
    data: [{ id: 'A' }]
  };

  const fetchFn = (method, path) => {
    if (path.indexOf('/data/unread/') !== -1) {
      return { data: baseUnread, responseCode: 200 };
    }
    if (path.indexOf('/data/all') !== -1) {
      return {
        data: {
          status: 'ok',
          data: [{ id: 'fallback-record' }]
        },
        responseCode: 200
      };
    }
    throw new Error('Unexpected path ' + path);
  };

  const okResolution = resolveUnreadDataset(fetchFn, formulaire, '/unread', false, baseUnread, log);
  assert.equals(okResolution.type, 'OK', 'cas données non lues');

  const noUnread = resolveUnreadDataset(
    () => ({ data: { status: 'ok', data: [] }, responseCode: 200 }),
    formulaire,
    '/unread',
    true,
    null,
    log
  );
  assert.equals(noUnread.type, 'NO_UNREAD', 'cas sans unread avec historique');

  const fallback = resolveUnreadDataset(
    fetchFn,
    formulaire,
    '/unread',
    false,
    { status: 'ok', data: [] },
    log
  );
  assert.equals(fallback.type, 'FALLBACK_OK', 'fallback data/all attendu');
  const hasFallbackData =
    Array.isArray(fallback.payload.data) && fallback.payload.data.length > 0;
  assert.truthy(hasFallbackData, 'fallback doit contenir des données');
});

TestSuite.addTest('bqPrepareParentRow construit les colonnes dynamiques attendues', function (assert) {
  const formulaire = { id: 'FORM-1', nom: 'Formulaire Test' };
  const data = {
    form_id: 'FORM-1',
    id: 'record-123',
    user_id: 'user-1',
    last_name: 'Doe',
    first_name: 'John',
    answer_time: '2024-06-10T08:00:00Z',
    update_time: '2024-06-10T09:00:00Z',
    origin_answer: 'mobile',
    fields: {
      temperature: { type: 'number', value: 18.7 },
      is_valid: { type: 'yesno', value: 'yes' },
      remarks: { type: 'text', value: 'RAS' },
      sous_formulaire: {
        type: 'subform',
        value: [
          {
            fields: {
              champ_a: { value: 'A' },
              champ_b: { value: 'B' }
            }
          }
        ]
      }
    }
  };

  const prepared = bqPrepareParentRow(formulaire, data);
  assert.truthy(prepared, 'résultat attendu');
  assert.equals(prepared.row.data_id, 'record-123', 'data_id attendu');
  const columnNames = prepared.columns.map((col) => col.name);
  assert.truthy(columnNames.some((name) => name.indexOf('temperature') !== -1), 'colonne dynamique temperature');
  assert.truthy(columnNames.some((name) => name.indexOf('is_valid') !== -1), 'colonne dynamique bool');
  assert.truthy(columnNames.some((name) => name.indexOf('table_sous_formulaire') !== -1), 'colonne table subform');
  assert.truthy(
    prepared.subforms && prepared.subforms.length === 1 && prepared.subforms[0].rows.length === 1,
    'subform préparée'
  );
});

TestSuite.addTest('bqComputeTableName gère les alias complexes', function (assert) {
  const table = bqComputeTableName('123', 'Formulaire Éxemple', ' 123__mesures journalières ');
  assert.equals(table, '123__mesures_journalieres', 'normalisation attendue');
  const fallback = bqComputeTableName('123', 'Nom', '');
  assert.equals(fallback, '123__nom', 'fallback sur nom');
  const aliasPart = bqExtractAliasPart('123__nom', '123');
  assert.equals(aliasPart, 'nom', 'extraction alias');
});

TestSuite.addTest('bqSlugifyIdentifier et bqEnsureUniqueName', function (assert) {
  const slug = bqSlugifyIdentifier(' Température / Eau ');
  assert.equals(slug, 'temperature_eau', 'Slugification attendue');
  const used = new Set(['field', 'field_1']);
  const unique = bqEnsureUniqueName('field', used);
  assert.equals(unique, 'field_2', 'Suffixe incrémental attendu');
  assert.equals(
    bqNormalizeTableIdentifier('demo___main'),
    'demo__main',
    'normalisation conserve separateur double sans triples'
  );
});

TestSuite.addTest('bqCoerceValueToString gère les types complexes', function (assert) {
  assert.equals(bqCoerceValueToString(['a', null, 'b']), 'a, b', 'tableau string');
  assert.equals(
    bqCoerceValueToString({ foo: 'bar' }),
    JSON.stringify({ foo: 'bar' }),
    'objet converti en JSON'
  );
  const repeated = bqCoerceValueToStringArray(['x', null, 'y']);
  assert.deepEquals(repeated, ['x', 'y'], 'tableau répété nettoyé');
});

TestSuite.addTest('bqConvertFieldValue prend en charge divers types', function (assert) {
  const numeric = bqConvertFieldValue({ type: 'number', value: '12.5' });
  assert.equals(numeric.type, 'FLOAT64', 'type numérique attendu');
  assert.equals(numeric.value, 12.5, 'conversion numérique');

  const repeatedInt = bqConvertFieldValue({ type: 'integer', value: ['1', '2', null] });
  assert.equals(repeatedInt.mode, 'REPEATED', 'mode répété attendu');
  assert.deepEquals(repeatedInt.value, [1, 2, null], 'conversion integer');

  const boolField = bqConvertFieldValue({ type: 'yesno', value: 'yes' });
  assert.equals(boolField.type, 'BOOL', 'conversion bool');
  assert.equals(boolField.value, true, 'valeur booléenne');

  const jsonField = bqConvertFieldValue({ type: 'object', value: { foo: 'bar' } });
  assert.equals(jsonField.type, 'JSON', 'type JSON attendu');
  assert.deepEquals(jsonField.value, { foo: 'bar' }, 'payload JSON conservé');
});

TestSuite.addTest('bqSerializeSubformValue et bqPrepareSubformRows formattent correctement', function (assert) {
  assert.equals(
    bqSerializeSubformValue(['A', '', 'B']),
    'A, B',
    'serialization tableau subform'
  );
  const formulaire = { id: 'FORM-123', nom: 'Formulaire' };
  const parentContext = {
    form_id: 'FORM-123',
    form_name: 'Formulaire',
    data_id: 'record-1',
    form_unique_id: 'unique',
    answer_time: '2024-01-01T00:00:00Z',
    update_time: '2024-01-01T01:00:00Z',
    ingestion_time: '2024-01-01T02:00:00Z'
  };
  const prepared = bqPrepareSubformRows(
    formulaire,
    parentContext,
    'Sous Formulaire',
    null,
    [
      { colonne1: 'val1', colonne2: 42 },
      { colonne1: 'val2', colonne2: 43 }
    ]
  );
  assert.truthy(prepared, 'résultat non nul');
  assert.equals(prepared.rows.length, 2, 'deux lignes produites');
  assert.truthy(
    prepared.columns.some((col) => col.name.indexOf('colonne1') !== -1),
    'colonne colonne1 attendue'
  );
  assert.truthy(
    prepared.columns.every((col) => col.sourceType === 'subform_field'),
    'sourceType subform_field attendu'
  );
});

TestSuite.addTest('markResponsesAsRead découpe les lots et journalise les erreurs', function (assert, withMockedGlobals) {
  const calls = [];
  let handleCalls = 0;
  const fetchFn = (method, path, payload) => {
    calls.push({ method, path, payload });
    if (calls.length === 2) {
      throw new Error('expected failure');
    }
    return { data: { status: 'ok' }, responseCode: 200 };
  };

  withMockedGlobals(
    {
      handleException: () => {
        handleCalls += 1;
      }
    },
    function () {
      markResponsesAsRead(
        { id: 'FORM-TEST' },
        'ACTION',
        Array.from({ length: 120 }, (_, i) => `ID-${i + 1}`),
        fetchFn
      );
    }
  );

  assert.equals(calls.length, 3, 'trois lots doivent être traités (120 éléments / 50)');
  assert.equals(calls[0].payload.data_ids.length, 50, 'chunk de 50 attendu');
  assert.equals(handleCalls, 1, 'une seule erreur doit être remontée via handleException');
});

TestSuite.addTest('logLegacyUsageStats synthétise les marqueurs legacy', function (assert, withMockedGlobals) {
  const logs = [];
  const store = createMockPropertiesStore({
    LEGACY_USAGE_sheet_append: '2024-06-01T10:00:00Z',
    LEGACY_USAGE_legacy_flag_enabled: '2024-06-01T10:05:00Z',
    OTHER_KEY: 'value'
  });

  withMockedGlobals(
    {
      PropertiesService: {
        getDocumentProperties: () => store,
        getScriptProperties: () => store
      },
      console: {
        log: (message) => logs.push(message)
      },
      handleException: () => {
        assert.fail('handleException ne doit pas être appelée');
      }
    },
    function () {
      const count = logLegacyUsageStats();
      assert.equals(count, 2, 'deux marqueurs legacy doivent être détectés');
    }
  );

  assert.truthy(
    logs.some((message) => message.indexOf('LEGACY_USAGE_sheet_append') !== -1),
    'le détail sheet_append doit apparaître'
  );
  assert.truthy(
    logs.some((message) => message.indexOf('total flags legacy = 2') !== -1),
    'le total doit être journalisé'
  );
});

TestSuite.addTest('createIngestionServices expose SheetSnapshot', function (assert, withMockedGlobals) {
  const snapshotStub = {
    marker: 'snapshot'
  };
  withMockedGlobals(
    {
      SheetSnapshot: snapshotStub
    },
    function () {
      const services = createIngestionServices();
      assert.equals(services.snapshot, snapshotStub, 'le service snapshot doit être exposé');
    }
  );
});

TestSuite.addTest('resolveUnreadDataset gère les réponses invalides', function (assert) {
  const logMessages = [];
  const log = (message) => logMessages.push(message);
  const formulaire = { id: 'FORM-INVALID' };

  const invalid = resolveUnreadDataset(
    () => ({ data: { unexpected: true }, responseCode: 200 }),
    formulaire,
    '/invalid',
    false,
    null,
    log
  );
  assert.equals(invalid.type, 'INVALID', 'un payload inattendu doit être marqué invalid');
});

TestSuite.addTest('handleResponses active le mode legacy Sheets lorsque flag présent', function (assert, withMockedGlobals) {
  const propertyStore = createMockPropertiesStore({
    ENABLE_LEGACY_SHEETS_SYNC: 'true'
  });
  const appendedRows = [];
  const usageMarkers = [];
  const snapshotStub = {
    buildRowSnapshot: () => ({ existingHeaders: ['col1', 'col2'], rowEnCours: ['A', 'B'] }),
    persistSnapshot: (spreadsheet, formulaire, snapshot) => {
      appendedRows.push(snapshot.rowEnCours);
      return snapshot;
    }
  };
  const fetchFn = (method, path) => {
    if (path.indexOf('/data/unread') !== -1) {
      return {
        data: {
          status: 'ok',
          data: [{ _id: 'R1' }]
        },
        responseCode: 200
      };
    }
    if (path.indexOf('/data/R1') !== -1) {
      return {
        data: {
          data: {
            id: 'R1',
            form_id: 'FORM-L',
            form_unique_id: 'U1',
            user_id: 'user',
            last_name: 'Doe',
            first_name: 'Jane',
            answer_time: '2024-01-01T00:00:00Z',
            update_time: '2024-01-01T01:00:00Z',
            origin_answer: 'mobile',
            fields: {}
          }
        },
        responseCode: 200
      };
    }
    if (path.indexOf('/markasreadbyaction') !== -1) {
      return { data: { status: 'ok' }, responseCode: 200 };
    }
    return { data: { status: 'ok', data: [] }, responseCode: 200 };
  };

  withMockedGlobals(
    {
      PropertiesService: {
        getDocumentProperties: () => propertyStore,
        getScriptProperties: () => propertyStore
      },
      SheetSnapshot: snapshotStub,
      recordLegacyUsage: (marker) => usageMarkers.push(marker)
    },
    function () {
      const formulaire = { id: 'FORM-L', nom: 'Legacy' };
      const result = handleResponses(
        { getId: () => 'SPREADSHEET' },
        formulaire,
        '/forms/FORM-L/data/unread/ACTION/10?includeupdated',
        'ACTION',
        [],
        false,
        {
          services: createIngestionServices({
            fetch: fetchFn,
            logger: { log: () => {} },
            now: () => new Date('2024-01-01T02:00:00Z'),
            bigQuery: {
              ingestRawBatch: () => {},
              ingestParentBatch: () => {},
              ingestSubTablesBatch: () => {},
              ingestMediaBatch: () => {}
            },
            snapshot: snapshotStub
          })
        }
      );
      assert.equals(result.status, 'INGESTED', 'ingestion attendue');
      assert.truthy(usageMarkers.includes('legacy_flag_enabled'), 'le flag legacy doit être enregistré');
      assert.equals(appendedRows.length, 1, 'une ligne synchronisée');
    }
  );
});

TestSuite.addTest('ingestBigQueryPayloads respecte le flag bigQuery', function (assert) {
  const context = {
    rawRows: [{ id: 'R1' }],
    parentRows: [{ id: 'R1' }],
    parentColumns: { dynamic_col: { name: 'dynamic_col', type: 'STRING', mode: 'NULLABLE' } },
    subTables: { table_sub: { rows: [{ parent_data_id: 'R1' }], columns: {} } }
  };
  const medias = [{ dataId: 'R1' }];
  const services = {
    bigQuery: {
      ingestRawBatch: () => {
        assert.fail('ingestRawBatch ne doit pas être appelé');
      },
      ingestParentBatch: () => {
        assert.fail('ingestParentBatch ne doit pas être appelé');
      },
      ingestSubTablesBatch: () => {
        assert.fail('ingestSubTablesBatch ne doit pas être appelé');
      },
      ingestMediaBatch: () => {
        assert.fail('ingestMediaBatch ne doit pas être appelé');
      }
    }
  };
  const logs = [];
  ingestBigQueryPayloads(
    { id: 'FORM', nom: 'Test' },
    context,
    medias,
    services,
    { bigQuery: false },
    (message) => logs.push(message)
  );
  assert.truthy(
    logs.some((msg) => msg.indexOf('ingestion BigQuery désactivée') !== -1),
    'un log doit indiquer la désactivation'
  );
});

TestSuite.addTest('runExternalListsSync renvoie le statut attendu', function (assert, withMockedGlobals) {
  const formulaire = { id: 'FORM-EXT', nom: 'Formulaire EXTERNAL' };
  const snapshot = { existingHeaders: ['col'], rowEnCours: ['value'] };

  const fetchResponses = [
    { data: { status: 'ok', data: [{ id: 'unread' }] }, responseCode: 200 },
    { data: { status: 'ok', data: [] }, responseCode: 200 },
    { data: { status: 'ok', data: [] }, responseCode: 200 }
  ];
  let fetchIndex = 0;
  const fetchFn = () => fetchResponses[fetchIndex++] || fetchResponses[fetchResponses.length - 1];

  withMockedGlobals(
    {
      majListeExterne: () => 'Mise A Jour OK',
      handleException: () => {}
    },
    function () {
      const first = runExternalListsSync(formulaire, snapshot, fetchFn, '/api', () => {});
      assert.equals(first.metadataUpdateStatus, 'SKIPPED', 'tant que unread non vide => SKIPPED');
      const second = runExternalListsSync(formulaire, snapshot, fetchFn, '/api', () => {});
      assert.equals(second.metadataUpdateStatus, 'Mise A Jour OK', 'mise à jour doit être rapportée');
    }
  );
});

TestSuite.addTest('bqApplySchemaAdjustmentsToRows convertit les colonnes', function (assert) {
  const rows = [
    {
      numbers: [1, 2],
      details: { foo: 'bar' }
    }
  ];
  const adjustments = {
    convertedToString: [
      { name: 'numbers', repeated: true },
      { name: 'details', repeated: false }
    ]
  };
  const result = bqApplySchemaAdjustmentsToRows(rows, adjustments);
  assert.deepEquals(result[0].numbers, ['1', '2'], 'tableau converti en chaînes');
  assert.equals(
    result[0].details,
    JSON.stringify({ foo: 'bar' }),
    'objet converti en chaîne JSON'
  );
});

TestSuite.addTest('bqSafeInsertAll relance après NOT_FOUND', function (assert, withMockedGlobals) {
  const insertCalls = [];
  let ensureCalls = 0;
  const mockBigQuery = {
    Tabledata: {
      insertAll: () => {
        insertCalls.push(true);
        if (insertCalls.length === 1) {
          const error = new Error('Not Found');
          error.message = 'Not Found';
          throw error;
        }
        return { status: 'OK' };
      }
    },
    Datasets: {
      get: () => ({}),
      insert: () => ({})
    },
    Tables: {
      insert: () => ({})
    }
  };

  withMockedGlobals(
    {
      BigQuery: mockBigQuery,
      Utilities: {
        sleep: () => {}
      }
    },
    function () {
      const response = bqSafeInsertAll({}, { projectId: 'p', datasetId: 'd' }, 't', () => {
        ensureCalls += 1;
      });
      assert.equals(insertCalls.length, 2, 'deux tentatives attendues');
      assert.equals(ensureCalls, 1, 'ensureFn doit être appelé une fois');
      assert.deepEquals(response, { status: 'OK' }, 'réponse finale attendue');
    }
  );
});

TestSuite.addTest('resolveIsoTimestamp tolère les valeurs invalides', function (assert) {
  const iso = resolveIsoTimestamp(() => new Date('2024-01-02T03:04:05Z'));
  assert.equals(iso, '2024-01-02T03:04:05.000Z', 'iso attendu');
  const fallback = resolveIsoTimestamp(() => 'valeur invalide');
  assert.truthy(fallback.indexOf('T') !== -1, 'fallback doit retourner une date ISO');
});

TestSuite.addTest('bqConvertFieldValue gère les timestamps répétés', function (assert) {
  const multiValue = bqConvertFieldValue({ type: 'datetime', value: ['2024-01-01T00:00:00Z', null] });
  assert.equals(multiValue.type, 'TIMESTAMP', 'type timestamp');
  assert.equals(multiValue.mode, 'REPEATED', 'mode répété');
  assert.deepEquals(
    multiValue.value,
    ['2024-01-01T00:00:00.000Z', null],
    'timestamp normalisé'
  );
});

function zzDescribeScenarioIngestion1018296() {
  const formulaire = {
    nom: 'Scenario Mock 1018296',
    id: '1018296',
    tableName: 'mock_table_1018296',
    alias: 'mock_alias'
  };
  const fakeUnreadData = {
    status: 'ok',
    data: [
      {
        _id: 'record-001',
        summary: 'Mock summary'
      }
    ]
  };
  const detailedRecord = {
    form_id: '1018296',
    form_unique_id: 'FORM-001',
    id: 'record-001',
    user_id: 'user-1',
    last_name: 'Durand',
    first_name: 'Alex',
    answer_time: '2024-05-10T08:00:00Z',
    update_time: '2024-05-10T09:00:00Z',
    origin_answer: 'mobile',
    fields: {
      temperature_air: { type: 'number', value: 18.5 },
      commentaires: { type: 'text', value: 'RAS' }
    }
  };
  const callLog = [];
  const bigQueryCapture = {
    raw: [],
    parent: [],
    parentColumns: [],
    subTables: null,
    media: []
  };
  let unreadCallCount = 0;

  const stubFetch = (method, path) => {
    if (method === 'GET' && path.indexOf('/data/unread/') !== -1) {
      unreadCallCount += 1;
      if (unreadCallCount === 1) {
        return { data: fakeUnreadData, responseCode: 200 };
      }
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/data/record-001') !== -1) {
      return { data: { data: detailedRecord }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/data/all') !== -1) {
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    }
    if (method === 'POST' && path.indexOf('/markasreadbyaction/') !== -1) {
      return { data: { status: 'ok' }, responseCode: 200 };
    }
    return { data: { status: 'unknown' }, responseCode: 200 };
  };

  const services = createIngestionServices({
    fetch: stubFetch,
    logger: {
      log: (message) => callLog.push(message)
    },
    now: () => new Date('2024-05-10T10:00:00Z'),
    bigQuery: {
      ingestRawBatch: (formulaireLocal, rows) => {
        bigQueryCapture.raw = rows.slice();
      },
      ingestParentBatch: (formulaireLocal, rows, columns) => {
        bigQueryCapture.parent = rows.slice();
        bigQueryCapture.parentColumns = (columns || []).slice();
      },
      ingestSubTablesBatch: (formulaireLocal, tables) => {
        bigQueryCapture.subTables = tables;
      },
      ingestMediaBatch: (formulaireLocal, rows) => {
        bigQueryCapture.media = rows.slice();
      }
    }
  });

  const medias = [];
  const apiPath = `/forms/${formulaire.id}/data/unread/test-action/1?includeupdated`;

  const result = handleResponses(
    { getId: () => 'MOCK-SPREADSHEET' },
    formulaire,
    apiPath,
    'test-action',
    medias,
    false,
    {
      services,
      targets: {
        bigQuery: true,
        externalLists: false,
        sheet: false
      },
      unreadPayload: fakeUnreadData
    }
  );

  Logger.log(
    JSON.stringify(
      {
        scenario: 'zzDescribeScenarioIngestion1018296',
        result,
        mediasCount: medias.length,
        rawRowCount: bigQueryCapture.raw.length,
        parentRowCount: bigQueryCapture.parent.length,
        logSample: callLog.slice(0, 5)
      },
      null,
      2
    )
  );

  return result;
}

function zzDescribeScenarioBackfillMinimal() {
  const logs = [];
  const capture = {
    raw: [],
    parent: [],
    parentColumns: [],
    subTables: null,
    media: []
  };

  const summaries = [
    {
      _id: 'record-001',
      update_time: '2024-05-10T09:00:00Z'
    },
    {
      _id: 'record-002',
      update_time: '2024-05-11T09:30:00Z'
    }
  ];

  const detailedRecords = {
    'record-001': {
      form_id: '1018296',
      form_unique_id: 'FORM-001',
      id: 'record-001',
      user_id: 'user-1',
      last_name: 'Durand',
      first_name: 'Alex',
      answer_time: '2024-05-10T08:00:00Z',
      update_time: '2024-05-10T09:00:00Z',
      origin_answer: 'mobile',
      fields: {
        temperature_air: { type: 'number', value: 18.5 },
        commentaires: { type: 'text', value: 'RAS' }
      }
    },
    'record-002': {
      form_id: '1018296',
      form_unique_id: 'FORM-002',
      id: 'record-002',
      user_id: 'user-2',
      last_name: 'Martin',
      first_name: 'Zoé',
      answer_time: '2024-05-11T08:10:00Z',
      update_time: '2024-05-11T09:30:00Z',
      origin_answer: 'tablet',
      fields: {
        temperature_air: { type: 'number', value: 21.1 },
        commentaires: { type: 'text', value: 'Contrôle OK' }
      }
    }
  };

  const stubFetch = (method, path) => {
    if (method === 'GET' && path === '/forms/1018296') {
      return { data: { form: { name: 'Formulaire Backfill Test' } }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/forms/1018296/data/all') !== -1) {
      return { data: { status: 'ok', data: summaries }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/forms/1018296/data/') !== -1) {
      const recordId = path.split('/').pop();
      return { data: { data: detailedRecords[recordId] }, responseCode: 200 };
    }
    return { data: { status: 'ok' }, responseCode: 200 };
  };

  const bigQueryOverrides = {
    getConfig: () => ({ projectId: 'test-project', datasetId: 'test_dataset', location: 'europe-west1' }),
    ensureDataset: () => {},
    ensureRawTable: () => {},
    ensureParentTable: () => {},
    ingestRawBatch: (formulaireLocal, rows) => {
      capture.raw = capture.raw.concat(rows);
    },
    ingestParentBatch: (formulaireLocal, rows, columns) => {
      capture.parent = capture.parent.concat(rows);
      capture.parentColumns = capture.parentColumns.concat(columns || []);
    },
    ingestSubTablesBatch: (formulaireLocal, subTables) => {
      capture.subTables = subTables;
    },
    ingestMediaBatch: (formulaireLocal, medias) => {
      capture.media = capture.media.concat(medias || []);
    }
  };

  const overrides = {
    fetch: stubFetch,
    logger: {
      log: (message) => logs.push(message)
    },
    now: () => new Date('2024-05-12T10:00:00Z'),
    bigQuery: bigQueryOverrides
  };

  const summary = bqBackfillForm('1018296', {
    services: overrides,
    chunkSize: 1
  });

  Logger.log(
    JSON.stringify(
      {
        scenario: 'zzDescribeScenarioBackfillMinimal',
        summary,
        rawRowCount: capture.raw.length,
        parentRowCount: capture.parent.length,
        parentColumns: capture.parentColumns,
        subTablesKeys: capture.subTables ? Object.keys(capture.subTables) : [],
        logs: logs.slice(0, 5)
      },
      null,
      2
    )
  );
}

function zzDescribeScenarioIngestion1018296SansBigQuery() {
  const formulaire = {
    nom: 'Scenario Mock 1018296',
    id: '1018296',
    tableName: 'mock_table_1018296',
    alias: 'mock_alias'
  };
  const fakeUnreadData = {
    status: 'ok',
    data: [
      {
        _id: 'record-001',
        summary: 'Mock summary'
      }
    ]
  };
  const detailedRecord = {
    form_id: '1018296',
    form_unique_id: 'FORM-001',
    id: 'record-001',
    user_id: 'user-1',
    last_name: 'Durand',
    first_name: 'Alex',
    answer_time: '2024-05-10T08:00:00Z',
    update_time: '2024-05-10T09:00:00Z',
    origin_answer: 'mobile',
    fields: {
      temperature_air: { type: 'number', value: 18.5 },
      commentaires: { type: 'text', value: 'RAS' }
    }
  };
  const callLog = [];
  const bigQueryCalls = {
    raw: 0,
    parent: 0,
    subTables: 0,
    media: 0
  };
  let unreadCallCount = 0;

  const stubFetch = (method, path) => {
    if (method === 'GET' && path.indexOf('/data/unread/') !== -1) {
      unreadCallCount += 1;
      if (unreadCallCount === 1) {
        return { data: fakeUnreadData, responseCode: 200 };
      }
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/data/record-001') !== -1) {
      return { data: { data: detailedRecord }, responseCode: 200 };
    }
    if (method === 'GET' && path.indexOf('/data/all') !== -1) {
      return { data: { status: 'ok', data: [] }, responseCode: 200 };
    }
    if (method === 'POST' && path.indexOf('/markasreadbyaction/') !== -1) {
      return { data: { status: 'ok' }, responseCode: 200 };
    }
    return { data: { status: 'unknown' }, responseCode: 200 };
  };

  const services = createIngestionServices({
    fetch: stubFetch,
    logger: {
      log: (message) => callLog.push(message)
    },
    now: () => new Date('2024-05-10T10:00:00Z'),
    bigQuery: {
      ingestRawBatch: () => {
        bigQueryCalls.raw += 1;
      },
      ingestParentBatch: () => {
        bigQueryCalls.parent += 1;
      },
      ingestSubTablesBatch: () => {
        bigQueryCalls.subTables += 1;
      },
      ingestMediaBatch: () => {
        bigQueryCalls.media += 1;
      }
    }
  });

  const medias = [];
  const apiPath = `/forms/${formulaire.id}/data/unread/test-action/1?includeupdated`;

  const result = handleResponses(
    { getId: () => 'MOCK-SPREADSHEET' },
    formulaire,
    apiPath,
    'test-action',
    medias,
    false,
    {
      services,
      targets: {
        bigQuery: false,
        externalLists: false,
        sheet: false
      },
      unreadPayload: fakeUnreadData
    }
  );

  Logger.log(
    JSON.stringify(
      {
        scenario: 'zzDescribeScenarioIngestion1018296SansBigQuery',
        result,
        mediasCount: medias.length,
        bigQueryCalls,
        logSample: callLog.slice(0, 5)
      },
      null,
      2
    )
  );

  return result;
}

function main_Test() {
  const spreadsheetBdD = SpreadsheetApp.openById("15F8a-5rU-4plaAPJuhyYC8-ndbEbWtqHw8RX94_C7cc");
  const onglets = spreadsheetBdD.getSheets();


  var etatExecution = "RaS pour le test";
  const nbFormulairesACharger=10;

  //action : limite la portée de l'action markasread et unread à un spreadSheet : Attention si plusieurs fichiers sheet portent le meme nom !!!
  let action='testMain'

    // Vérifie si l'exécution précédente est terminée (ou si c'est la première fois).
  if (etatExecution !== 'enCours') {
    // Marque cette exécution comme étant "en cours".
    try{
      for (const onglet of onglets) {
        const ongletName = onglet.getName();
        const ongletTabName = ongletName.split(' || ');
        const formulaire = {
          nom: ongletTabName[0],
          id: ongletTabName[1]
        };
        if (ongletTabName.length < 3 && ongletTabName.length > 1) {
          const lastRow=onglet.getLastRow()
          let reponseAPI =  requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?includeupdated`);
          if (!reponseAPI) {
            throw new Error('La réponse de requeteAPIDonnees est indéfinie');
          }
          let listeReponses = reponseAPI.data;
          if (!listeReponses || !listeReponses.status || listeReponses.status !== "ok") {
            console.log(`Erreur requeteAPIDonnees : statut ${listeReponses ? listeReponses.status : 'inconnu'}`);
          }
          if (listeReponses && listeReponses.data.length > 0) {
             processData(spreadsheetBdD, formulaire,action,nbFormulairesACharger);
          } else {
            Logger.log('Pas de nouveaux enregistrements');
          }
        }
      }

    } catch (error) {
       handleException('main', error);
    }
  }else {
    // Si l'exécution précédente est toujours en cours, vous pouvez choisir de ne rien faire
    // ou d'ajouter une logique spécifique pour gérer ce cas.
    console.log("L'exécution précédente est toujours en cours.");
    console.log("En cas de problème, veuillez réinitialiser l'onglet");
  }
}




function testRequetelistform (){
  let testBp
  let action="testDebug0"
  let nbFormulairesACharger=3
  let idFormulaire="710028"   //id du formulaire  
  //let idReponse="191428737"   //id de la réponse au formulaire idFormulaire    136860393
  //let dataReponse2=requeteAPIDonnees('GET', `/forms/${idFormulaire}/data/${idReponse}`)  //data de la réponse idReponse 

  var limites = {
    nbMaxTab: 3,
    listeObjAReduire: {
      "fields": 2,
      "options":4
    }
  };
  //let dataReponse=requeteAPIDonnees('GET', `/forms/1036551/data/unread/test/10?includeupdated&format=basic`)  //data de la réponse idReponse 
  let dataReponse=requeteAPIDonnees('GET', `/forms/${idFormulaire}/data/unread/${action}/${nbFormulairesACharger}`);
  let dataLigth=reduireJSON(dataReponse, limites)

  emailLogger(dataLigth)
  testBp
}


function testmainSheet(){
  let t0="0.1"
  let t1=isNumeric(t0)
  let t2=parseFloat(t0)

  const nbFormulairesACharger=10;
  const spreadsheetBdD = SpreadsheetApp.openById('1VeUH3ypEDqyTiSUwm_N3-Xqzo0LvHzijULYfklgKjTA')
  const action="test1"
  const ongletTabName = ['Test Jso/Aro Liste externe automatique (récupéré)','914817']; 
  const formulaire = {
    nom: ongletTabName[0],
    id: ongletTabName[1]
  };
  //processData(spreadsheetBdD, formulaire,action,nbFormulairesACharger)

  // Appliquer un format  à toute la feuille
  //let range = spreadsheetBdD.getDataRange();
  //range.setNumberFormat("#,###.##########");
  /*

  let startTime = new Date(); // Début de la mesure du temps
  let reponseAPI = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}`);
  let reponseAPI2 = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?format=basic`);
  let reponseAPI3 = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?includeupdated&format=basic`);
  let reponseAPI4 = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}?includeupdated&format=simple`);
  
  let timeTaken = new Date() - startTime; // Temps écoulé en millisecondes
  console.log('Temps écoulé: ' + timeTaken + ' ms');

  if (ongletTabName.length < 3 && ongletTabName.length > 1) {
    let reponseAPI = requeteAPIDonnees('GET', `/forms/${formulaire.id}/data/unread/${action}/${nbFormulairesACharger}`);
    if (!reponseAPI) {
      throw new Error('La réponse de requeteAPIDonnees est indéfinie');
    }
    let listeReponses = reponseAPI.data;
    if(!listeReponses.status){
      throw new Error(`Erreur requeteAPIDonnees`);
    }
    if(listeReponses.status!="ok"){
      throw new Error(`Erreur requeteAPIDonnees`);
    }
    if (listeReponses && listeReponses.data.length > 0) {
      processData(spreadsheetBdD, formulaire,action,nbFormulairesACharger);
    } else {
      Logger.log('Pas de nouveaux enregistrements');
    }
  }
  */
}


function testRequete2 (){
  let testBp

  let idFormulaire="710028"   //id du formulaire  
  let idReponse="191428737"   //id de la réponse au formulaire idFormulaire    136860393
  let dataReponse=requeteAPIDonnees('GET', `/forms/${idFormulaire}/data/${idReponse}`)  //data de la réponse idReponse 
  emailLogger(dataReponse.data)
  let testnum_ph1=dataReponse.data.data.fields.ph1.value
  let testnum_niveau_dynamique_au_moment_d1=dataReponse.data.data.fields.niveau_dynamique_au_moment_d1.value
  //niveau_dynamique_au_moment_d1
  let isNum_ph1=isNumeric(testnum_ph1)
  let float_ph1=parseFloat(testnum_ph1)
  let float_niveau_dynamique_au_moment_d=parseFloat(testnum_niveau_dynamique_au_moment_d1)
  let float_temperature_c_1=parseFloat(dataReponse.data.data.fields.temperature_c_1.value)
  //isNumeric(value) ? parseFloat(value) : value)
  let test=isNumeric(testnum_ph1) ? parseFloat(testnum_ph1) : testnum_ph1
  testBp
}

function testMainUI(){
  console.log('test')
  let formulaire={id:710028,nom:"Prélèvements - Piézomètres"};

  let dataEnCours={}
  majListeExterne(formulaire, dataEnCours) 

  
  let bp=1;
}



function testReinitUI(){
  reInitOngletActif()
}

function testEnregistrementUI(){
  let formulaire={id:962645 ,nom:"Soitec Pilote rev3", action:"TestAction"}   
  //  let formulaire={id:900548,nom:"Labo - MES"} 
  // let formulaire={id:893304,nom:"Labo - Mesures in-situ (pH, cond., redox, oxygène)"}   
  // let formulaire={id:856959,nom:"Tuto - 1er formulaire"}
  // requeteAPIDonnees('ettt', 'type') 
  console.log(formulaire)
  enregistrementUI(formulaire) 
}



function testPhoto(){
  //c75248f893304pu592427_20230705143825_31651f20-bc3a-484b-b264-8272d7f9f5d2
  let formulaire={id:893304,nom:"Labo - Mesures in-situ (pH, cond., redox, oxygène)"}   //165657529

  let mediaName='c75248f893304pu592427_20230705143825_31651f20-bc3a-484b-b264-8272d7f9f5d2'
  let idReponse=165657529
  let champEnCours="image1"
  let image=requeteAPIDonnees('GET',`/forms/${formulaire.id}/data/${idReponse}/medias/${mediaName}`) 
  
  if(image.responseCode==404){
    console.error('Photo non trouvée')
  }else{
    let nomImage=idReponse + "||"+champEnCours+"||"+mediaName+".jpg"
    gestionChampPhoto(image.data,nomImage)
  }
  
  let bp=1
}




function testRequete (){
  let testBp
  const spreadsheetBdD = SpreadsheetApp.openById("1pNbuRk7gweWpmOXrNxhJ8KLiqyvnnyv2J2e-Z-QGmpU");
  var action=spreadsheetBdD.getName()
  let listeFormulaires=requeteAPIDonnees(`/forms`)   //liste les formulaires
  let idFormulaire=710028   //id du formulaire  
  let listeReponses=requeteAPIDonnees(`/forms/${idFormulaire}/data/all`)   //liste des réponses au formulaire idFormulaire
  let listeReponses2=requeteAPIDonnees(`/forms/${idFormulaire}/data/unread/${action}/10`) 
  let idReponse=136861958   //id de la réponse au formulaire idFormulaire    136860393
  let dataReponse=requeteAPIDonnees(`/forms/${idFormulaire}/data/${idReponse}`)  //data de la réponse idReponse 

  testBp
}

function testNonLues(type) {
  let formulaires=requeteAPIDonnees('GET',`/forms`).data
  let listeReponses=requeteAPIDonnees('GET',`/forms/856959/data/readnew`).data //134908884 134937998
  let testBp
}

function testMarquerLu(type) {
  //let formulaires=requeteAPIDonnees('GET',`/forms`).data
  let listeReponses=requeteAPIDonnees('GET',`/forms/816148/data/readnew`).data
  //https://www.kizeoforms.com/rest/v3/forms/{formId}/markasread.
  let lues={
          "data_ids": [134937998]
            }
  let elementLus=requeteAPIDonnees('POST',`/forms/816148/markasread`,lues).data
  let testBp
}

function testMarquerLuAction(type) {
  //ne lit que les non lus par l'action XX et marque en lu par l'action XX 
  let action="testJSO"
  let listeReponses=requeteAPIDonnees('GET','/forms/816148/data/unread').data
  let listeReponsesAction=requeteAPIDonnees('GET',`/forms/816148/data/unread/`+action+"/100").data
  //https://www.kizeoforms.com/rest/v3/forms/{formId}/markasread.
  let lues={
          "data_ids": [135013804]
            }
  let elementLus=requeteAPIDonnees('POST',`/forms/816148/markasreadbyaction/`+action,lues).data
  let listeReponses2=requeteAPIDonnees('GET',`/forms/816148/data/unread/`+action+"/100").data
  let testBp
}



function testStoreDataInGoogleSheet(){
  const dataObject={}
  dataObject.id_response=987
  dataObject.values={test1:14,"GE1 Dernier Delta Nombre de demarrages":3,"GE3 Dernier Nombre de demarrages":5002,test4:8}
  const sheetName = "Calculs"; // Change this to your desired sheet name
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  storeDataInGoogleSheet(dataObject,sheet)

}

function storeDataInGoogleSheet(dataObject,sheet) {
  try {
    const headers = ["id_response", ...Object.keys(dataObject.values)];
    const values = [dataObject.id_response, ...Object.values(dataObject.values)];

    // Find the last row with data in the sheet
    const lastRow = sheet.getLastRow();

    // If the sheet is empty, add headers as the first row
    if (lastRow === 0) {
      sheet.appendRow(headers);
    }

    // Get the headers from the first row in the sheet
    const existingHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];

    // Find the column index for each value in the dataObject
    const columnIndices = values.map((value, index) => {
      const headerIndex = existingHeaders.indexOf(headers[index]);
      if (headerIndex === -1) {
        // If the header doesn't exist in the sheet, add it
        sheet.getRange(1, sheet.getLastColumn() + 1).setValue(headers[index]);
        return sheet.getLastColumn();
      }
      return headerIndex + 1;
    });

    // Append the values to the sheet
    const rowValues = [];
    for (let i = 0; i < values.length; i++) {
      rowValues[columnIndices[i] - 1] = values[i];
    }
    sheet.appendRow(rowValues);
    let testbp=10
  } catch (error) {
    Logger.log("Error: " + error);
  }
}



function testListeExterne(){
  let testBp
  configurerDeclencheurHoraire(5)
  let spreadsheetBdD = SpreadsheetApp.getActiveSpreadsheet();
  let methode='GET';
  let type= '/lists' ;
  let listeExterneTotales=requeteAPIDonnees(methode,type).data;
  /*
  let onglets = spreadsheetBdD.getSheets();
  // Parcourir tous les onglets et mettre à jour leurs données
  for (let i = 0; i < onglets.length; i++) {
    let onglet = onglets[i].getName();
    let nomOngletTab=onglet.split(" || ");
    let formulaire = {
      nom: nomOngletTab[0],
      id: nomOngletTab[1]
    };
    if(nomOngletTab.length<3 && nomOngletTab.length>1 ){
      let sheetEnCours = onglets[i];
      for(let i = 0; i < listeExterneTotales.lists.length; i++){
        let listeEnCours=listeExterneTotales.lists[i].name.split(" || ");
        let liste={
          nom:listeEnCours[0],
          idFormulaire:listeEnCours[1],
          idListe:listeExterneTotales.lists[i].id
        };
        if(formulaire.id==liste.idFormulaire){
          console.log("liste à traiter!");
          let finfichier=sheetEnCours.getLastRow()
          type= `/lists/${liste.idListe}`
          let detailListeExterne=requeteAPIDonnees(methode,type).data
          let variables=[]
          const splitArray = detailListeExterne.list.items[0].split('|');
          for(let i = 1; i < splitArray.length; i++){     //premiere valeur correspond au label      --------> regle à fixer
            let variable=splitArray[i].split(':');
            variables[i]=variable[0]
            testBp
          }
          testBp
        }

      }
    }
  }
  */
  let formulaire={id:900548,nom:"Labo - MES"}  

  type= `/lists/${386615}`   //386615
  
  let detailListeExterne=requeteAPIDonnees(methode,type).data
  let testfonctionreplace=replaceKizeoData(detailListeExterne.list.items, "GE2 - Compteur Heure de fonctionnement (heure)", 100)


  console.log(detailListeExterne.list.items)
  let split=detailListeExterne.list.items[0].split('|')
  let split1=detailListeExterne.list.items[1].split('|')
  let split1Length=split1[1].length
  let split1Length2=split1[2].length
  let split2=detailListeExterne.list.items[2].split('|')
  let split20=split2[0].split('\\')
  let split201=split20[1].split(':')
  let nouvelleChaine='CHANGEMENT CHARBON - CUVE3'
  split201.splice(0, 2, nouvelleChaine);
  split2[3]='50000:50010'
  let separator=":"
  split20=joinArrayWithSeparator(split201,separator)
  separator="|"
  let testJoin=joinArrayWithSeparator(split2,separator)
  detailListeExterne.list.items[2]=testJoin
  methode='PUT'
  let items={items:detailListeExterne.list.items}

  let testPUT=requeteAPIDonnees(methode,type,items).data
  
  testBp
}



function testajouteTrigger(){
  configurerDeclencheurHoraire(5)
}

function joinArrayWithSeparator(array,separator) {
  // Vérifier si le tableau est vide ou nul
  if (!array || array.length === 0) {
    return "";
  }
  // Utiliser la méthode Array.join() pour concaténer les éléments du tableau avec le séparateur "|"
  const joinedString = array.join(separator);
  return joinedString;
}
