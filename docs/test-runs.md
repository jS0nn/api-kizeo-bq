# Journal exécutions manuelles

> Consigner ici chaque campagne de test (date, scénario exécuté, résultat, observations).
>
> Format suggéré :
> ```
> ## AAAA-MM-JJ — Contexte
> - Fonctions : `zzDescribeScenarioProcessManager`, `zzDescribeScenarioSheetInterface`, ...
> - Résultat : ✅ / ⚠️ (détails)
> - Observations : logs BigQuery, Drive, erreurs éventuelles
> ```

## 2025-10-30 — Scénarios de validation post-refactor

- `zzDescribeScenarioProcessManager`  
  - Résultat : ✅ `{"status":"INGESTED","metadataUpdateStatus":"SKIPPED","rowCount":1,"rawRowsCaptured":1,"parentRowsCaptured":1,"mediaCaptured":0,"subTablesCaptured":0,"markAsReadCalls":1,"logSamples":[...]}`  
  - Observations : ingestion complète avec mocks, marquage réussi.

- `zzDescribeScenarioSheetInterface`  
  - Résultat : ✅ `{"isValid":true,"tableName":"formulaire_sc_nario_form_scenario","batchLimit":15,"availability":true}`  
  - Observations : validation de configuration OK avec stub `bqComputeTableName`.

- `zzDescribeScenarioMajListesExternes`  
  - Résultat : ✅ `{"status":"Mise A Jour OK","putCalls":1,"samplePayload":{"items":["id:id|champ:champ","rec-000:rec-000|Valeur mise à jour:Valeur mise à jour"]}}`  
  - Observations : scénario exécuté via l’éditeur Apps Script, `ExternalListsService.updateFromSnapshot` appelé directement (plus d’alias `libKizeo`).

## 2025-10-30 — Validation post-push (suppression legacy Sheets)
- Fonctions : `zzDescribeScenarioProcessManager`, `zzDescribeScenarioSheetInterface`, `zzDescribeScenarioMajListesExternes`.
- Résultat :
  - ✅ `zzDescribeScenarioProcessManager` — `{"status":"INGESTED","metadataUpdateStatus":"SKIPPED","rowCount":1,"rawRowsCaptured":1,"parentRowsCaptured":1,"mediaCaptured":0,"subTablesCaptured":0,"markAsReadCalls":1,...}`
  - ✅ `zzDescribeScenarioSheetInterface` — `{"isValid":true,"tableName":"form_scenario__form_scenario","batchLimit":15,"availability":true}`
  - ✅ `zzDescribeScenarioMajListesExternes` — `{"status":"Mise A Jour OK","putCalls":1,"samplePayload":{"items":["id:id|champ:champ","rec-000:rec-000|Valeur mise à jour:Valeur mise à jour"]}}`
- Observations :
  - Logs ProcessManager sans références legacy ; sérialisation JSON dérivée de `FormResponseSnapshot`.
  - Nom de table BigQuery `form_scenario__form_scenario` validé dans le scénario SheetInterface.
  - Listes externes : payload identique, pas d’erreur `ExternalListsService`.

## 2025-10-31 — Harmonisation appels libKizeo
- Fonctions : `node tests/run-tests.js`.
- Résultat : ✅ `Tous les tests sont passés.` (vérifie `DriveMediaService`, `createIngestionServices`, `collectResponseArtifacts` sans dépendance legacy).
- Observations : scripts `sheetInterface` et `MAJ Listes Externes` appellent désormais la librairie via `libKizeo.*` ; suppression des alias globaux `00_libGlobals.js`.

## 2025-11-01 — Helpers config/Drive mutualisés
- Fonctions : `node tests/run-tests.js`, `zzDescribeScenarioSheetInterface`, `zzDescribeScenarioSyncExternalLists`.
- Résultat :
  - ✅ `node tests/run-tests.js` — nouvelles couvertures (`SheetInterfaceHelpers`, `SheetConfigHelpers`, `SheetDriveExports`, scénarios `processData`).
  - ✅ `zzDescribeScenarioSheetInterface` — `{"isValid":true,"tableName":"formulaire_sc_nario_form_scenario","batchLimit":15,"availability":true}` (utilise `SheetConfigHelpers`).
- ✅ `zzDescribeScenarioSyncExternalLists` — `{"configValid":true,"validationErrors":[],"external":{"status":"Mise A Jour OK","putCalls":1,"samplePayload":{"items":["id:id|champ:champ","rec-000:rec-000|Valeur mise à jour:Valeur mise à jour"]}}}`.
- Observations : `lib/SheetConfigHelpers.js` + `lib/SheetDriveExports.js` alimentent les deux scripts Sheets (config + exports Drive) ; plus de duplication locale et notifications/verrous alignés.

## 2025-11-02 — API publique gelée
- Fonctions : `node tests/run-tests.js`.
- Résultat : ✅ `Tous les tests sont passés.` (nouvelle couverture `getLibPublicApi fige l’API exposée`).
- Observations : `lib/zz_PublicApi.js` construit l’objet figé à partir de `LIB_PUBLIC_SYMBOLS`; suppression des exports `this.*` et extraction des helpers sous-formulaire vers `lib/process/subforms.js`.
