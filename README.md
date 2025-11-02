# Kizeo → Sheets → BigQuery

## Vue d’ensemble
- Automatisation Apps Script divisée en deux projets : une bibliothèque (`lib/`) et un script lié à un classeur (`sheetInterface/`). Les fonctions de la bibliothèque sont exposées directement dans l’espace global (`processData`, `requeteAPIDonnees`, `bqComputeTableName`, …). L’objet figé retourné par `getLibPublicApi()` (défini dans `lib/zz_PublicApi.js`) permet d’inspecter cette surface et remplace les anciens exports `this.*`.
- Les manifests (`sheetInterface/appsscript.json`, `MAJ Listes Externes/appsscript.json`) référencent la bibliothèque sous le symbole `libKizeo` uniquement pour la charger ; le code applicatif invoque désormais les fonctions globales sans alias supplémentaire.
- Ingestion des formulaires Kizeo, synchronisation des sous-formulaires et export des médias vers Drive.
- Déduplication BigQuery déclenchée automatiquement et disponible à la demande depuis le menu du classeur.
- Documentation d’architecture détaillée dans `context-kizeo.md`.

## Pré-requis
- `npm install -g @google/clasp` puis `clasp login` avec le compte Google associé aux scripts.
- Accès aux projets Apps Script sous-jacents (librairie + script lié).
- ScriptProperties `BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_LOCATION` configurées côté Apps Script.
- Secrets (token Kizeo, IDs de formulaires, etc.) stockés hors dépôt.

### Gestion des secrets
- **Token Kizeo**
  1. Ouvrir le classeur dédié au stockage du token (`token`!A1) et récupérer un nouveau jeton depuis Kizeo.
  2. Remplacer la valeur de la cellule `A1`, puis enregistrer le classeur.
  3. Depuis l’éditeur Apps Script de la librairie (`lib/`), exécuter `cacheResetKizeoToken()` (ou `clasp run cacheResetKizeoToken`) pour invalider le cache en mémoire.
  4. Lancer `zzDescribeScenarioProcessManager` ou `main` afin de vérifier que l’authentification se fait avec le nouveau token.
  5. Consigner la rotation dans la documentation interne / coffre-fort d’équipe.
- **ScriptProperties BigQuery (`BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_LOCATION`)**
  1. Dans Apps Script → **Project Settings**, mettre à jour les propriétés avec les valeurs cibles.
  2. Option alternative : exécuter `setBigQueryConfig({ projectId, datasetId, location })` via `clasp run`.
  3. Utiliser la fonction `initBigQueryConfigFromSheet` (menu Configuration Kizeo) pour propager et contrôler les valeurs depuis la feuille.
  4. Vérifier immédiatement la configuration avec `zzDescribeScenarioSheetInterface` ou `runBigQueryDeduplication`.
  5. Documenter toute déviation (ex. dataset temporaire) dans l’onglet `Config` du classeur et dans `docs/test-runs.md`.

## Organisation du dépôt
```
.
├── lib/                # Bibliothèque Apps Script (fonctions exposées globalement)
│   ├── 0_Data.js            # Référentiel des symboles publics + constantes communes
│   ├── backfill.js          # Backfill BigQuery (lecture data/all, ingestion raw/parent)
│   ├── bigquery/ingestion.js   # Ingestion, audit et déduplication BigQuery
│   ├── process/             # Orchestration (collecte, unread, external lists…)
│   ├── KizeoClient.js       # Client HTTP Kizeo (cache token, gestion erreurs)
│   ├── DriveMediaService.js # Téléchargement médias Drive + caches
│   ├── ExternalListsService.js # Synchronisation des listes externes Kizeo
│   ├── SheetInterfaceHelpers.js # Helpers communs aux scripts Sheets (formatage, notifications)
│   ├── SheetConfigHelpers.js    # Lecture/validation/écriture de la feuille Config
│   ├── SheetDriveExports.js     # Export Drive (PDF, médias) partagé par les scripts Sheets
│   ├── Outils.js            # Config formulaire, helpers feuille principale
│   ├── zz_Tests.js          # Scénarios exploratoires ou de test manuel
│   └── appsscript.json      # Manifest Apps Script de la librairie
├── sheetInterface/     # Script lié au classeur et assets HtmlService
│   ├── Code.js         # Menus, triggers, délégation directe vers `libKizeo.*`
│   ├── UI.js           # Logique UI (modales, sélection formulaire)
│   ├── outils.js       # Utilitaires communs côté sheet
│   ├── timeIntervalSelector.html # Dialogue de fréquence
│   └── ZZ_tests.js     # Harness de tests manuels côté sheet
├── docs/               # Notes complémentaires
├── context-kizeo.md    # Décisions d’architecture, inventaires et plan BQ
└── TASKS.md / AGENTS.md # Notes de suivi interne
```

## Flux de travail
### Fonctions publiques principales
Les principaux points d’entrée exposés dans l’espace global Apps Script sont :

| Fonction | Localisation | Description |
|----------|-------------|-------------|
| `processData(spreadsheet, formulaire, action, limit, options)` | `lib/ProcessManager.js` | Récupère les réponses Kizeo non lues, ingère BigQuery, synchronise les listes externes et renvoie un résumé (médias, dernier enregistrement, statistiques). |
| `handleResponses(...)` | `lib/ProcessManager.js` | Variante bas niveau utilisée par `processData` et les scénarios tests. |
| `requeteAPIDonnees(method, path, payload)` | `lib/KizeoClient.js` | Wrapper UrlFetch avec cache token et gestion des erreurs structurée. |
| `ensureBigQueryCoreTables()` / `bqRunDeduplicationForForm(formulaire)` | `lib/bigquery/ingestion.js` | Prépare les tables cibles et pilote la déduplication. |
| `FormResponseSnapshot.buildRowSnapshot(...)` | `lib/FormResponseSnapshot.js` | Produit le snapshot utilisé par l’ingestion et les listes externes (headers + ligne + médias). |
| `ExternalListsService.updateFromSnapshot(...)` | `lib/ExternalListsService.js` | Met à jour les listes externes Kizeo à partir d’un snapshot. |
| `DriveMediaService.getDefault().saveBlobToFolder(blob, folderId, fileName)` | `lib/DriveMediaService.js` | Sauvegarde un export ou média dans Drive avec gestion de la duplication. |

Les scripts liés (`sheetInterface/`, `MAJ Listes Externes/`) appellent directement ces fonctions globales. Pensez à `clasp push` la librairie avant d’exécuter les scripts consommateurs.

### Bibliothèque (`lib/`)
1. `cd lib`
2. `clasp pull` pour récupérer l’état distant.
3. Développer suivant les conventions : indentation 2 espaces, simples quotes, camelCase.
4. `clasp push` pour mettre à jour le projet Apps Script.
5. Déclencher le batch BigQuery via `clasp run bqIngestParentBatch` ou le scénario ad hoc (`zzDescribeScenario`).

### Script lié (`sheetInterface/`)
1. `cd sheetInterface`
2. `clasp pull` / modifications / `clasp push`.
3. Déploiement manuel : ouvrir le classeur cible → menu **Configuration Kizeo**.
4. Entrées disponibles : sélection du formulaire, actualisation BigQuery, déduplication forcée, configuration des déclencheurs.

## Déduplication BigQuery
- `lib/bigquery/ingestion.js` fournit `bqRunDeduplicationForForm` qui nettoie table parent + sous-tables (restitution des stats et des erreurs).
- Côté sheet, `runBigQueryDeduplication` vérifie l’état du script (`etatExecution`), valide la configuration, puis délègue à la bibliothèque.
- `ensureDeduplicationTrigger` crée un déclencheur horaire dédié (`runBigQueryDeduplication`) et cohabite avec le déclencheur principal `main`.
- Menu “Forcer la déduplication BigQuery” permet un lancement manuel avec feedback détaillé.

## Backfill BigQuery
- `bqBackfillForm(formId, options)` exécute un backfill direct depuis Kizeo vers BigQuery sans passer par les feuilles :
  - Lecture complète `data/all`, filtrage optionnel par `startDate`, `endDate`, `limit`.
  - Ingestion `raw` + tables parent & sous-formulaires via les mêmes helpers que le run quotidien.
  - Option `chunkSize` (par défaut 25) pour piloter la taille des lots ; `includeMedia: true` possible si un classeur cible est fourni (`spreadsheetId`) pour réutiliser la logique Drive.
- Exemple : `clasp run bqBackfillForm --params '["1018296", {"startDate":"2024-01-01","endDate":"2024-01-31","chunkSize":50}]'`
- Les scénarios manuels sont documentés dans `lib/zz_Tests.js` (`zzDescribeScenarioBackfillMinimal`).

## Tests

### Tests automatisés
- `node tests/run-tests.js` (vérifie les intégrations clés : `FormResponseSnapshot`, `createIngestionServices`, `collectResponseArtifacts`).

### Tests manuels
- Utiliser `zz_Tests.js` ou `sheetInterface/ZZ_tests.js` pour ajouter des scénarios `zzDescribeScenario()`.
- Exécution distante via `clasp run zzDescribeScenario` après mise à jour (`clasp push`).
- Vérifications attendues : écriture BigQuery (`BigQuery.Tables.list`), mutations Sheets et exports Drive.
- Trois scénarios sont à compléter pour valider la refonte : `zzDescribeScenarioProcessManager`, `zzDescribeScenarioSheetInterface`, `zzDescribeScenarioMajListesExternes`.

## Dépannage
- **Script bloqué (`etatExecution = 'enCours'`)** : exécuter la fonction `setScriptPropertiesTermine()` depuis l’éditeur Apps Script ou lancer `clasp run setScriptPropertiesTermine`. Elle délègue à `setScriptProperties('termine')` et libère le verrou manuel sans toucher aux autres propriétés.
- **Journalisation d’une réponse volumineuse** : utiliser `emailLogger()` (exposé dans `lib/zz_Tests.js` pour les scénarios manuels) ou, pour réduire le payload, les helpers `reduireJSON*` du même fichier.
- **Inspection ponctuelle d’une ingestion** : privilégier les fonctions `zzDescribeScenario()` dans `zz_Tests.js` / `sheetInterface/ZZ_tests.js` afin d’isoler le contexte plutôt que de modifier la librairie.

## Bonnes pratiques de commit & PR
- Messages courts, impératif, scope explicite (`lib: refresh dedupe helpers`).
- Documenter dans la PR : problème, solution, vérifications (`clasp push/pull`, jobs manuels) et impacts ScriptProperties / triggers.
- Conserver les secrets hors dépôt, préciser les overrides éventuels dans les onglets de configuration.

## Ressources utiles
- [Apps Script + clasp](https://developers.google.com/apps-script/guides/clasp)
- [Kizeo Forms API](https://www.kizeoforms.com/fr/api-rest-version-3/)
- [BigQuery Tables API](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
