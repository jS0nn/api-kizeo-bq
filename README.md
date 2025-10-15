# Kizeo → Sheets → BigQuery

## Vue d’ensemble
- Automatisation Apps Script divisée en deux projets : une bibliothèque (`lib/`) et un script lié à un classeur (`sheetInterface/`).
- Ingestion des formulaires Kizeo, synchronisation des sous-formulaires et export des médias vers Drive.
- Déduplication BigQuery déclenchée automatiquement et disponible à la demande depuis le menu du classeur.
- Documentation d’architecture détaillée dans `context-kizeo.md`.

## Pré-requis
- `npm install -g @google/clasp` puis `clasp login` avec le compte Google associé aux scripts.
- Accès aux projets Apps Script sous-jacents (librairie + script lié).
- ScriptProperties `BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_LOCATION` configurées côté Apps Script.
- Secrets (token Kizeo, IDs de formulaires, etc.) stockés hors dépôt.

## Organisation du dépôt
```
.
├── lib/                # Bibliothèque Apps Script (libKizeo)
│   ├── BigQuery.js     # Ingestion, audit et déduplication BigQuery
│   ├── APIHandler.js   # Appels Kizeo (UrlFetch)
│   ├── GestionDonneesMaJ.js # Synchronisation feuilles Google Sheets
│   └── zz_*.js         # Scénarios exploratoires ou de test manuel
├── sheetInterface/     # Script lié au classeur et assets HtmlService
│   ├── Code.js         # Menus, triggers, orchestrateur `main`
│   ├── UI.js           # Logique UI (modales, sélection formulaire)
│   ├── outils.js       # Utilitaires communs côté sheet
│   ├── timeIntervalSelector.html # Dialogue de fréquence
│   └── ZZ_tests.js     # Harness de tests manuels côté sheet
├── docs/               # Notes complémentaires
├── context-kizeo.md    # Décisions d’architecture, inventaires et plan BQ
└── TASKS.md / AGENTS.md # Notes de suivi interne
```

## Flux de travail
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
- `lib/BigQuery.js` fournit `bqRunDeduplicationForForm` qui nettoie table parent + sous-tables (restitution des stats et des erreurs).
- Côté sheet, `runBigQueryDeduplication` vérifie l’état du script (`etatExecution`), valide la configuration, puis délègue à la bibliothèque.
- `ensureDeduplicationTrigger` crée un déclencheur horaire dédié (`runBigQueryDeduplication`) et cohabite avec le déclencheur principal `main`.
- Menu “Forcer la déduplication BigQuery” permet un lancement manuel avec feedback détaillé.

## Tests manuels
- Aucun runner automatique : utiliser `zz_Tests.js` ou `sheetInterface/ZZ_tests.js` pour ajouter des scénarios `zzDescribeScenario()`.
- Exécution distante via `clasp run zzDescribeScenario` après mise à jour (`clasp push`).
- Vérifications attendues : écriture BigQuery (`BigQuery.Tables.list`), mutations Sheets et exports Drive.

## Bonnes pratiques de commit & PR
- Messages courts, impératif, scope explicite (`lib: refresh dedupe helpers`).
- Documenter dans la PR : problème, solution, vérifications (`clasp push/pull`, jobs manuels) et impacts ScriptProperties / triggers.
- Conserver les secrets hors dépôt, préciser les overrides éventuels dans les onglets de configuration.

## Ressources utiles
- [Apps Script + clasp](https://developers.google.com/apps-script/guides/clasp)
- [Kizeo Forms API](https://www.kizeoforms.com/fr/api-rest-version-3/)
- [BigQuery Tables API](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
