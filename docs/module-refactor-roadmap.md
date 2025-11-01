# Architecture Apps Script – Roadmap de modularisation

## 1. Constat actuel

- **Monolithique** : la majorité des fonctions vivent dans `lib/0_Data.js` et `lib/bigquery/ingestion.js`.
- **Couplage fort** : `processData` orchestre des responsabilités hétérogènes (récupération API, ingestion BQ, gestion médias, fallback Sheets, listes externes).
- **Tests** : la nouvelle suite `runAllTests` couvre de nombreux cas mais nécessite encore des mocks complexes faute de séparation claire.

## 2. Cible proposée

### Statut au 30/10/2025
- ✅ `api` : `lib/KizeoClient.js` opérationnel, accès direct via `requeteAPIDonnees` (plus de wrapper `APIHandler`).
- ✅ `ingest` : `lib/bigquery/ingestion.js` expose toutes les primitives (ensure/ingest/audit/dedup) consommées via `createIngestionServices()`.
- ✅ `media` : `lib/DriveMediaService.js` centralise le traitement Drive (processField, saveBlobToFolder, caches).
- ✅ `orchestration` : `lib/ProcessManager.js` regroupe `processData`, `handleResponses` et la collecte d’artefacts ; la préparation BigQuery et la construction du résultat sont désormais isolées via `ensureBigQueryForForm`, `prepareFormulaireForRun` et `buildProcessResult`.
- ✅ `sync` : `lib/ExternalListsService.js` reste autonome.
- ✅ `external-lists` : script “MAJ Listes Externes” documenté, consomme les services exposés (`processData`, `ExternalListsService`).

| Module | Fichier(s) suggérés | Responsabilité principale |
|--------|--------------------|----------------------------|
| `api` | `lib/KizeoClient.js` | Communication Kizeo (HTTP, token, retries) |
| `ingest` | `lib/backfill.js`, `lib/bigquery/ingestion.js`, `lib/process/*.js` | Préparation et écriture BigQuery (raw, parent, sous-formes, médias, audit) |
| `media` | `lib/DriveMediaService.js` | Gestion Drive des médias (downloading, stockage) |
| `orchestration` | `lib/ProcessManager.js` | `processData`, `handleResponses`, triggers, gestion des erreurs |
| `sync` | `lib/ExternalListsService.js` | Mise à jour listes externes / autres systèmes |
| `sheet-ui` | `lib/SheetInterfaceHelpers.js` | Mise en forme des onglets de configuration, notifications d’exécution, garde-fous BigQuery |
| `sheet-config` | `lib/SheetConfigHelpers.js` | Lecture/écriture/validation de la configuration Sheets, résolution du contexte formulaire |
| `external-lists` (script dédié) | `lib/ExternalListsService.js`, `MAJ Listes Externes/*` | Synchronisation listes Kizeo (`updateFromSnapshot`), UI Google Sheets + exports Drive spécifiques (autonome, consomme `processData` et `buildRowSnapshot`). |

## 3. Étapes recommandées

1. **Isoler l’API Kizeo** *(fait)*
   - `lib/KizeoClient.js` fournit désormais l’unique point d’accès HTTP (token cache, retries).

2. **Stabiliser l’ingestion BigQuery** *(fait)*
   - `createIngestionServices` renvoie un objet `bigQuery` bâti à partir de `lib/bigquery/ingestion.js` (plus de wrapper `BigQueryBindings`).

3. **Centraliser la gestion Drive** *(fait)*
   - `lib/DriveMediaService.js` remplace l’ancien couple `Images.js` / `SheetSnapshot`.

4. **Séparer `processData`**
   - ✅ Préparation BigQuery et agrégation des résultats déléguées à `ensureBigQueryForForm` / `buildProcessResult`.
   - ☐ Finaliser la découpe en modules indépendants (`fetchUnreadResponses`, `ingestResponses`, `finalizeRun`) pour alléger davantage `ProcessManager`.
   - ☐ Fournir un adaptateur explicite pour le projet externe « MAJ Listes Externes » afin qu’il consomme l’API sans dépendre du code legacy désactivé.

5. **Adapter les tests**
   - Créer des tests ciblés par module (ex. `api/KizeoClientTests.gs`, `SheetSnapshotTests.gs`).
   - Garder `runAllTests` comme agrégateur mais réduire le mocking à chaque cas.

## 4. Bénéfices attendus

- Lecture et maintenance facilitées (responsabilités mieux délimitées).
- Possibilité de réutiliser le client Kizeo ou le service BigQuery dans d’autres scripts.
- Simplification des tests : moins de mocks globaux, meilleure isolation.
- Décommission progressif du code legacy (module dédié, désactivation simple).

## 5. Prochaines actions

1. Continuer à documenter l’API publique (`processData`, `ExternalListsService`, `DriveMediaService`).
2. Poursuivre le découpage de `processData` (ingestion vs orchestration) pour faciliter les tests unitaires.
3. Mettre à jour `context-kizeo.md` avec le nouveau diagramme (sans legacy Sheets).
4. Ajuster les imports (`global` Apps Script) en exposant chaque module via `globalThis` si de nouvelles unités sont introduites.
