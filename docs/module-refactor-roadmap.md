# Architecture Apps Script – Roadmap de modularisation

## 1. Constat actuel

- **Monolithique** : la majorité des fonctions vivent dans `lib/0_Data.js` et `lib/BigQuery.js`.
- **Couplage fort** : `processData` orchestre des responsabilités hétérogènes (récupération API, ingestion BQ, gestion médias, fallback Sheets, listes externes).
- **Tests** : la nouvelle suite `runAllTests` couvre de nombreux cas mais nécessite encore des mocks complexes faute de séparation claire.

## 2. Cible proposée

| Module | Fichier(s) suggérés | Responsabilité principale |
|--------|--------------------|----------------------------|
| `api` | `lib/KizeoClient.js`, `lib/APIHandler.js` (wrapper) | Communication Kizeo (HTTP, token, retries) |
| `ingest` | `lib/BigQuery.js`, `lib/0_Data.ingest*` | Préparation et écriture BigQuery (raw, parent, sous-formes, médias, audit) |
| `legacy` | `lib/SheetSnapshot.js`, `lib/Tableaux.js` (isolé) | Fonctions Sheets/Drive héritées (activation conditionnelle) |
| `media` | `lib/Images.js` | Gestion Drive des médias (downloading, stockage) |
| `orchestration` | `lib/ProcessManager.js` (nouveau) | `processData`, `handleResponses`, triggers, gestion des erreurs |
| `sync` | `lib/ListesExternes.js` | Mise à jour listes externes / autres systèmes |
| `external-lists` (script dédié) | `MAJ Listes Externes/*` | UI Google Sheets + exports Drive spécifiques aux listes externes (autonome, consomme `processData` et `buildRowSnapshot`). |

## 3. Étapes recommandées

1. **Isoler l’API Kizeo**
   - Déplacer `requeteAPIDonnees` + helpers token dans `api/KizeoClient.js`.
   - Exposer une interface (`fetch({ method, path, payload })`) testable indépendamment.

2. **Créer un module `ingest/BigQueryService`**
   - Rassembler `bqIngest*`, `bqEnsure*`, `bqRecordAudit`.
   - Injecter le service dans `processData` via `createIngestionServices()`.
   - Simplifier les tests en mockant uniquement ce service.

3. **Isoler `SheetSnapshot`** *(partiellement fait)*
   - `lib/SheetSnapshot.js` expose `prepareDataForSheet`, `buildRowSnapshot`, `persistSnapshot`, etc., et les fonctions globales sont désormais des wrappers.
   - Étapes restantes : retirer progressivement `lib/Tableaux.js`, déplacer la collecte médias Drive (si souhaité) et réduire les dépendances vers le code legacy.

4. **Séparer `processData`**
   - `fetchUnreadResponses(formulaire, action, services)` → module API.
   - `ingestResponses(formulaire, payload, services)` → module ingestion.
   - `finalizeRun(formulaire, context)` → module orchestration (mark-as-read, listes externes, logs).
   - Fournir un adaptateur explicite pour le projet externe « MAJ Listes Externes » afin qu’il consomme l’API sans dépendre du code legacy désactivé.

5. **Adapter les tests**
   - Créer des tests ciblés par module (ex. `api/KizeoClientTests.gs`, `SheetSnapshotTests.gs`).
   - Garder `runAllTests` comme agrégateur mais réduire le mocking à chaque cas.

## 4. Bénéfices attendus

- Lecture et maintenance facilitées (responsabilités mieux délimitées).
- Possibilité de réutiliser le client Kizeo ou le service BigQuery dans d’autres scripts.
- Simplification des tests : moins de mocks globaux, meilleure isolation.
- Décommission progressif du code legacy (module dédié, désactivation simple).

## 5. Prochaines actions

1. Introduire des fichiers dédiés (`lib/KizeoClient.js`, futurs `lib/BigQueryService.js`, etc.).
2. Déplacer `requeteAPIDonnees` + `createIngestionServices` vers ces nouvelles unités (sans changer la logique).
3. Mettre à jour `context-kizeo.md` avec le nouveau diagramme.
4. Ajuster les imports (`global` Apps Script) en exposant chaque module via `globalThis`.
