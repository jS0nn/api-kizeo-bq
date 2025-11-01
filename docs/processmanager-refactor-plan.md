# Plan détaillé — Refactor `lib/ProcessManager.js`

## Objectifs
- Supprimer l’IIFE et l’alias `libKizeo` en exposant directement les fonctions nécessaires.
- Découper le fichier (>700 lignes) en responsabilités claires pour faciliter la maintenance et les tests.
- Préparer l’injection de dépendances (fetch, BigQuery, snapshot, log) sans couplage aux globals implicites.

## Découpage ciblé

| Module | Rôle actuel | Actions prévues |
|--------|-------------|-----------------|
| `processData` | Orchestration complète (lecture unread, collecte, ingestion, listes externes) | Conserver comme point d’entrée ; déléguer les sous-étapes à des fonctions importées. |
| `resolveUnreadDataset` | Gère unread/fallback | Extraire dans `lib/process/unread.js` (fonction pure utilisant `fetchFn`, `log`). |
| `collectResponseArtifacts` | Prépare contexte BQ + snapshots | Extraire dans `lib/process/collector.js`. Injecter `fetchDetailedRecord`, `snapshotService`. |
| `ingestBigQueryPayloads` | Délègue l’écriture BQ | Extraire dans `lib/process/ingest-bigquery.js` avec une API simple (`ingest(formulaire, context, services, log)`). |
| `runExternalListsSync` | Mise à jour listes externes | Déplacer dans `lib/process/external-lists.js`, dépendant de `ExternalListsService`. |
| `markResponsesAsRead` | Marque les données traitées | Laisser dans le module principal ou déplacer dans `lib/process/mark-read.js`. |
| Helpers (`resolveIsoTimestamp`, `pickMostRecentRecord`, `fetchDetailedRecord`) | Utilitaires réutilisables | Déplacer dans `lib/process/utils.js`. |

## Étapes concrètes
1. Créer un dossier `lib/process/` et y déplacer les sous-modules :
   - `unread.js`, `collector.js`, `bigquery.js`, `external-lists.js`, `utils.js`.
   - Exporter des fonctions pures sans dépendances globales.
2. Réécrire `lib/ProcessManager.js` en module principal :
   - Importer les sous-modules (Apps Script : via inclusion automatique, utiliser un simple objet `ProcessManager` interne).
   - Exposer les fonctions publiques (`processData`, `handleResponses`, `markResponsesAsRead`, `resolveBatchLimit`) via déclarations directes.
3. Supprimer les assignations `global.ProcessManager` et les duplications d’exports individuels (`global.resolveLogFunction = ...`).
4. Ajouter un commentaire de version mis à jour en tête du fichier.

## Tests à prévoir
1. **Scénario librairie** (`lib/zz_Tests.js`)
   - `zzDescribeScenarioProcessManager()` pour exécuter `processData` avec mocks : `fetchFn`, `bigQuery`, `snapshot`.
   - Assertions : nombre de lignes ingérées, appels `markResponsesAsRead`, statut de retour.
2. **Validation manuelle**
   - `clasp run processData` (ou wrapper spécifique) sur un formulaire de test : vérifier logs et absence d’erreurs.
   - Vérifier l’ingestion BigQuery réelle et la synchronisation listes externes sur un environnement de test.
3. **Analyse statique**
   - `rg "ProcessManager"` pour s’assurer que seules les fonctions nécessaires sont exposées.
   - `rg "libKizeo"` pour garantir la suppression de l’alias.

## Dépendances croisées à surveiller
- `lib/0_Data.js`, `lib/SheetSnapshot.js`, `lib/FormResponseSnapshot.js` : vérifier les appels à `ProcessManager`.
- Scripts liés (`sheetInterface`, `MAJ Listes Externes`) : planifier la migration vers les nouvelles fonctions sans alias.
- Documentation (`context-kizeo.md`, `README.md`, `docs/legacy-deprecation-plan.md`) : actualiser les références à `ProcessManager`.

## Risques & mitigations
- **Risque** : régression sur l’ingestion (BigQuery, listes) lors du découpage.
  - *Mitigation* : scénarios `zzDescribeScenario`, tests manuels `clasp run`, validation dans un environnement de test.
- **Risque** : dépendances externes non identifiées (calls via `global.*`).
  - *Mitigation* : inspection complète via `rg ProcessManager`, `rg handleResponses`, documentation mise à jour.
- **Risque** : oubli d’incrémenter les versions `// Version x.y.z`.
  - *Mitigation* : checklist par fichier modifié et revue finale avant `clasp push`.
