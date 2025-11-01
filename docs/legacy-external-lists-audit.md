# Audit – Listes externes & médias (octobre 2025)

## 1. Modules concernés

| Module | Fonctions clés | Consommateurs | Commentaires |
|--------|----------------|---------------|--------------|
| `lib/FormResponseSnapshot.js` | `buildRowSnapshot`, `extractFields` | `processData`, `ExternalListsService`, tests | Produit le snapshot mémoire (headers + ligne + métadonnées médias). Plus aucun lien avec l’ancienne persistance Sheets. |
| `lib/DriveMediaService.js` | `processField`, `saveBlobToFolder`, `normalizeDrivePublicUrl`, caches | `FormResponseSnapshot`, exports PDF/médias (`sheetInterface`, `MAJ Listes Externes`) | Service Drive unique (remplace `Images.js`). Fournit `DriveMediaService.getDefault()`. |
| `lib/ExternalListsService.js` | `updateFromSnapshot`, `replaceItems` | `ProcessManager.runExternalListsSync`, `MAJ Listes Externes` | Met à jour les listes Kizeo à partir du snapshot ; dépendances injectables (`fetch`, `log`, `handleException`). |

## 2. Flux actuel

1. `processData` récupère les réponses Kizeo, alimente BigQuery et construit un snapshot via `FormResponseSnapshot.buildRowSnapshot`.
2. `DriveMediaService.processField` télécharge les médias, crée les dossiers Drive et renvoie les formules/caches utilisés par les scripts (`exportPdfBlob`, exports médias).
3. `ExternalListsService.updateFromSnapshot` consomme ce snapshot pour mettre à jour les listes externes Kizeo (projet `MAJ Listes Externes` et orchestration quotidienne).

## 3. Points de vigilance

- Les scripts liés n’utilisent plus de wrappers `libKizeo.*` : vérifier que toute nouvelle fonctionnalité s’appuie sur les fonctions globales (`processData`, `ExternalListsService`, `DriveMediaService`).
- Conserver des scénarios manuels (`zzDescribeScenarioProcessManager`, `zzDescribeScenarioSheetInterface`, `zzDescribeScenarioMajListesExternes`) pour valider ingestion, médias Drive et listes externes après toute évolution.
- Documenter les procédures de rotation (token Kizeo, ScriptProperties BigQuery, droits Drive) dans `README.md` et `docs/test-runs.md`.
