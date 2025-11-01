# Legacy Sheets & Drive – État final (octobre 2025)

Tous les composants hérités liés à la persistance Google Sheets ont été retirés de la bibliothèque Apps Script.

## 1. Résumé

| Zone | Statut | Commentaire |
|------|--------|-------------|
| `lib/0_Data.js` | ✅ Nettoyé | Suppression des wrappers (`saveDataToSheet`, `prepareDataForSheet`, etc.) et de l’instrumentation (`recordLegacyUsage`, `logLegacyUsageStats`). `createIngestionServices` délègue directement à `FormResponseSnapshot` et `DriveMediaService`. |
| `lib/Tableaux.js` | ✅ Supprimé | Plus de persistance Sheets secondaire ; les sous-formulaires sont sérialisés via `FormResponseSnapshot`. |
| `lib/SheetSnapshot.js` | ✅ Supprimé | Fonctionnalités fusionnées dans `FormResponseSnapshot`. |
| `lib/Images.js` | ✅ Supprimé | Remplacé par `lib/DriveMediaService.js` (gestion Drive centralisée). |
| Alias `libKizeo` | ✅ Déprécié | Les scripts consommateurs appellent désormais les fonctions globales (`processData`, `requeteAPIDonnees`, `bqComputeTableName`, …) sans wrapper. |

## 2. Architecture actuelle

- Les snapshots (headers + row + médias + sous-formulaires) sont produits par `FormResponseSnapshot.buildRowSnapshot` et consommés par `processData` / `ExternalListsService`.
- `DriveMediaService.getDefault()` fournit `processField`, `saveBlobToFolder`, `normalizeDrivePublicUrl`, `getOrCreateFolder`, etc. – plus aucun wrapper `gestionChampImage`.
- `ProcessManager.collectResponseArtifacts` ne connaît plus de mode « legacy Sheets » : la sortie `lastSnapshot` provient directement de `FormResponseSnapshot`.
- `MAJ Listes Externes` et `sheetInterface` utilisent les mêmes primitives (`processData`, `runExternalListsSync`, `DriveMediaService`) sans alias `libKizeo`.

## 3. Actions restantes

- Surveiller ponctuellement les scénarios `zzDescribeScenario*` (ingestion, listes externes, exports Drive) et consigner les exécutions dans `docs/test-runs.md`.
- Poursuivre la documentation de l’API publique (`processData`, `ExternalListsService`, `DriveMediaService`) et des procédures (rotation token Kizeo, mise à jour ScriptProperties BigQuery).
- Supprimer, le cas échéant, les anciennes clés `LEGACY_USAGE_*` dans les `DocumentProperties` si elles existent encore dans les environnements historiques.

## 4. Notes pratiques

- **Exports Drive** : `DriveMediaService.saveBlobToFolder` réutilise les fichiers existants (déduplication par nom). En cas d’erreur, `handleException` journalise et les scénarios `zzDescribeScenario` permettent de reproduire.
- **Listes externes** : `ExternalListsService.updateFromSnapshot` reste la porte d’entrée, alimentée par le snapshot renvoyé par `processData` / `FormResponseSnapshot`.
- **BigQuery** : les tables parent/sous-formulaires/médias sont toujours préparées via `bqEnsure*`. La suppression du legacy Sheets n’affecte pas la structure BQ.
