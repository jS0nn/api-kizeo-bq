# Audit legacy – Listes externes & médias (octobre 2025)

## Objectif

Cartographier le code legacy encore nécessaire pour la synchronisation des listes externes Kizeo et les exports médias, afin de planifier l’isolation ou la suppression progressive des helpers historiques.

## 1. Modules `lib/` impactés

| Module | Fonctions clés | Responsabilités | Consommateurs identifiés | Notes |
|--------|----------------|-----------------|--------------------------|-------|
| `lib/ExternalListsService.js` | `updateFromSnapshot`, `replaceItems`, helpers internes | Service central pour mettre à jour les listes Kizeo à partir du snapshot Apps Script. | `ProcessManager.runExternalListsSync`, `MAJ Listes Externes/*`, tests (`lib/zz_Tests`) | Injection des dépendances (`fetch`, `handleException`, `log`). Retourne `Mise A Jour OK` / `IGNORED` / `null`. |
| `lib/FormResponseSnapshot.js` | `buildRowSnapshot`, `prepareDataForSheet`, `extractFields` | Produit le snapshot mémoire (headers + ligne) utilisé par les listes externes et collecte les métadonnées Drive. | `ProcessManager.collectResponseArtifacts`, `ExternalListsService`, tests | Indépendant de la persistance Sheets ; n’appelle plus `lib/Tableaux`. |
| `lib/ListesExternes.js` | `majListeExterne` (wrapper historique) | Compatibilité globale : expose `majListeExterne` et `replaceKizeoData` en redirigeant vers `ExternalListsService`. | Scripts legacy qui importaient directement `majListeExterne`. | Permet une migration progressive sans casser l’API globale existante. |
| `lib/Images.js` | `gestionChampImage`, `saveBlobToFolder`, `getOrCreateFolder`, `buildMediaFolderName`, `buildDriveMediaUrls`, `normalizeDrivePublicUrl`, `getCachedDriveFolder`, `lookupFileIdInFolder`, `rememberFileInCache`, `findExistingFileInFolder` | Téléchargement Drive des médias Kizeo, mise en cache des dossiers/fichiers, génération de formules `HYPERLINK` vers les médias ou dossiers. | `SheetSnapshot.buildRowSnapshot`, `lib/Tableaux` (legacy subforms), exports médias/PDF (`sheetInterface/Code.js`), harness **MAJ Listes Externes/ZZ_tests.js** (`saveBlobToFolder`) | Crée des dossiers `Medias <formId> <Nom>` à côté du classeur. Utilise caches en mémoire (réinitialiser en tests). |
| `lib/Tableaux.js` | `gestionTableaux`, `getOrCreateSheet`, `getOrCreateHeaders`, `getNewHeaders`, `createNewRows`, `appendRowsToSheet`, `createHyperlinkToSheet` | Persistance historique des sous-formulaires dans des onglets Sheets (`Nom || ID || sousForm`) et génération de liens hypertexte. | `SheetSnapshot.prepareDataToRowFormat` (fallback legacy), `ProcessManager.collectResponseArtifacts` via `SheetSnapshot.persistSnapshot` | Chaque ligne appendée déclenche `gestionChampImage` pour les médias. Depuis oct. 2025, `SheetSnapshot` peut sérialiser le sous-formulaire en JSON (`tableaux_fallback_json`) si le module est absent, ce qui facilite sa mise hors service. |

### 1.1 Fonctions `lib/ListesExternes.js`

| Fonction | Description | Appelée par | Dépendances sortantes | Remarques |
|----------|-------------|-------------|-----------------------|-----------|
| `majListeExterne(formulaire, dataEnCours)` | Met à jour la liste externe correspondant au formulaire (nomenclature `Nom || formId`). | `ProcessManager.runExternalListsSync`, **MAJ Listes Externes/** | `requeteAPIDonnees`, `replaceKizeoData`, `handleException`, `console.log` | Retourne `'Mise A Jour OK'`, `'IGNORED'` ou `null`. Utilise le snapshot Kizeo pour remplacer les valeurs `tag:value`. |
| `replaceKizeoData(array, searchValue, replaceValue)` | Injecte une valeur dans le tableau `items` renvoyé par Kizeo (`search:value`). | `majListeExterne`, tests dans `lib/zz_Tests.js` | — | Ne gère que la ligne d’en-têtes (`items[0]`) et la première ligne de données (`items[1]`). |

### 1.2 Fonctions `lib/Images.js`

| Fonction | Description | Appelée par | Dépendances sortantes | Notes |
|----------|-------------|-------------|-----------------------|-------|
| `gestionChampImage(formId, dataId, fieldName, mediaIds, spreadsheet, options)` | Télécharge les médias d’une réponse, crée/obtient le dossier Drive, renvoie formule + métadonnées (files, folderId/url). | `SheetSnapshot.buildRowSnapshot`, `lib/Tableaux.createNewRows` (legacy) | `buildMediaFolderName`, `getOrCreateFolder`, `getCachedDriveFolder`, `lookupFileIdInFolder`, `saveBlobToFolder`, `buildDriveMediaUrls`, `requeteAPIDonnees`, `handleException` | Retourne `null` si le dossier ou le média est indisponible. |
| `saveBlobToFolder(blob, folderId, fileName, options)` | Sauvegarde un blob dans Drive avec déduplication par nom. | `gestionChampImage`, exports PDF/médias (`sheetInterface/Code.js`), **MAJ Listes Externes/ZZ_tests.js** | `getCachedDriveFolder`, `lookupFileIdInFolder`, `rememberFileInCache`, `DriveApp` | Retourne l’ID existant ou créé. |
| `getOrCreateFolder`, `buildMediaFolderName`, `buildDriveMediaUrls`, `normalizeDrivePublicUrl` | Helpers pour nommer dossiers et URLs publics. | `gestionChampImage`, `saveBlobToFolder` | `DriveApp` (pour `getOrCreateFolder`) | À extraire dans un service Drive dédié si migration. |
| `getCachedDriveFolder`, `getFolderFileCache`, `rememberFileInCache`, `lookupFileIdInFolder`, `findExistingFileInFolder` | Gestion du cache dossier/fichier pour limiter les requêtes Drive. | `gestionChampImage`, `saveBlobToFolder` | `DriveApp` | Les caches sont globaux au runtime (reset nécessaire en tests). |

### 1.3 Fonctions `lib/Tableaux.js`

| Fonction | Description | Appelée par | Dépendances sortantes | Notes |
|----------|-------------|-------------|-----------------------|-------|
| `gestionTableaux(spreadsheet, formulaire, idReponse, nomTableau, tableau)` | Crée/peuple un onglet `Nom || ID || sousForm` avec les lignes d’un sous-formulaire. | `SheetSnapshot.prepareDataToRowFormat`, `lib/Tableaux.createNewRows` | `getOrCreateSheet`, `getOrCreateHeaders`, `createNewRows`, `appendRowsToSheet`, `createHyperlinkToSheet` | Marquée `@deprecated` mais encore utilisée pour les environnements où les équipes consultent les sous-formulaires dans Sheets. |
| `getOrCreateSheet`, `getOrCreateHeaders`, `getNewHeaders` | Préparent la feuille et les en-têtes. | `gestionTableaux` | `SpreadsheetApp` | Ajoutent automatiquement des colonnes lorsque de nouveaux champs sont détectés. |
| `createNewRows`, `appendRowsToSheet`, `createHyperlinkToSheet` | Génèrent les valeurs, écrivent dans la feuille, retournent un lien. | `gestionTableaux` | `gestionChampImage`, `isNumeric`, `SpreadsheetApp` | `createNewRows` déclenche `gestionChampImage` pour les champs médias. |

### 1.4 Nettoyage en cours

- `ensureFormActionCode` et `upsertFormConfig` (anciens helpers Sheets) ont été retirés du code actif.
- Les harness historiques (`test_majListeExterne`, etc.) ont été déplacés vers `lib/zz_Tests.js` afin de clarifier la surface de production.
- Les prochaines purges viseront `lib/Tableaux.js` et la persistance Sheets dès que les équipes auront migré vers les rapports BigQuery/Looker.

## 2. Projet **MAJ Listes Externes/**

- Récupère la bibliothèque `libKizeo` et invoque directement :
  - `processData` (packages médias + snapshots pour alimenter ses exports),
  - `majListeExterne`,
  - utilitaires Drive (`saveBlobToFolder`) pour l’export PDF.
- Conserve une copie locale du socle UI (menus, triggers, config) similaire à `sheetInterface`.
- Ne consomme pas directement `gestionChampImage`, mais dépend du snapshot produit par la bibliothèque (médias déjà téléchargés à ce stade).

## 3. Feuilles Google Sheets encore actives

| Classeur | Onglets concernés | Usage actuel | Points de vigilance |
|----------|------------------|--------------|---------------------|
| Classeur principal d’ingestion | `Config`, `Suivi`, onglets `Nom || ID`, onglets `Nom || ID || <sous-formulaire>` | `Config` conserve l’action Kizeo (`action`), l’alias BigQuery et les horodatages (`last_*`). Les onglets legacy servent de vue de secours pour les équipes terrain. | La persistance legacy dépend de `SheetSnapshot` + `lib/Tableaux`. Prévoir un plan de purge/archivage une fois la migration Looker finalisée. |
| Classeur **MAJ Listes Externes** | `Config`, onglets auxiliaires pour exports (Drive/PDF) | Interface dédiée aux listes externes : sélection du formulaire, lancement d’exports, publication dans Kizeo. | Repose sur la compatibilité du `snapshot` retourné par `processData`. Toute évolution doit conserver `existingHeaders` / `rowEnCours`. |
| Classeur token Kizeo | `token` (A1) | Stockage manuel du token API. | Lecture ponctuelle lors du run (cache mémoire + `ScriptProperties`). Formaliser une procédure de rotation + audit d’accès. |

## 4. Flux legacy encore en service

1. **Ingestion + snapshot** : `processData` collecte les réponses, écrit dans BigQuery puis produit un snapshot mémoire (`FormResponseSnapshot`). Aucune écriture Sheets n’est effectuée.
2. **Listes externes** : `ProcessManager.runExternalListsSync` consomme ce snapshot pour mettre à jour Kizeo via `majListeExterne`. Les équipes peuvent ensuite exporter les listes depuis le projet **MAJ Listes Externes**.
3. **Exports médias** : `gestionChampImage` continue d’alimenter les dossiers Drive `Medias <formId>` ; les exports PDF/médias (`sheetInterface/Code.js`, **MAJ Listes Externes**) réutilisent `saveBlobToFolder`.

## 5. Audit BigQuery vs Sheets

| Étape | Dépendances | Observations |
|-------|-------------|--------------|
| Ingestion BigQuery (`processData` → `ingestBigQueryPayloads`) | `ProcessManager`, `lib/BigQuery.js` | Complètement indépendante des feuilles. BigQuery est l’unique destination des données de formulaire. |
| Snapshot mémoire (`collectResponseArtifacts`) | `SheetSnapshot.buildRowSnapshot` | Produit toujours `existingHeaders` / `rowEnCours`. Les listes externes s’appuient sur ce snapshot même sans écriture Sheets. |
| Persistance Sheets (`persistSnapshot`, `lib/Tableaux`) | Héritage consultatif | Désactivée en dur. Le module reste présent uniquement pour compatibilité historique en cas de besoin exceptionnel. |

## 6. Actions proposées

1. **Isoler le service listes externes** : extraire `buildRowSnapshot` + `majListeExterne` dans un module dédié (`ExternalListsService`) afin de découpler du reste de Sheet legacy.
2. **Plan de retrait Sheets** : enclencher une période d’observation (flag désactivé par défaut) puis prévoir la suppression de `lib/Tableaux.js` et des wrappers `@deprecated` (`saveDataToSheet`, etc.).
3. **Service Drive** : encapsuler `lib/Images` dans un module Drive distinct avec dépendances injectables pour les tests (UrlFetch/Drive mockés).
- **Documentation & tests** :
   - Mettre à jour `context-kizeo.md` avec le flux minimal (BigQuery ↔ Snapshots ↔ Listes).
   - Ajouter un test automatisé validant la mise à jour des listes externes en l’absence totale de persistance Sheets.
   - Continuer à réduire les harness manuels en `zzDescribeScenario()` côtés bibli et projets Sheets.
