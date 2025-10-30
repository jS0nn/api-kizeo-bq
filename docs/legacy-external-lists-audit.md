# Audit legacy – Listes externes & médias (octobre 2025)

## Objectif

Cartographier le code legacy encore nécessaire pour la synchronisation des listes externes Kizeo et les exports médias, afin de planifier l’isolation ou la suppression progressive des helpers historiques.

## 1. Modules `lib/` impactés

| Module | Fonctions clés | Responsabilités | Consommateurs identifiés | Notes |
|--------|----------------|-----------------|--------------------------|-------|
| `lib/ListesExternes.js` | `majListeExterne`, `replaceKizeoData` | Lecture/écriture des listes externes Kizeo (`/lists`). Conversion d’un snapshot (`existingHeaders`, `rowEnCours`) en payload PUT. | `ProcessManager.runExternalListsSync` (bibliothèque), projet **MAJ Listes Externes/** via `libKizeo` | Dépend de `requeteAPIDonnees` et `handleException`. Le harness `test_majListeExterne` a été déplacé dans `lib/zz_Tests.js`. |
| `lib/Images.js` | `gestionChampImage`, `saveBlobToFolder`, `getOrCreateFolder`, `buildMediaFolderName`, `buildDriveMediaUrls`, `normalizeDrivePublicUrl`, `getCachedDriveFolder`, `lookupFileIdInFolder`, `rememberFileInCache`, `findExistingFileInFolder` | Téléchargement Drive des médias Kizeo, mise en cache des dossiers/fichiers, génération de formules `HYPERLINK` vers les médias ou dossiers. | `SheetSnapshot.buildRowSnapshot`, `lib/Tableaux` (legacy subforms), exports médias/PDF (`sheetInterface/Code.js`), harness **MAJ Listes Externes/ZZ_tests.js** (`saveBlobToFolder`) | Crée des dossiers `Medias <formId> <Nom>` à côté du classeur. Utilise caches en mémoire (réinitialiser en tests). |
| `lib/Tableaux.js` | `gestionTableaux`, `getOrCreateSheet`, `getOrCreateHeaders`, `getNewHeaders`, `createNewRows`, `appendRowsToSheet`, `createHyperlinkToSheet` | Persistance historique des sous-formulaires dans des onglets Sheets (`Nom || ID || sousForm`) et génération de liens hypertexte. | `SheetSnapshot.prepareDataToRowFormat` (fallback legacy), `ProcessManager.collectResponseArtifacts` via `SheetSnapshot.persistSnapshot` | Chaque ligne appendée déclenche `gestionChampImage` pour les médias. Les onglets restent consultés par les équipes terrain ; aucune automatisation de purge n’est en place. |

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

1. **Ingestion + snapshot** : `processData` collecte les réponses, écrit dans BigQuery puis, si l’option Sheets est active, persiste un snapshot (`SheetSnapshot` → `lib/Tableaux` + `lib/Images`).
2. **Listes externes** : `ProcessManager.runExternalListsSync` consomme ce snapshot pour mettre à jour Kizeo via `majListeExterne`. Les équipes peuvent ensuite exporter les listes depuis le projet **MAJ Listes Externes**.
3. **Exports médias** : `gestionChampImage` continue d’alimenter les dossiers Drive `Medias <formId>` ; les exports PDF/médias (`sheetInterface/Code.js`, **MAJ Listes Externes**) réutilisent `saveBlobToFolder`.

## 5. Actions proposées

1. **Inventaire détaillé** : lister, dans un tableau, chaque helper de `lib/ListesExternes` / `lib/Images` avec :
   - statut (prod, legacy, test uniquement),
   - point(s) d’entrée (`ProcessManager`, `SheetSnapshot`, projet MAJ),
   - dépendances (Drive, UrlFetch),
   - plan d’isolation (module dédié `legacyExternalLists` ?).
2. **Projet MAJ Listes Externes** :
   - Identifier les fonctions réellement utilisées du bundle `lib` (notamment exports et BigQuery) pour anticiper la migration vers les services refactorés.
   - Documenter comment `processData` est injecté et quels champs du snapshot sont nécessaires (médias, latestRow).
3. **Refactor progressif** :
   - Encapsuler `majListeExterne` dans un mini-service (`ExternalListsService`) pour séparer l’API Kizeo des accès Sheets.
   - Déplacer les helpers Drive vers un module partagé (`DriveMediaService`) avec dépendances injectables (mockables).
   - Conserver les wrappers legacy (`saveDataToSheet`, `gestionTableaux`) uniquement côté bibliothèque, en les marquant `@deprecated`.

## 6. Prochaines étapes

- Valider le plan d’inventaire (responsable + échéance) et renseigner le tableau d’usage.
- Décider si le projet **MAJ Listes Externes/** reste autonome ou doit consommer un module partagé (maîtrise des variations UI vs. bibli).
- Prioriser l’extraction d’un service API/Drive commun pour faciliter les tests (UrlFetch/Drive mocked).
- Mettre à jour `docs/tasks-apps-script-refacto.md` en conséquence lorsque l’inventaire sera terminé.
