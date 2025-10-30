# Legacy Sheets & Drive – Plan de décommissionnement

## 1. État des lieux

| Zone | Fonctions marquées `@deprecated` | Utilisation actuelle (octobre 2025) |
|------|----------------------------------|-------------------------------------|
| `lib/0_Data.js` | `saveDataToSheet`, `prepareDataForSheet`, `prepareDataToRowFormat`, `prepareSheet`, `getColumnIndices` | - `saveDataToSheet` n’est plus appellée.<br>- `prepareDataForSheet` reste utilisée par `buildRowSnapshot` pour produire un résumé structuré (métadonnées + médias) avant la synchro listes externes. |
| `lib/Tableaux.js` | `gestionTableaux`, `getOrCreateSheet`, `getOrCreateHeaders`, `getNewHeaders`, `createNewRows`, `appendRowsToSheet`, `createHyperlinkToSheet` | Plus aucun appel direct : ces utilitaires ne sont plus référencés depuis le flux principal (ils étaient déclenchés via `saveDataToSheet`). |
| `lib/Images.js` | Code principal non déprécié mais fortement couplé à l’ancien flux Sheets | Toujours invoqué depuis `buildRowSnapshot` pour conserver les médias sur Drive. |

Instrumentation ajoutée :
- `recordLegacyUsage()` marque chaque passage dans une fonction historique (`LEGACY_USAGE_*` dans les `DocumentProperties`).
- `logLegacyUsageStats()` (Apps Script global) affiche les temps de dernier passage et le nombre total de marqueurs.

## 2. Observations

1. **`saveDataToSheet` & `lib/Tableaux.js`** : l’instrumentation confirme l’absence d’appels depuis l’activation du flag `ENABLE_LEGACY_SHEETS_SYNC`. Ces fonctions sont donc candidates à la suppression pure et simple une fois la période de monitoring terminée.
2. **`prepareDataForSheet` & co.** : toujours nécessaires pour construire les snapshots destinés à :
   - l’update des listes externes (`runExternalListsSync`),
   - la collecte des médias Drive (`buildRowSnapshot`),
   - les rapports UI (ex. export manuel).
3. **`lib/Images.js`** : la logique Drive est volontaire. BigQuery ne stocke que les métadonnées (`drive_file_id`, URLs, dossier). La suppression du traitement Drive n’est pas souhaitée : les utilisateurs consomment toujours les médias via Drive/Looker.

## 3. Plan de retrait

### Étape 1 — Observation (en cours)
1. Déployer le script et laisser tourner les déclencheurs de production pendant ≥ 2 semaines.
2. Exécuter ponctuellement `logLegacyUsageStats()` (via `clasp run logLegacyUsageStats` ou l’éditeur Apps Script) pour vérifier l’absence d’entrées récentes :
   ```bash
   clasp run logLegacyUsageStats
   ```
   - Retour `0` => aucune fonction legacy utilisée depuis la dernière purge.
   - Retour `>0` => investiguer les usages résiduels (vérifier les `DocumentProperties` ou les logs).

### Étape 2 — Isolation
1. Déplacer les fonctions encore utiles (`prepareDataForSheet`, `getDataFromFields`, etc.) dans un module dédié (`lib/SheetSnapshot.js`). *(fait)*
2. Extraire `buildRowSnapshot` pour qu’il dépende uniquement de ce module, en supprimant les références directes à `saveDataToSheet`.
3. Marquer le module d’un flag `ENABLE_LEGACY_SNAPSHOT` si besoin (permet de désactiver entièrement le traitement Sheets/Drive dans certains environnements).

### Étape 3 — Suppression
1. Supprimer `saveDataToSheet` et tout `lib/Tableaux.js` après validation des points ci-dessus.
2. Effacer les clés `LEGACY_USAGE_*` des `DocumentProperties` et retirer `recordLegacyUsage`.
3. Documenter le changement dans `context-kizeo.md` et le changelog du projet.

## 4. Médias Drive (`lib/Images.js`)

### Rappel fonctionnel
- Chaque média reste stocké sur Drive (dans le dossier `Images <formId>`).
- BigQuery reçoit exclusivement :
  - `drive_file_id`, `drive_public_url`, `folder_id`, `folder_url`, etc.
  - Les valeurs sont utilisées par Looker Studio et les exports internes.

### Actions recommandées
1. **Documentation** :
   - Mentionner dans le README / AGENTS que l’hébergement Drive est contractuel (besoin de lien public, quotas Kizeo).
   - Préciser les impacts RGPD : les métadonnées sont conservées côté BQ, les fichiers peuvent être supprimés via Drive si nécessaire.
2. **Surveillance** :
   - Ajouter un script périodique (optionnel) pour vérifier la cohérence Drive ↔ BigQuery (ex. présence du fichier, droits).
3. **Option long terme** :
- Étudier la migration vers Cloud Storage + Signed URLs si la DSI demande une solution hors Drive.

## 5. Cas particulier : projet « MAJ Listes Externes »

Le répertoire `MAJ Listes Externes/` contient un script dédié à la maintenance des listes externes Kizeo (synchronisation Sheets ↔ listes). Il fonctionne indépendamment de l’ingestion BigQuery et s’appuie largement sur des écritures directes dans Sheets :

- Lecture/écriture de la feuille de configuration (`Config`) du classeur dédié.
- Génération des menus UI (`MAJ Listes Externes/UI.js`) et des dialogues HTML.
- Traitement des listes externes via `majListeExterne` et `requeteAPIDonneesExport` (répertoire voisin `lib/`).
- Appel direct à `libKizeo.processData` qui fournit un `processResult` compatible (`rowCount`, `runTimestamp`, `latestRecord`, `medias`, `status`) et repose sur `buildRowSnapshot` pour collecter les médias Drive nécessaires aux exports.

Ce projet est volontairement **séparé du flux BigQuery** :

- Il ne doit **pas** être décommissionné avec `saveDataToSheet`/`Tableaux.js` tant que les listes externes sont gérées via ce fichier.
- Les helpers nouveaux (`logLegacyUsageStats`, instrumentation legacy) peuvent être réutilisés pour surveiller ses fonctions, mais il faut conserver les écritures Sheets dans ce contexte.
- Lors du refactoring modulaire, prévoir un module distinct `external-lists/` ou conserver le projet comme script autonome documenté.

## 6. Prochaines étapes

| Étape | Owner | Deadline | Notes |
|-------|-------|----------|-------|
| Observer `LEGACY_USAGE_*` via `logLegacyUsageStats()` | Ops | déc-2025 | Cibler 0 utilisation avant retrait. |
| Factoriser `buildRowSnapshot` dans un module `legacySnapshot` | Dev | jan-2026 | Préparer la suppression de `Tableaux.js`. |
| Mettre à jour la doc (README / AGENTS) sur le stockage Drive | Dev | nov-2025 | Inclure les risques et modes opératoires. |
| Décider suppression finale du code Sheets | Équipe projet | fév-2026 | Si aucun usage legacy recensé. |
| Documenter la séparation `MAJ Listes Externes` vs ingestion BQ | Dev | nov-2025 | Réaffirmer que les listes externes restent gérées côté Sheets. |
