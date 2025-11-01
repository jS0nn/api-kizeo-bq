# Suivi migration BigQuery

## Phase actuelle
- [x] Ajouter un squelette d'ingestion BigQuery (table brute + audit)
- [x] Définir les ScriptProperties `BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_LOCATION` *(conservé la valeur par défaut `fr-tpd-sarpi-datagrs-dev.Kizeo`, unique cible autorisée)*
- [x] Vérifier la création automatique des tables `kizeo_raw_events` et `etl_audit` *(confirmé, `clasp run bqIngestParentBatch` crée bien les tables)*
- [x] Cartographier les champs Kizeo → schéma BigQuery typé (parents)
- [x] Implémenter l'écriture des tables parent (colonnes dynamiques)
- [x] Implémenter l'écriture BigQuery des médias Drive *(fait: `bqIngestMediaBatch` crée une table par formulaire via `formId__alias__media` et enregistre `drive_file_id`, champs parent, URL Drive)*
- [x] Optimiser la déduplication Drive des médias sans multiplier les appels `DriveApp` *(cache Drive côté Apps Script pour éviter les `getFilesByName` répétés)*
- [x] Gérer les évolutions de schéma (ALTER ADD COLUMN)
- [x] Renforcer `bqRecordAudit` (statuts d'échec, volumétrie, durée, référence run)
- [x] Préparer le backfill historique et validation BI *(disponible via `bqBackfillForm`, export direct BigQuery sans passer par Sheets)*
- [ ] Valider la spécification migration Cloud Run (voir docs/cloudrun-migration.md)

## Backlog prioritaire
- [x] `lib/APIHandler.js`: externaliser la récupération du token Kizeo dans `PropertiesService`, ne relire le classeur que si l'appel API échoue, puis repropager la valeur (éviter toute boucle infinie).
- [x] `lib/APIHandler.js`: harmoniser le retour d'erreur pour ne plus renvoyer `{data: undefined, responseCode: undefined}` après exception.
- [x] `lib/GestionErreurs.gs.js`: sécuriser l'accès à `SpreadsheetApp.getActiveSpreadsheet()` et limiter les envois d'e-mails (fallback si classeur absent).
- [x] `lib/0_Data.js`, `lib/Tableaux.js`: analyser les dépendances des fonctions `@deprecated`, définir un plan de débranchement ou d'isolation et refléter l'état dans la documentation. *(Voir `docs/legacy-deprecation-plan.md`.)*
- [x] `lib/Images.js`: conserver l'écriture Drive des médias (métadonnées seulement en BigQuery) et documenter la responsabilité. *(Section Drive dédiée dans `docs/legacy-deprecation-plan.md`.)*
- [x] ETL: persister un mapping `{slug,label,type}` (ex. dans `etl_audit` ou table dédiée) pour historiser la nomenclature dynamique.
- [x] Logs/erreurs: adopter un préfixe cohérent (`lib:module:fct`), centraliser le throttling des mails et enrichir l'audit d'échecs.
- [x] `handleResponses`: scinder les responsabilités (ingestion BigQuery, mise à jour Sheets, listes externes) et clarifier les retours d'état.
- [x] Architecture: découper les rôles principaux en modules dédiés (API, ingestion, persistance Sheets/Drive, orchestration). *(ProcessManager gère désormais l’orchestration ; restent à traiter les triggers et le nettoyage complet du legacy Sheets.)*
- [x] Legacy: introduire un flag de feature pour le code historique (Sheets/Drive), vérifier les usages actuels et décider de sa désactivation par défaut.

## Notes rapides
- Les fonctions BigQuery nécessitent le service avancé `BigQuery` activé (fait dans `lib/appsscript.json`).
- Les tables parent sont désormais alimentées (`bqIngestParentBatch`), avec création automatique des colonnes dynamiques.
- Les sous-formes sont déjà envoyées vers BigQuery (`bqIngestSubTablesBatch`), les médias sont désormais stockés dans des tables `__media` dédiées.
- La feuille `Config` stocke alias BigQuery + dernier traitement (`last_data_id`, `last_update_time`, `last_run_at`).
- L'action utilisée pour Kizeo est désormais le nom du classeur (UI sans champ dédié), alias BigQuery saisi via la modale.
- Liste externe maintenue dans Sheets pour l'instant ; BigQuery devient la source de vérité pour les réponses et médias.
- La table d'audit `etl_audit` expose désormais table cible, durée, statut et message d'erreur via `bqRecordAudit`.
