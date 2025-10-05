# Suivi migration BigQuery

## Phase actuelle
- [x] Ajouter un squelette d'ingestion BigQuery (table brute + audit)
- [ ] Définir les ScriptProperties `BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_LOCATION`
- [ ] Vérifier la création automatique des tables `kizeo_raw_events` et `etl_audit`
- [x] Cartographier les champs Kizeo → schéma BigQuery typé (parents)
- [x] Implémenter l'écriture des tables parent (colonnes dynamiques)
- [ ] Implémenter l'écriture des sous-formes / médias
- [ ] Gérer les évolutions de schéma (ALTER ADD COLUMN)
- [ ] Activer la journalisation détaillée (table `etl_audit` + métriques)
- [ ] Préparer le backfill historique et validation BI

## Notes rapides
- Les fonctions BigQuery nécessitent le service avancé `BigQuery` activé (fait dans `lib/appsscript.json`).
- Les tables parent sont désormais alimentées (`bqIngestParentBatch`), avec création automatique des colonnes dynamiques.
- Prochaines étapes : répliquer la logique sur les sous-formes et médias, puis activer la journalisation détaillée.
- Nouveau : la feuille `Config` stocke alias BigQuery + dernier traitement (`last_data_id`, `last_update_time`, `last_run_at`).
- L'action utilisée pour Kizeo est désormais le nom du classeur (UI sans champ dédié), alias BigQuery saisi via la modale.
