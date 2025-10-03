# Suivi migration BigQuery

## Phase actuelle
- [x] Ajouter un squelette d'ingestion BigQuery (table brute + audit)
- [ ] Définir les ScriptProperties `BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_LOCATION`
- [ ] Vérifier la création automatique des tables `kizeo_raw_events` et `etl_audit`
- [ ] Cartographier les champs Kizeo → schéma BigQuery typé
- [ ] Implémenter l'écriture des tables parent / sous-forme / médias
- [ ] Gérer les évolutions de schéma (ALTER ADD COLUMN)
- [ ] Activer la journalisation détaillée (table `etl_audit` + métriques)
- [ ] Préparer le backfill historique et validation BI

## Notes rapides
- Les fonctions BigQuery nécessitent le service avancé `BigQuery` activé (fait dans `lib/appsscript.json`).
- Les insertions sont pour l'instant limitées à la table brute `kizeo_raw_events` : à enrichir lors des prochaines étapes.
