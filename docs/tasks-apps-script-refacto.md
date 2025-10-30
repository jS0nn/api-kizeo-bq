# Roadmap d’amélioration Apps Script

> Contexte : conserver `setScriptPropertiesTermine()` pour usage manuel en cas de blocage, mais renforcer le reste de la base (lib/ + sheetInterface/) selon les axes ci-dessous.  
> Prioriser les items **P1** avant les évolutions fonctionnelles.

## 1. Hygiène du code et nettoyage *(P1)*
- [x] Supprimer les fonctions orphelines (`lib/Outils.generateActionCode`, `formatNumberAllSheets`, `reduireJSON`, `reduireJSON2`, `lib/zz_archives.writeData`) ou les déplacer dans `zz_*` si besoin de debug.
- [x] Retirer les modules obsolètes liés aux feuilles (`lib/GestionDonneesMaJ`, `lib/DataNonLues`) et mettre `context-kizeo.md` en cohérence.
- [x] Auditer `lib/ListesExternes`, `lib/Images`, `lib/Tableaux` et le répertoire **MAJ Listes Externes/** pour isoler les helpers legacy / tests et documenter les dépendances restantes. *(Voir `docs/legacy-external-lists-audit.md`.)*
- [ ] Planifier la suppression de `lib/Tableaux.js` et des wrappers legacy (`saveDataToSheet`, `prepareDataForSheet`, etc.) une fois la période de monitoring terminée.
- [x] Documenter la procédure manuelle `setScriptProperties('termine')` dans `README.md` (section dépannage) pour éviter les confusions.

## 2. Pipeline d’ingestion & découplage *(P1)*
- [x] Éclater `lib/0_Data.handleResponses` en sous-fonctions testables (fetch des data, préparation des lignes, synchronisation médias, marquage lus).
- [x] Éviter la double requête `data/unread` : partager la réponse entre `main()` (sheetInterface) et `processData`.
- [x] Injecter les dépendances (`SpreadsheetApp`, `BigQuery`, `DriveApp`) via paramètres ou wrappers pour faciliter les mocks.
- [x] Clarifier les responsabilités : écrire dans Sheets vs. pousser vers BigQuery, afin de pouvoir désactiver l’un sans impacter l’autre.
- [x] Ajouter un interrupteur de configuration (`ingest_bigquery`) pour piloter l’ingestion BigQuery sans modifier le code.

## 3. Configuration et secrets *(P1)*
- [x] Conserver la lecture du token Kizeo via la feuille dédiée mais ajouter un cache `ScriptProperties` + invalidation automatique (`lib/KizeoClient.js`).
- [x] Valider la présence des propriétés BigQuery en amont (UI + librairie) avec des messages d’erreur explicites.
- [ ] Documenter la procédure de mise à jour des secrets (README + `context-kizeo.md`).

## 4. Robustesse & observabilité *(P2)*
- [x] Enrichir `handleException` pour classifier les erreurs (HTTP, quota, auth) et restituer une structure commune aux appelants.
- [x] Ajouter des retries/backoff sur les points sensibles (UrlFetch Kizeo, BigQuery streaming, Drive).
- [ ] Tracer la consommation (temps, quota) dans les logs et envisager une feuille/BigQuery d’audit minimale.
- [x] Harmoniser les retours d’état des exports PDF/Média (succès, partial, échec).
- [ ] Factoriser un module `DriveMediaService` (encapsulation de `lib/Images.js`) avec dépendances injectables pour tests.

## 5. Tests & validation *(P2)*
- [ ] Transformer les scénarios manuels en fonctions `zzDescribeScenario()` documentées (`lib/zz_Tests`, `sheetInterface/ZZ_tests`).
- [ ] Ajouter un test automatisé couvrant `targets.sheet = false` + mise à jour de liste externe.
- [ ] Couvrir au moins un test d’ingestion complet (form ID fictif) et un test d’export Drive.
- [ ] Ajouter une checklist “run manuel” (clasp push/run, vérif tables BigQuery, inspection Drive) dans `TASKS.md`.
- [ ] Préparer un plan de migration vers des tests automatisés (Apps Script + mocks UrlFetch/BigQuery) lorsque l’API le permettra.
