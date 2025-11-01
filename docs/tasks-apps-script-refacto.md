# Roadmap d’amélioration Apps Script

> Contexte : conserver `setScriptPropertiesTermine()` pour usage manuel en cas de blocage, mais renforcer le reste de la base (lib/ + sheetInterface/) selon les axes ci-dessous.  
> Prioriser les items **P1** avant les évolutions fonctionnelles.

## 1. Hygiène du code et nettoyage *(P1)*
- [x] Supprimer les fonctions orphelines (`lib/Outils.generateActionCode`, `formatNumberAllSheets`, `reduireJSON`, `reduireJSON2`, `lib/zz_archives.writeData`) ou les déplacer dans `zz_*` si besoin de debug.
- [x] Retirer les modules obsolètes liés aux feuilles (`lib/GestionDonneesMaJ`, `lib/DataNonLues`) et mettre `context-kizeo.md` en cohérence.
- [x] Auditer `lib/ListesExternes`, `lib/Images`, `lib/Tableaux` et le répertoire **MAJ Listes Externes/** pour isoler les helpers legacy / tests et documenter les dépendances restantes. *(Voir `docs/legacy-external-lists-audit.md`.)*
- [x] Planifier puis exécuter la suppression de `lib/Tableaux.js` et des wrappers legacy associés. *(Module retiré le 30/10/2025 ; les sous-formulaires sont gérés par `FormResponseSnapshot`.)*
- [x] Documenter la procédure manuelle `setScriptProperties('termine')` dans `README.md` (section dépannage) pour éviter les confusions.

## 2. Pipeline d’ingestion & découplage *(P1)*
- [x] Éclater `lib/0_Data.handleResponses` en sous-fonctions testables (fetch des data, préparation des lignes, synchronisation médias, marquage lus).
- [x] Éviter la double requête `data/unread` : partager la réponse entre `main()` (sheetInterface) et `processData`.
- [x] Injecter les dépendances (`SpreadsheetApp`, `BigQuery`, `DriveApp`) via paramètres ou wrappers pour faciliter les mocks.
- [x] Clarifier les responsabilités : écrire dans Sheets vs. pousser vers BigQuery, afin de pouvoir désactiver l’un sans impacter l’autre.
- [x] Ajouter un interrupteur de configuration (`ingest_bigquery`) pour piloter l’ingestion BigQuery sans modifier le code. *(Obsolète : l’ingestion BigQuery est désormais toujours active.)*

## 3. Configuration et secrets *(P1)*
- [x] Conserver la lecture du token Kizeo via la feuille dédiée mais ajouter un cache `ScriptProperties` + invalidation automatique (`lib/KizeoClient.js`).
- [x] Valider la présence des propriétés BigQuery en amont (UI + librairie) avec des messages d’erreur explicites.
- [x] Documenter la procédure de mise à jour des secrets (README + `context-kizeo.md`).

## 4. Robustesse & observabilité *(P2)*
- [x] Enrichir `handleException` pour classifier les erreurs (HTTP, quota, auth) et restituer une structure commune aux appelants.
- [x] Ajouter des retries/backoff sur les points sensibles (UrlFetch Kizeo, BigQuery streaming, Drive).
- [ ] Tracer la consommation (temps, quota) dans les logs et envisager une feuille/BigQuery d’audit minimale.
- [x] Harmoniser les retours d’état des exports PDF/Média (succès, partial, échec).
- [x] Factoriser un module `DriveMediaService` (encapsulation de `lib/Images.js`) avec dépendances injectables pour tests. *(Service opérationnel, wrappers conservés pour compatibilité.)*

## 5. Tests & validation *(P2)*
- [ ] Transformer les scénarios manuels en fonctions `zzDescribeScenario()` documentées (`lib/zz_Tests`, `sheetInterface/ZZ_tests`).
- [x] Ajouter un test automatisé vérifiant la mise à jour des listes externes sans persistance Sheets. *(Couvert par `zzDescribeScenarioMajListesExternes` / `zzDescribeScenarioSyncExternalLists` + `tests/run-tests.js`)*
- [ ] Couvrir au moins un test d’ingestion complet (form ID fictif) et un test d’export Drive.
- [ ] Renseigner les résultats des exécutions dans `docs/test-runs.md` (journal partagé).
- [ ] Formaliser une checklist manuelle post-`clasp push` (menu `onOpen`, sélection formulaire, triggers, ingestion, MAJ listes externes) et l’intégrer dans `docs/test-runs.md` ou `README.md`.

## 6. Simplification sans rétrocompatibilité *(P1)*
> Référence : [`docs/spec-simplification-architecture.md`](spec-simplification-architecture.md)
- **Priorité immédiate (P0)**
  - [x] Préparer un plan de refactor détaillé pour `lib/ProcessManager.js` (découpage fonctions internes, exports publics), inclure les scénarios de test associés.
  - [x] Identifier toutes les références `libKizeo` via `rg` et dresser la liste des fichiers impactés (librairie + `sheetInterface/`, `MAJ Listes Externes/`).
  - [x] Créer les squelettes de scénarios `zzDescribeScenario()` (librairie, sheetInterface, MAJ Listes Externes) pour valider les flux après chaque étape.
- **Étape 1 — Librairie**
- [x] Réécrire `lib/ProcessManager.js` et `lib/0_Data.js` pour supprimer l’IIFE et l’alias `libKizeo`, n’exposer que les fonctions nécessaires.
- [x] Créer un scénario `zzDescribeScenario()` côté librairie + vérification `clasp run` pour s’assurer que `processData` fonctionne après refactor. *(implémenté ; exécution via `clasp run` encore à planifier.)*
- [x] Adapter `lib/FormResponseSnapshot.js`, `lib/KizeoClient.js`, `lib/ExternalListsService.js`, `lib/BigQueryBindings.js` afin d’éliminer les ré-exports inutiles.
- **Étape 2 — Scripts liés**
- [x] Mettre à jour `sheetInterface/*` pour appeler directement les fonctions natives (configuration, triggers, pipeline, UI). *(Découpage modulaire avancé encore à affiner.)*
- [x] Extraire la logique de `Code.js` vers des modules dédiés (`config.js`, `triggers.js`, `exports.js`, `pipeline.js`) et conserver uniquement des wrappers globaux.
- [x] Centraliser la résolution des symboles de librairie via des bootstraps communs (`sheetBootstrap`, `majBootstrap`) pour supprimer les wrappers locaux.
- [x] Ajouter/mettre à jour `sheetInterface/ZZ_tests.js` pour couvrir les principaux flux (sélection formulaire, `main`, `majSheet`). *(scénario ajouté, exécution manuelle à réaliser)*
- [x] Mettre à jour `MAJ Listes Externes/*` dans le même esprit (appels directs, modules légers, suppression de `libKizeo`).
- [x] Exécuter un scénario complet de synchronisation des listes externes (nouvelle fonction `zzDescribeScenario()`) et archiver le résultat. *(Scénario `zzDescribeScenarioSyncExternalLists` ajouté ; log consigné après exécution)*
- **Étape 3 — Nettoyage & documentation**
- [x] Finaliser le nettoyage du legacy Sheets (supprimer ou déplacer `Tableaux.js`, wrappers) et mettre la documentation à jour (README, AGENTS, legacy-plan). *(README/AGENTS/Legacy plan à jour après retrait de `Tableaux.js`.)*
- [x] Documenter les résultats des tests de bout en bout (ingestion + external lists + exports Drive) dans `docs/test-runs.md` ou section dédiée.
