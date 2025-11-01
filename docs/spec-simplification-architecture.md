# Spécification — Simplification sans rétrocompatibilité

## 1. Contexte & constats
- Le projet dépendait d’un alias global `libKizeo` (hérité de l’ancienne bibliothèque) pour accéder aux fonctions de `lib/`.  
  Les appels passent désormais par un helper commun (`requireLibKizeoSymbol`) et l’agrégateur `libKizeo` a été supprimé de `0_Data.js`, ce qui simplifie la navigation et réduit les couches intermédiaires.
- `lib/ProcessManager.js` concentre toutes les responsabilités (lecture Kizeo, staging, BigQuery, listes externes, marquage) dans une IIFE de plus de 700 lignes.  
  Résultat : faible lisibilité, dépendances implicites (`global.*`) et faible testabilité.
- Les modules clefs (`FormResponseSnapshot`, `KizeoClient`, `ExternalListsService`, `DriveMediaService`) exposent désormais directement leurs fonctions globales.
- Les scripts liés `sheetInterface/*` et `MAJ Listes Externes/*` invoquaient auparavant `libKizeo.*`.  
  Ils appellent désormais directement les fonctions natives (API Kizeo, ingestion, configuration, erreurs).
- Le code Sheets legacy (gestion des tableaux/persistance) a été retiré ; l’ingestion repose uniquement sur BigQuery et les snapshots mémoire.

## 2. Objectifs
1. Supprimer toute dépendance à l’alias `libKizeo` et exposer les fonctions directement (déclarations globales ou modules Apps Script simples).
2. (en cours) Découper l’orchestration en modules plus petits et explicites pour réduire la taille de `ProcessManager` et clarifier les enchaînements. Les fonctions `fetchUnreadResponses`, `ingestResponsesBatch` et `finalizeIngestionRun` ont été introduites pour structurer le flux ; il reste à externaliser la boucle exports Drive / triggers.
3. Mettre à jour les scripts liés pour appeler les fonctions natives (`processData`, `requeteAPIDonnees`, `ensureBigQueryCoreTables`, etc.) sans wrapper.
4. Éliminer ou déplacer le code Sheets legacy restant, afin de simplifier la maintenance et préparer la suppression définitive.
5. Documenter l’API publique réelle de la librairie et les dépendances à conserver (token Kizeo, BigQuery, Drive).

## 3. Architecture cible
- **Modules principaux (`lib/`)**
  - `ProcessManager.js` : regrouper uniquement l’orchestration et séparer les blocs internes en fonctions pures (`fetchUnreadResponses`, `ingestResponsesBatch`, `finalizeIngestionRun`, `collectResponseArtifacts`, `runExternalListsSync`, `markResponsesAsRead`).  
    Supprimer l’IIFE et exposer directement les fonctions utilisées.
  - `FormResponseSnapshot.js`, `KizeoClient.js`, `ExternalListsService.js`, `DriveMediaService.js` : conserver des fonctions pures exportées globalement (sans IIFE ni réexport intermédiaire).
  - `0_Data.js` : fournir uniquement les helpers utiles (`createIngestionServices`, `bqBackfillForm`, …) sans alias additionnel.
- Legacy Sheets : supprimé (plus de `SheetSnapshot.js`, `Images.js`, `Tableaux.js`).

- **Scripts liés (`sheetInterface/`, `MAJ Listes Externes/`)**
  - Supprimer toute référence à `libKizeo.*`.  
    Appeler directement `processData`, `handleException`, `requeteAPIDonnees`, `ensureBigQueryCoreTables`, `gestionFeuilles`, etc. *(complété)*
  - ✅ Factoriser la logique de configuration (lecture/écriture feuille `Config`, normalisation des flags) via `lib/SheetConfigHelpers.js` consommé par les deux scripts Sheets.
  - ✅ Mutualiser les exports Drive (PDF/médias) via `lib/SheetDriveExports.js` pour supprimer la duplication entre `sheetInterface` et `MAJ Listes Externes`.
  - Réorganiser `Code.js` : séparer triggers, orchestration (`main`, `majSheet`), exports Drive, utilitaires.  
    Garder `Code.js` comme point d’entrée déclarant les fonctions appelées par les triggers.
  - Adapter `UI.js` à la nouvelle API (gestion des erreurs directe, chargement des formulaires via `requeteAPIDonnees`).

## 4. Plan d’action détaillé

### Phase A — Nettoyage de la librairie
1. Réécrire `lib/ProcessManager.js` sans IIFE et sans re-export massif vers `global`.  
   Extraire les sous-fonctions internes en blocs indépendants et limiter les exports publics à `processData`, `handleResponses`, `markResponsesAsRead`, `resolveUnreadDataset` (si nécessaire).
2. Simplifier `lib/0_Data.js` pour qu’il n’expose plus que la liste des fonctions publiques (ou qu’il soit supprimé si les déclarations globales suffisent).  
   Toute fonction ne faisant que déléguer devient inutile. *(fait — `getLibPublicSymbols` documente désormais l’API)*
3. Adapter les autres modules (`FormResponseSnapshot`, `KizeoClient`, `ExternalListsService`, `DriveMediaService`) pour qu’ils exposent directement leurs fonctions sans alias supplémentaires. *(fait)*
4. Vérifier l’absence de références internes au pattern `ensureProcessManager()` ou `recordLegacyUsage()` ; retirer ces helpers si non utilisés. *(fait)*
5. **Tests associés (à planifier)**  
   - Créer un scénario `zzDescribeScenario()` côté librairie pour invoquer `processData` avec des dépendances mockées et vérifier le flux BigQuery + listes externes.  
   - Exécuter `clasp run processData` (ou point d’entrée dédié) avec un formulaire de test afin de contrôler que les exceptions sont remontées correctement.  
   - Lancer un lint ou analyse statique (si disponible) pour détecter les références obsolètes à `libKizeo`.

### Phase B — Mise à jour des scripts liés
5. Remplacer dans `sheetInterface/*` toutes les occurrences de `libKizeo.*` par les fonctions natives. *(fait)*  
   Ajuster les imports implicites (Apps Script charge tous les fichiers du projet : s’assurer que les fonctions sont déclarées au niveau global).
6. Réorganiser `sheetInterface/Code.js` en modules dédiés (`triggers.js`, `pipeline.js`, `config.js`, `exports.js`).  
   `Code.js` ne doit plus dépasser quelques centaines de lignes et se contenter de déclarer les points d’entrée utilisés par les triggers/UI.
7. Répliquer la même approche dans `MAJ Listes Externes/*` : appels directs, modules légers, suppression de l’alias. *(fait)*
8. **Tests associés**  
   - Ajouter un scénario UI manuel (`ZZ_tests.js`) simulant la sélection de formulaire, la configuration et le déclenchement de `main`.  
   - Tester `main` et `majSheet` via `clasp run` après chaque refactor pour vérifier l’ingestion réelle et la mise à jour de la feuille `Config`.  
   - Vérifier les scripts `MAJ Listes Externes` en exécutant un scénario de synchronisation complète (lecture feuille + update listes) et consigner les résultats.

### Phase C — Legacy, documentation et validation
8. (Fait) Supprimer le code Sheets legacy (`SheetSnapshot.js`, wrappers temporaires) après vérification qu’aucun appel ne subsiste.
9. Mettre à jour `docs/legacy-deprecation-plan.md`, `docs/tasks-apps-script-refacto.md`, `README.md` et `AGENTS.md` pour refléter la nouvelle architecture (plus de wrappers, API publique simplifiée).
10. Créer/mettre à jour un fichier listant l’API publique (`lib/public-api.md` ou commentaire en tête de `0_Data.js`) pour garder la trace des fonctions exposées. *(fait — `getLibPublicSymbols` retourne la liste officielle)*
11. Vérifier l’exécution manuelle (`sheetInterface/main`, `majSheet`, `MAJ Listes Externes`) et ajuster les tests manuels (`ZZ_tests.js`) pour utiliser les nouveaux appels.
12. **Tests associés**  
    - Exécuter la batterie de scénarios manuels (ingestion, listes externes, exports Drive) avant et après la suppression du legacy pour comparer les logs.  
    - Documenter les résultats des tests dans `docs/test-runs.md` (ou fichier existant) afin de tracer les validations.  
    - Vérifier qu’aucun `rg libKizeo` ne renvoie de résultat et que les ScriptProperties/Drive restent accessibles.

## 5. Impacts & points d’attention
- **Déploiement** : la suppression de `libKizeo` impose de pousser simultanément la librairie et les scripts liés. Prévoir une fenêtre de maintenance ou une coordination des `clasp push`.
- **Tests** : aucune automatisation n’existe. Préparer des scénarios `zzDescribeScenario()` pour valider l’ingestion, la synchro listes externes et les exports Drive avant et après refactor.
- **Documentation** : toute mention de `libKizeo` doit être retirée des guides internes pour éviter toute confusion (README, AGENTS, Context).
- **Gestion des secrets** : confirmer que la lecture du token Kizeo et des ScriptProperties BigQuery fonctionne toujours après réécriture (les accès restent globaux).
- **Versioning** : incrémenter les commentaires de version en tête de fichier (`lib/0_Data.js`, `sheetInterface/Code.js`, etc.) dès que des modifications seront apportées conformément à la règle projet.

## 6. Livrables attendus
- Librairie `lib/` sans IIFE ni alias `libKizeo`, avec modules découpés.
- Scripts `sheetInterface` et `MAJ Listes Externes` simplifiés, appelant directement les fonctions principales.
- Documentation mise à jour (`docs/`, `README`, `AGENTS`) et nouvelle liste des fonctions publiques.
- Tâches de suivi créées dans `docs/tasks-apps-script-refacto.md` et/ou `TASKS.md`.
- Scénarios manuels adaptés pour valider la nouvelle architecture.
