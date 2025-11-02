# Roadmap simplification codebase

## Objectifs
- Supprimer les reliquats legacy et les fallbacks introduits pour la transition 2025.
- Factoriser les scripts Sheets autour de la librairie commune.
- Réduire les doublons et clarifier l’orchestration `processData`.

## Tâches

### 1. Nettoyage legacy
- [x] Retirer le fallback `getFormConfig` dans `lib/ProcessManager.js` et supprimer toute référence résiduelle.
- [x] Supprimer l’alias global `replaceKizeoData` recréé dans `lib/ExternalListsService.js` et aligner les consommateurs.
- [x] Remplacer le `persistSnapshot` neutre par une implémentation documentée (ou retirer l’API si inutile).

### 2. Harmonisation scripts Sheets
- [x] Introduire un module commun (ex. `SheetAppBindings`) exposant les fonctions partagées *(déployé en 2025 puis retiré au profit d’appels directs)*.
- [x] Simplifier `Code.js` et `MAJ Listes Externes/Code.js` pour déléguer directement vers `libKizeo.*` sans façade intermédiaire.
- [x] Aligner `MAJ Listes Externes` sur les garde-fous récents (`etatExecution`, notifications).

### 3. Orchestration & tests
- [x] Découper `lib/ProcessManager.js` en sous-modules ciblés (préparation, ingestion, finalisation).
- [x] Étendre `tests/run-tests.js` pour couvrir les nouvelles branches (verrouillage menu, snapshot persistant).
- [ ] Documenter les évolutions dans `docs/legacy-deprecation-plan.md`.
