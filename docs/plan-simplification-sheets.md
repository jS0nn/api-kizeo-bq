## Plan d’action – Simplification des scripts Sheets

### Hypothèses
- Les scripts liés (`sheetInterface/`, `MAJ Listes Externes/`) continueront à consommer la librairie publiée via le préfixe `libKizeo`.
- Aucun refactor majeur côté librairie n’est prévu après cette simplification.
- Les tests manuels se feront via `clasp push` + exécutions ciblées (`main`, `majSheet`, `runBigQueryDeduplication`).

---

### Étapes détaillées

1. **Cartographier les entrées utilisées**
   - Lister toutes les fonctions `libKizeo.*` actuellement consommées dans `sheetInterface/` et `MAJ Listes Externes/`.
   - Vérifier qu’elles existent bien dans `lib/zz_PublicApi.js` / `lib/0_Data.js`.

2. **Supprimer la couche `SheetAppBindings`**
   - Retirer `SheetAppBindings.create` et `sheetAppInvoke` dans `sheetInterface/Code.js` et `MAJ Listes Externes/Code.js`.
   - Replacer chaque délégation par un appel direct `libKizeo.<fonction>` (ex. `return libKizeo.processData(...)`).
   - Supprimer les dépendances à `SheetAppBindings` dans `lib/zz_PublicApi.js` si elles deviennent inutiles.

3. **Nettoyer les modules locaux en doublon**
   - Pour chaque fichier `sheetInterface/*.js` et `MAJ Listes Externes/*.js`, enlever les wrappers qui exposaient simplement la librairie.
   - Mutualiser les constantes partagées si nécessaire (via un petit helper commun ou en les conservant dans chaque script si elles sont propres à ce contexte).

4. **Adapter les tests manuels**
   - Mettre à jour `sheetInterface/ZZ_tests.js` et `MAJ Listes Externes/ZZ_tests.js` afin d’utiliser directement `libKizeo.*` sans passer par la façade supprimée.
   - Vérifier que `tests/run-tests.js` n’a plus besoin de mocker `SheetAppBindings`.

5. **Validation fonctionnelle**
   - `clasp push` sur chaque projet (`lib/`, `sheetInterface/`, `MAJ Listes Externes/`) après modifications.
   - Exécuter manuellement `main` (menu ou `clasp run`) pour confirmer que l’ingestion démarre.
   - Lancer `runBigQueryDeduplication` et `majSheet` pour vérifier que les menus restent opérationnels.

6. **Documentation**
   - Mettre à jour `README.md`, `AGENTS.md` et `context-kizeo.md` pour refléter le nouveau flux (plus de `SheetAppBindings`).
   - Archiver une note rapide dans `docs/test-runs.md` avec la date et le résultat de la validation.

---

### Checklist finale
- [x] Façade `SheetAppBindings` supprimée des scripts liés.
- [x] Tous les wrappers remplacés par des appels directs `libKizeo.*`.
- [x] Tests manuels / harness ajustés.
- [x] `clasp push` + scénarios principaux vérifiés pour les trois projets.
- [x] Documentation synchronisée.
- [x] Note de validation ajoutée dans `docs/test-runs.md`.
