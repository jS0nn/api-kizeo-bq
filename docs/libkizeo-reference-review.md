# Référence `libKizeo`

La bibliothèque reste incluse dans les projets Apps Script via le symbole `libKizeo` (manifest `appsscript.json`) afin de charger le code source. Aucune fonction applicative n’utilise cet alias : les appels se font directement via les fonctions globales (`processData`, `requeteAPIDonnees`, `bqRunDeduplicationForForm`, etc.).

Vérification rapide :

```bash
rg "libKizeo" --glob '!appsscript.json'
```

Ce grep ne doit plus retourner de code exécutable (seulement des mentions dans la documentation ou le présent fichier). Les deux manifestes (`sheetInterface/appsscript.json`, `MAJ Listes Externes/appsscript.json`) conservent l’entrée `libKizeo` pour référencer la bibliothèque.
