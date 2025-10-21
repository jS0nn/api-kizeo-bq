# Repository Guidelines

## Project Structure & Module Organization
The repository contains two Apps Script projects. `lib/` hosts the reusable library consumed by production spreadsheets; key modules include `BigQuery.js` pour l’ingestion BigQuery, `APIHandler.js` pour les appels Kizeo et le tandem `Outils.js`/`Tableaux.js` pour la synchronisation Sheets. `sheetInterface/` holds the bound spreadsheet script and HtmlService assets (`UI.js`, `timeIntervalSelector.html`) that expose menus, dialogs, and triggers. Keep exploratory code in the existing `zz_*.js` files and capture architecture decisions in `context-kizeo.md`.

## Build, Sync & Development Commands
Work inside the relevant subdirectory before running commands. Use `npm install -g @google/clasp` once, then `clasp login` with the right Google account. Typical flows are `cd lib && clasp pull` to fetch the latest scripts, edit locally, then `clasp push` to upload. For the spreadsheet UI, run the same commands inside `sheetInterface/`. To trigger remote execution, call `clasp run main` (sheet) or `clasp run bqIngestParentBatch` (library) after pushing.

## Coding Style & Naming Conventions
JavaScript files follow a 2-space indent, single quotes, and `camelCase` for functions and variables (`bqEnsureDataset`, `handleResponses`). Reserve `SCREAMING_SNAKE_CASE` for configuration constants such as `BQ_DEFAULT_CONFIG`. Keep HTML dialogs lightweight and inline styles minimal. Place guard clauses and logging at the top of functions, reuse utilities from `Outils.js`, and prefer composable helpers over long procedures.

## Testing Guidelines
There is no automated test runner; manual harnesses live in `zz_Tests.js` and `sheetInterface/ZZ_tests.js`. Create targeted entry points there, name them `zzDescribeScenario()` to signal manual status, and remove or disable them once validated. Use `clasp run zzDescribeScenario` to execute against the remote project, and log detailed context so failures can be replayed. Aim to verify BigQuery writes (`BigQuery.Tables.list`) and spreadsheet mutations before merging.

## Commit & Pull Request Guidelines
Existing history is terse; keep messages short, in the imperative, and scoped (`lib: initialise BigQuery ingestion`). Reference Jira or task IDs when available. Pull requests should summarise the problem, outline the solution, list manual checks (`clasp push/pull`, key runs), and highlight any required ScriptProperty changes or new triggers. Add screenshots or Drive links when UI dialogs change and tag reviewers owning the target spreadsheet.

## Configuration & Secrets
The library reads ScriptProperties `BQ_PROJECT_ID`, `BQ_DATASET`, and `BQ_LOCATION`. Set them via Apps Script > Project Settings or the `PropertiesService` console before running ingestion. Never commit real keys; share them through the team vault. Keep default locations (`europe-west1`) aligned between environments and document overrides in the relevant spreadsheet tab.


tu dois toujours retourner tes informations en francais
