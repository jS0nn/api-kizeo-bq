# AGENTS.md — Apps Script Repository (BigQuery / Kizeo / Sheets)

## TL;DR
- The repository contains two Apps Script projects:
  - `lib/`: a reusable library consumed by production spreadsheets.
  - `sheetInterface/`: a bound script for the spreadsheet along with HtmlService assets.
- Use `clasp pull` and `clasp push` inside the appropriate subdirectory to synchronize changes with Google Apps Script.
- Remote execution: run `clasp run main` (for the sheet) or `clasp run bqIngestParentBatch` (for the library) after pushing.
- To access up‑to‑date documentation while developing, append **`use context7`** to your prompts and enable **Context7 MCP** in Codex (see below).

---

## Repository structure

- `lib/` — main modules:
  - `BigQuery.js`: BigQuery ingestion logic.
  - `APIHandler.js`: Kizeo API calls.
  - `Outils.js` / `Tableaux.js`: utilities for synchronising data with Sheets.
- `sheetInterface/` — bound script and UI:
  - `UI.js`, `timeIntervalSelector.html`: menus, dialogs and triggers.
- `zz_*.js` — exploratory code and manual test harnesses.
- `context-kizeo.md` — architecture decisions.

---

## Build, sync & execution

> Always change into the relevant subdirectory before running `clasp` commands.
> Always ask user autorisation before running `clasp` commands.

### One‑time installation
```bash
npm i -g @google/clasp
clasp login
```

### Library workflow
```bash
cd lib
clasp pull        # fetch the latest script files
# edit your code here
clasp push        # upload changes
clasp run bqIngestParentBatch  # execute the main ingestion function remotely
```

### Sheet interface workflow
```bash
cd sheetInterface
clasp pull
# edit your code here
clasp push
clasp run main
```
Consult the official `clasp` documentation for details on `pull`, `push`, `login` and `run`.

---

## Coding style — KISS & Clean Code

- Use **2‑space indentation**, **single quotes** and **camelCase** for functions and variables. Reserve `SCREAMING_SNAKE_CASE` for configuration constants (e.g. `BQ_DEFAULT_CONFIG`).
- Keep functions short. Use early returns instead of deeply nested conditionals. Avoid hidden side effects.
- Place shared utilities in `Outils.js` and prefer small, composable helpers to long procedures.
- Logging: at the beginning of critical functions, prefix messages with `lib:module:function` (e.g. `lib:BigQuery:bqEnsureDataset…`).
- Error handling: use guard clauses at the top of functions; provide useful error messages; avoid silent `try/catch` blocks.

---

## Manual testing

There is no automated test runner. Manual harnesses live in `zz_Tests.js` (for the library) and `sheetInterface/ZZ_tests.js` (for the UI).

- Create targeted entry points in these files and name them `zzDescribeScenario()` to signal manual status.
- Remove or disable these functions once validated.
- To run a scenario remotely, execute:

```bash
clasp run zzDescribeScenario
```

- Before merging, verify that BigQuery writes (`BigQuery.Tables.list`) and spreadsheet mutations behave as expected.

---

## Commit & pull request guidelines

- Keep commit messages short, imperative and scoped by directory, for example:
  “`lib: initialise BigQuery ingestion`”.
- In your pull request description, summarise the problem, outline the solution, list the manual checks performed (`clasp push/pull`, key runs), and highlight any required `ScriptProperty` changes or new triggers.
- Add screenshots or Drive links when the UI changes.
- If the documentation used comes from Context7, add `#docs-via: context7` in the commit message to make it explicit.

---

## Configuration & secrets

- The library reads the following `ScriptProperties`: `BQ_PROJECT_ID`, `BQ_DATASET` and `BQ_LOCATION` (default `europe-west1`).
- Set these via **Apps Script → Project Settings** or through the `PropertiesService` console before running ingestion.
- **Never commit real keys or secrets.** Share them through the team vault. Keep default locations aligned across environments and document overrides in the relevant spreadsheet tab.

---

## UI (HtmlService) reminders

- Keep dialogs and sidebars lightweight. Use asynchronous server calls via `google.script` for better responsiveness.
- Avoid heavy inline styles; prefer external CSS when possible.

---

## Context7 MCP (for Codex) — up‑to‑date documentation during development

### When to use

- Whenever you work with an external API or library (Apps Script services, BigQuery, fetch, Kizeo, etc.).
- When you are unsure about a method signature, quota, or need an example.
- Append **`use context7`** to your prompt to force Codex to query Context7 for documentation.

### Activate Context7 in Codex (one‑time setup)

**Option A — CLI:**
```bash
codex mcp add context7 -- npx -y @upstash/context7-mcp
```

**Option B — `~/.codex/config.toml`:**
```toml
[mcp_servers.context7]
command = "npx"
args    = ["-y", "@upstash/context7-mcp"]  # add --api-key if you have one

# Optional (timeouts / RMCP)
# experimental_use_rmcp_client = true
```

- Codex reads MCP configuration from `~/.codex/config.toml` (shared between the CLI and the VS Code extension).
- It supports **STDIO** and **HTTP streamable** modes.
- Prerequisites: Node >= 18. Providing an API key increases usage limits.

### Sample prompts

- “List the options for `BigQuery.Tables.insert` in Apps Script. **use context7**”
- “Provide a minimal example of `UrlFetchApp.fetch` with JSON headers and error handling. **use context7**”
- **Advanced:** if you know the exact library, you can skip auto‑detection:
  “Implement an upsert in BigQuery via the Jobs API; **use library /google/bigquery**; return an idiomatic GAS snippet. **use context7**”

### Best practices

- Specify the version when relevant (e.g. REST v2, current quotas).
- Ask for short, executable examples and original links when available.
- Summarise and cite documentation from Context7 in your pull requests (see `#docs-via: context7`).
- If no relevant information is found, note “fallback to generic docs” and propose an alternative.

---

## Playbooks

### Ingestion BigQuery (quick)

1. `cd lib && clasp pull`
2. Check that `ScriptProperties` (project, dataset, location) are set correctly.
3. `clasp run bqIngestParentBatch`
4. Perform a sanity check by listing the tables/partitions of the target dataset.

### Sync Sheets ↔ Library

1. `cd sheetInterface && clasp pull`
2. Modify `UI.js` / `timeIntervalSelector.html` as needed.
3. `clasp push` then `clasp run main`
4. Check the mutations on the target sheet and review logs.

---

## Definition of Done (verify before merging)

- [ ] `clasp push` completed on the modified directory/directories.
- [ ] Manual tests are green (temporary `zzDescribeScenario()` functions removed).
- [ ] Logs are readable and errors are handled appropriately.
- [ ] New `ScriptProperties` are documented if introduced.
- [ ] If documentation came from Context7, the commit message includes `#docs-via: context7`.

---

## Notes

- **AGENTS.md** is a recognised format used by many agents (including Codex). It contains operational guidelines without cluttering the README.
- **Context7 MCP**: details the concept, the `use context7` signal, configuration for local and HTTP modes, and how to select specific libraries.
- **Codex + MCP**: explains the shared configuration (`~/.codex/config.toml`), CLI commands (`codex mcp add …`), and the supported STDIO/HTTP modes.
- **Apps Script** essentials: using `clasp` for local development and deployment, the advanced BigQuery service, `PropertiesService`, and `HtmlService` for dialogs/sidebars.


---

##Important : 
- **Tu dois ajouter un numero de version du code en commentaire (pour sheetInterface au debut de Code.js, pour lib au debut de 0_Data.js), si aucune version n'est notée commence à 0.1.0 **

tu dois toujours retourner tes informations en francais
