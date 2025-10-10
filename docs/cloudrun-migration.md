# Migration vers Cloud Run – Spécification fonctionnelle et technique

## 1. Résumé exécutif
- Migrer l’ingestion Kizeo → BigQuery d’Apps Script vers un service Cloud Run conteneurisé, plus robuste et observable.
- Conserver le modèle de données BigQuery actuel (tables parents, sous-formulaires, médias, audit) tout en remplaçant les déclencheurs Apps Script par Cloud Scheduler.
- Offrir une API REST sécurisée pour déclencher des batchs (temps réel manuel) et un traitement planifié (horaires).

## 2. Contexte & motivations
- Apps Script atteint ses limites (temps d’exécution, quotas, gestion des secrets).
- Besoin de logs centralisés (Cloud Logging), de traces d’erreurs exploitables, et d’une gouvernance IAM plus fine.
- Préparation à des pipelines futurs (backfill massif, transformations additionnelles) mieux gérés sur Cloud Run.

## 3. Portée
- Implémenter un service Node.js (ou autre runtime compatible) conteneurisé.
- Reprendre les fonctionnalités suivantes : récupération des réponses Kizeo, marquage lu, ingestion BigQuery, stockage audit.
- Intégrer la gestion des médias Drive (sauvegarde, métadonnées) ou définir alternative si stockage différent.
- Exposer endpoints :
  - `POST /batch/current` : ingestion incrémentale (équivalent `bqIngestParentBatch` + médias).
  - `POST /batch/backfill` : ingestion historique paramétrable (période, offset).
  - `POST /sync/lists` : mise à jour listes externes si conservée.
- Ajouter une tâche Cloud Scheduler (cron configurable) → Pub/Sub → Cloud Run.
- Gérer la configuration via Secrets Manager + variables d’environnement (IDs projet, dataset, token Kizeo).

## 4. Hors portées connues
- Refactoring de l’interface Sheets (menus, modales) hors besoin d’adapter le déclencheur manuel.
- Remplacement de Drive pour stockage média (option étudiée mais non livrée dans ce lot).
- Automatisation terraform complète (peut être livrée plus tard).
- Migration complète de la logique UI (HTMLService) vers une app web autonome.

## 5. Hypothèses & dépendances
- Dataset BigQuery existant (`fr-tpd-sarpi-datagrs-dev.Kizeo`) reste la cible.
- Accès Kizeo assuré via token stocké en Secret Manager.
- Compte de service Cloud Run avec rôles BigQuery Data Editor, BigQuery Job User, Storage Object Admin (si Drive via API, rôle Drive API OAuth requis).
- Project GCP disposant de Cloud Run, Pub/Sub, Cloud Scheduler activés.
- Limites Kizeo API : quotas respectés par throttling côté service.

## 6. Architecture cible

### 6.1 Diagramme logique
```
[Cloud Scheduler] → [Pub/Sub Topic] → [Cloud Run Service]
                                  ↘︎ [Manual caller via HTTPS]
Cloud Run → (Kizeo API) → parsing → BigQuery streaming (tables)
           ↘︎ Drive API (médias)   ↘︎ Cloud Logging / Error Reporting
```

### 6.2 Composants
- **Cloud Run Service `kizeo-bq-ingester`**
  - App Node.js (Express ou Fastify)
  - Clients : Kizeo REST v3, BigQuery API, Drive API (optionnel)
  - Gestion configuration : dotenv (local), env vars en production
  - Observabilité : Cloud Logging structuré (traceIds), métriques via Cloud Monitoring (latence, volume)
- **Pub/Sub**
  - Sujet `kizeo-bq-batch` recevant messages `{"type": "current", "formId": ..., ...}`
- **Cloud Scheduler**
  - Cron paramétrable (ex: toutes les 15 min) publiant un message standard
- **Secrets Manager**
  - `kizeo-token`
  - `bq-project-id`, `bq-dataset`, `bq-location` (ou env vars)
- **BigQuery**
  - Tables existantes + ALTER ADD COLUMN automatique
  - Table audit enrichie (status, durée, volume, runId)

## 7. Spécifications fonctionnelles

### 7.1 Ingestion incrémentale
- Lecture liste des formulaires configurés (source ?)
  - Option A : table BigQuery `config_forms`
  - Option B : feuille Sheets existante via API (transition)
- Pour chaque formulaire :
  - Appel `GET /forms/{id}/data/unread` avec paramètres action (alias) et limite.
  - Pour chaque réponse :
    - Récupérer détails (`GET /data/{id}`) + sous-formulaires
    - Écrire dans table BigQuery `kizeo_raw_events` avec schéma dynamique (gérer nouveaux champs via `ALTER COLUMN` → `bqEnsureTableSchema`)
    - Gérer médias : télécharger blob, stocker Drive (ou Cloud Storage), enregistrer metadata table `__media`
    - Marquer comme lu (POST `markasreadbyaction`)
  - Écrire entrée `etl_audit` (status, start/end, counts, error)
- Exposer runId (UUID) pour corrélation dans BigQuery + logs.

### 7.2 Backfill
- Endpoint acceptant paramètres : `formId`, `fromDate`, `toDate`, `pageSize`.
- Itération via `GET /forms/{id}/data/search` (si API supporte) ou `list` paginée.
- Inscrire volumes distincts dans audit (type = BACKFILL).
- Protection : limiter aux rôles IAM autorisés (auth Cloud Run invoker + IAP).

### 7.3 Gestion des médias
- MVP : conserver Drive (App Folder) avec service account.
  - Stockage: dossier par formulaire `formId__alias`
  - Table BigQuery `formId__alias__media` contenant `drive_file_id`, `parent_data_id`, URLs.
- Option (à étudier) : Cloud Storage bucket pour simplifier IAM + signatures URL.

### 7.4 Configuration
- Mapping formulaire ↔ alias ↔ action stockée dans table BigQuery/Firestore ou fichier JSON déployé (préférer BQ pour édition).
- Intervalle de polling paramétrable `CRON_SCHEDULE`.
- Limite throttling (sleep configurable) pour respecter quotas Kizeo.

### 7.5 Interfaces
- **HTTP API** (Swagger)
  - `POST /batch/current` { forms?: [ids], dryRun?: bool }
  - `POST /batch/backfill` { formId, from, to, includeMedia? }
  - `POST /sync/lists` { formId?, listId? }
  - `GET /healthz` : vérifie accès BigQuery, Secrets.
- **Pub/Sub message schema**
  - `{"type":"current","forms":["123","456"],"trigger":"scheduler"}`
  - `{"type":"backfill","formId":"123","from":"2023-01-01","to":"2023-01-31"}`

## 8. Spécifications techniques

### 8.1 Stack & packaging
- Node.js 20 LTS
- Gestion libs : npm, bundler (esbuild) si besoin
- Tests : Jest + Harness simulant Kizeo (mock)
- Dockerfile multistage (builder + runtime)
- Déploiement via `gcloud run deploy` ou Terraform
- Observabilité :
  - Logs JSON structurés (`severity`, `runId`, `formId`, `count`)
  - Export vers BigQuery audit via Logging sink (optionnel)

### 8.2 Schéma BigQuery
- Reprendre `bqEnsureDataset`, `bqEnsureTable` logiques existantes
- Ajout colonnes :
  - `ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
  - `source_run_id STRING`
  - `batch_trigger STRING` (scheduler, manual, backfill)
- Table audit : `run_id`, `trigger`, `status`, `started_at`, `ended_at`, `duration_ms`, `forms_processed`, `responses_processed`, `errors`

### 8.3 Secrets & IAM
- Service account `kizeo-ingester-sa`
  - Roles: `roles/run.invoker`, `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`, `roles/secretmanager.secretAccessor`, `roles/drive.admin` (si Drive)
- Cloud Run invokers:
  - Cloud Scheduler (via Pub/Sub push)
  - Utilisateurs autorisés (IAM + API key ? Préférer IAP ou authentication via Identity-Aware Proxy/Identity Tokens)
- Secret Manager versioning (rotation token Kizeo)
- Variables d’environnement :
  - `BQ_PROJECT`
  - `BQ_DATASET`
  - `BQ_LOCATION`
  - `KIZEO_BASE_URL`
  - `DRIVE_FOLDER_ROOT` (optionnel)
  - `DEFAULT_ACTION_ALIAS`

## 9. Plan de migration

1. **Analyse & préparation**
   - Cartographier formulaires actifs, volumes, dépendances UI.
   - Extraire config existante (feuille `Config`) → format JSON/BQ.
2. **MVP Cloud Run**
   - Implé module ingestion parent tables sans médias.
   - Tests locaux (mocks Kizeo) + tests intégration (sandbox dataset).
3. **Extension médias & sous-formulaires**
   - Ajouter modules `media`, `subForm`.
   - Vérifier quotas Drive, options Cloud Storage.
4. **Audit & observabilité**
   - Implémenter `runId`, logs structurés, métriques.
5. **Infrastructure**
   - Déployer Cloud Scheduler + Pub/Sub + Secrets.
   - Configurer pipeline CI/CD (Cloud Build ou GitHub Actions).
6. **Double run & validation**
   - Exécuter service en parallèle d’Apps Script (mode read-only) pendant période d’observation.
   - Comparer volumes BigQuery, colonnes (table audit).
7. **Basculer**
   - Désactiver déclencheurs Apps Script.
   - Documenter nouveaux workflows (manual triggers).
8. **Nettoyage**
   - Déprécier code obsolète dans `lib/`.
   - Mettre à jour `context-kizeo.md`, `TASKS.md`.

## 10. Validation & tests
- Tests unitaires : parsing Kizeo → BigQuery row, traitement erreurs réseau.
- Tests intégration : batch complet sur dataset de staging.
- Test charge : run répétées pour estimer latence et quotas.
- Vérification BigQuery :
  - Tables créées + colonnes dynamiques ajoutées automatiquement.
  - Table audit reflète tous les statuts.
- Test déclencheur Cloud Scheduler + Pub/Sub (message ack, run success).
- Tests de reprise : simuler échec (HTTP 500 Kizeo) → audit status = FAILED, logs.

## 11. Observabilité & exploitation
- Dashboard Cloud Monitoring : temps d’exécution, count responses, erreur par formulaire.
- Alertes :
  - Erreur 5xx Cloud Run (>3 sur 15 min) → Slack / email.
  - Absence de run réussi > 1h (via alerting).
- Runbook :
  - Relancer batch manuel (`POST /batch/current`).
  - Vérifier logs (`gcloud logging read ... runId`).
  - Rotation token Kizeo : update Secret, redeploy/roll restart.

## 12. Risques & atténuations
- **Quota Kizeo** : throttling, scheduler différé, monitoring.
- **Coût Cloud Run** : évaluer temps d’exécution, autoscaling max (ex: 3 instances).
- **Gestion médias** : Drive API quotas/partage → envisager GCS.
- **Schéma BigQuery** : colonnes dynamiques imposent `ALTER TABLE` ; prévoir latences (utiliser jobs asynchrones si > 5 colonnes).
- **Sécurité** : secret exposé si log accidentel → filtrer logs, valider payloads.
- **Backfill massif** : prévoir partitionnement par date, limiter volume.

## 13. Documentation & livrables
- README migration (ce fichier + guide de déploiement).
- Diagrammes architecture (mermaid), éventuellement export PNG.
- Scripts Terraform/YAML Cloud Run.
- Post-mortem “Go/No-Go” + check-list de désactivation Apps Script.
