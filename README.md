# 📦 SFTP → GCS → BigQuery Pipeline

Pipeline automatisé permettant de transférer des fichiers CSV depuis un serveur SFTP vers Google Cloud Storage (GCS), puis de les charger dans Google BigQuery avec gestion des doublons via une table de staging.

---

## 🗂️ Structure du projet

```
.
├── main.py              # Logique principale du pipeline
├── Dockerfile           # Image Docker pour Cloud Run / Cloud Functions
├── requirements.txt     # Dépendances Python
└── README.md            # Documentation du projet
```

---

## ⚙️ Fonctionnement

Le pipeline s'exécute en **3 étapes** :

### 1. SFTP → GCS
- Connexion au serveur SFTP via les credentials fournis en variable d'environnement.
- Parcours des fichiers `.csv` présents dans le répertoire distant.
- Upload de chaque fichier vers le bucket GCS configuré.
- Les fichiers déjà présents dans GCS sont ignorés (idempotence).

### 2. GCS → BigQuery (Staging)
- Chaque fichier CSV uploadé est chargé dans une **table de staging temporaire** dans BigQuery.
- Le schéma de la staging est automatiquement calqué sur la table cible `sylius_imports`.
- La table de staging est recréée à chaque exécution (`WRITE_TRUNCATE`).

### 3. Staging → Table cible (Merge)
- Un `MERGE` BigQuery est exécuté pour fusionner les données staging dans la table cible.
- **Dédoublonnage** : si une ligne existe déjà (basé sur `purchase_id`), elle n'est pas modifiée.
- Seules les **nouvelles lignes** sont insérées.
- La table de staging est **supprimée** après le merge, qu'il y ait eu une erreur ou non.

---

## 🔐 Variables d'environnement

| Variable           | Description                                      | Valeur par défaut         |
|--------------------|--------------------------------------------------|---------------------------|
| `PROJECT_ID`       | ID du projet Google Cloud                        | `sfx-reworld-media`       |
| `GCS_BUCKET`       | Nom du bucket GCS cible                          | `reworld_media_bucket`    |
| `BQ_DATASET`       | Dataset BigQuery cible                           | `import_data`             |
| `BQ_TABLE`         | Table BigQuery cible                             | `sylius_imports`          |
| `BQ_LOG_TABLE`     | Table BigQuery de logs                           | `pipeline_logs`           |
| `BQ_LOCATION`      | Localisation BigQuery                            | `EU`                      |
| `SFTP_CREDENTIALS` | JSON contenant les credentials SFTP (voir ci-dessous) | *(obligatoire)*      |

### Format de `SFTP_CREDENTIALS`

```json
{
  "host": "sftp.example.com",
  "port": "22",
  "user": "mon_utilisateur",
  "password": "mon_mot_de_passe",
  "dir": "/chemin/vers/les/fichiers/"
}
```

> ⚠️ Il est fortement recommandé de stocker ce secret dans **Google Secret Manager** et de l'injecter au démarrage du conteneur.

---

## 🗄️ Schéma BigQuery

### Table `pipeline_logs`

| Colonne         | Type      | Description                              |
|-----------------|-----------|------------------------------------------|
| `job_id`        | STRING    | Identifiant unique du job (UUID)         |
| `file_name`     | STRING    | Nom du fichier traité                    |
| `status`        | STRING    | `started`, `success`, `error`            |
| `started_at`    | TIMESTAMP | Horodatage de début                      |
| `ended_at`      | TIMESTAMP | Horodatage de fin                        |
| `rows_inserted` | INTEGER   | Nombre de lignes insérées                |
| `error_message` | STRING    | Message d'erreur si applicable           |
| `job_type`      | STRING    | `sftp_to_gcs` ou `gcs_to_bq`            |

### Table `sylius_imports` (table cible)

Clé d'unicité : **`purchase_id`**

> Le schéma complet est défini directement dans BigQuery. Le pipeline s'appuie sur ce schéma pour créer dynamiquement les tables de staging.

---

## 🚀 Déploiement

### Prérequis

- Google Cloud SDK installé et configuré
- Docker installé
- Projet GCP avec les APIs activées :
  - Cloud Storage
  - BigQuery
  - Cloud Run (ou Cloud Functions)

### Build & Push de l'image Docker

```bash
docker build -t gcr.io/<PROJECT_ID>/sftp-bq-pipeline .
docker push gcr.io/<PROJECT_ID>/sftp-bq-pipeline
```

### Déploiement sur Cloud Run

```bash
gcloud run deploy sftp-bq-pipeline \
  --image gcr.io/<PROJECT_ID>/sftp-bq-pipeline \
  --region europe-west1 \
  --set-env-vars PROJECT_ID=sfx-reworld-media,GCS_BUCKET=reworld_media_bucket \
  --set-secrets SFTP_CREDENTIALS=sftp-credentials:latest \
  --no-allow-unauthenticated
```

### Déclenchement automatique via Cloud Scheduler

```bash
gcloud scheduler jobs create http sftp-bq-daily \
  --schedule="0 6 * * *" \
  --uri="https://<CLOUD_RUN_URL>" \
  --oidc-service-account-email=<SERVICE_ACCOUNT> \
  --time-zone="Europe/Paris"
```

---

## 🧪 Test local

```bash
pip install -r requirements.txt

export SFTP_CREDENTIALS='{"host":"...","port":"22","user":"...","password":"...","dir":"/data/"}'
export PROJECT_ID="sfx-reworld-media"

functions-framework --target=transfer_sftp_to_gcs --port=8080
```

Puis dans un autre terminal :

```bash
curl -X POST http://localhost:8080
```

---

## 🔗 Intégration avec le reporting BigQuery

Ce pipeline alimente la table `sylius_imports` qui est utilisée en amont de la procédure stockée `load_mail_product_daily`. Cette procédure calcule quotidiennement les KPIs abonnements papier par titre (J, M-1, A-1) et les stocke dans `reporting.product_mail_daily` pour être visualisés dans **Google Looker Studio**.

```
SFTP → GCS → sylius_imports → product_mail_daily → Looker Studio
```

---

## 📋 Dépendances

| Package                        | Version  |
|-------------------------------|----------|
| `paramiko`                    | 2.12.0   |
| `google-cloud-storage`        | 2.19.0   |
| `google-cloud-secret-manager` | 2.22.0   |
| `google-cloud-bigquery`       | 3.31.0   |
| `functions-framework`         | 3.8.2    |

---
