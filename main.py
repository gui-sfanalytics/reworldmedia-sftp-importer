import paramiko
import os
import json
import uuid
from google.cloud import storage, bigquery
from datetime import datetime, timezone
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

PROJECT_ID        = os.environ.get("PROJECT_ID", "sfx-reworld-media")
GCS_BUCKET        = os.environ.get("GCS_BUCKET", "reworld_media_bucket")
BQ_DATASET        = os.environ.get("BQ_DATASET", "import_data")
BQ_TABLE          = os.environ.get("BQ_TABLE", "sylius_imports")
BQ_LOG_TABLE      = os.environ.get("BQ_LOG_TABLE", "pipeline_logs")
BQ_LOCATION       = os.environ.get("BQ_LOCATION", "EU")

# Clés uniques qui identifient une ligne comme unique dans sylius_imports
# ⚠️ À adapter selon votre schéma réel
UNIQUE_KEYS = [
    "purchase_id",
]

def get_sftp_credentials():
    return json.loads(os.environ["SFTP_CREDENTIALS"])

def log_to_bq(bq, job_type, job_id, file_name, status, started_at, 
              ended_at=None, rows=None, error=None):
    try:
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_LOG_TABLE}"
        rows_to_insert = [{
            "job_id": job_id,
            "file_name": file_name,
            "status": status,
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat() if ended_at else None,
            "rows_inserted": rows,
            "error_message": error,
            "job_type": job_type,
        }]
        errors = bq.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Log BQ error: {errors}")
    except Exception as e:
        print(f"Logging FAILED (non bloquant): {e}")


def ensure_dataset_exists(bq: bigquery.Client, dataset_id: str):
    try:
        bq.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = BQ_LOCATION
        bq.create_dataset(dataset)
        print(f"Dataset créé : {dataset_id}")


def ensure_table_exists(bq: bigquery.Client, table_id: str, schema: list):
    try:
        bq.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        bq.create_table(table)
        print(f"Table créée : {table_id}")

def get_pipeline_logs_schema():
    return [
        bigquery.SchemaField("job_id", "STRING"),
        bigquery.SchemaField("file_name", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("ended_at", "TIMESTAMP"),
        bigquery.SchemaField("rows_inserted", "INT64"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("job_type", "STRING"),
    ]

def get_sylius_schema():
    return [
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("purchase_id", "STRING"),
        bigquery.SchemaField("payer_subscriber_id", "STRING"),
        bigquery.SchemaField("idefix_response", "STRING"),

        bigquery.SchemaField("purchase_day", "STRING"),
        bigquery.SchemaField("purchase_month", "STRING"),
        bigquery.SchemaField("purchase_year", "STRING"),
        bigquery.SchemaField("purchase_time", "STRING"),

        bigquery.SchemaField("billing_customer", "STRING"),
        bigquery.SchemaField("shipping_customer", "STRING"),

        bigquery.SchemaField("order_state", "STRING"),
        bigquery.SchemaField("payment_state", "STRING"),
        bigquery.SchemaField("payment_method", "STRING"),

        bigquery.SchemaField("billing_country", "STRING"),
        bigquery.SchemaField("shipping_country", "STRING"),

        bigquery.SchemaField("discount_rules", "STRING"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("title_code", "STRING"),
        bigquery.SchemaField("doc_code", "STRING"),
        bigquery.SchemaField("choice", "STRING"),

        bigquery.SchemaField("prime_1", "STRING"),
        bigquery.SchemaField("prime_2", "STRING"),
        bigquery.SchemaField("prime_3", "STRING"),

        bigquery.SchemaField("product_type", "STRING"),

        bigquery.SchemaField("taxon_1", "STRING"),
        bigquery.SchemaField("taxon_2", "STRING"),
        bigquery.SchemaField("taxon_3", "STRING"),
        bigquery.SchemaField("taxon_4", "STRING"),

        bigquery.SchemaField("is_couplage", "STRING"),
        bigquery.SchemaField("add_or_adl", "STRING"),
        bigquery.SchemaField("renewal_offer", "STRING"),
        bigquery.SchemaField("is_extension", "STRING"),

        bigquery.SchemaField("periodicity", "STRING"),
        bigquery.SchemaField("prelevement", "STRING"),
        bigquery.SchemaField("is_multi", "STRING"),
        bigquery.SchemaField("operation", "STRING"),
        bigquery.SchemaField("is_abo_premium", "STRING"),

        bigquery.SchemaField("issue_number", "STRING"),
        bigquery.SchemaField("format", "STRING"),

        bigquery.SchemaField("product_analytic_value", "STRING"),
        bigquery.SchemaField("order_analytic_value", "STRING"),

        bigquery.SchemaField("unit_price", "STRING"),
        bigquery.SchemaField("quantity", "STRING"),

        bigquery.SchemaField("order_amount_excl_shipping", "STRING"),
        bigquery.SchemaField("purchase_amount_excl_shipping", "STRING"),

        bigquery.SchemaField("purchase_discount", "STRING"),
        bigquery.SchemaField("order_discount", "STRING"),

        bigquery.SchemaField("shipping_cost", "STRING"),

        bigquery.SchemaField("utm_source", "STRING"),
        bigquery.SchemaField("utm_medium", "STRING"),
        bigquery.SchemaField("utm_campaign", "STRING"),
    ]



def get_staging_table_id(filename: str) -> str:
    """
    Génère un nom de table de staging unique par fichier.
    Ex: sylius_imports_staging_mon_fichier_20240101
    """
    safe_name = filename.replace(".csv", "").replace("-", "_").replace(" ", "_")
    return f"{PROJECT_ID}.{BQ_DATASET}.staging_{safe_name}"


def load_to_staging(bq: bigquery.Client, gcs_uri: str, staging_table_id: str):
    """
    Charge le fichier CSV depuis GCS dans une table de staging temporaire.
    La table est recréée à chaque exécution (WRITE_TRUNCATE).
    """
    schema = None
    try:
        schema = bq.get_table(f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}").schema
    except NotFound:
        schema = get_sylius_schema()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        field_delimiter=";",
        quote_character='"',
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        # WRITE_TRUNCATE : recrée la table staging proprement à chaque run
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        # Le schéma de la staging doit correspondre à la table cible
        schema = schema,
    )

    load_job = bq.load_table_from_uri(gcs_uri, staging_table_id, job_config=job_config)
    load_job.result()
    print(f"Staging chargée : {staging_table_id}")


def merge_staging_to_target(bq: bigquery.Client, staging_table_id: str) -> int:
    """
    Fusionne la table staging dans la table cible.
    - Si la ligne existe déjà (même clés uniques) → on ne fait rien (NOT UPDATE)
    - Si la ligne est nouvelle → INSERT
    Retourne le nombre de lignes insérées.
    """
    target_table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # Récupère toutes les colonnes de la table cible
    target_table = bq.get_table(target_table_id)
    all_columns = [field.name for field in target_table.schema]

    # Construction de la condition de jointure sur les clés uniques
    join_condition = " AND ".join(
        [f"T.{key} = S.{key}" for key in UNIQUE_KEYS]
    )

    # Construction des colonnes pour le INSERT
    insert_columns = ", ".join(all_columns)
    insert_values  = ", ".join([f"S.{col}" for col in all_columns])

    merge_query = f"""
        MERGE `{target_table_id}` AS T
        USING (
            SELECT * EXCEPT(row_num)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {", ".join(UNIQUE_KEYS)}
                        ORDER BY (SELECT NULL)
                    ) AS row_num
                FROM `{staging_table_id}`
            )
            WHERE row_num = 1
        ) AS S
        ON {join_condition}

        -- La ligne existe déjà → on ne fait rien
        WHEN MATCHED THEN
            UPDATE SET T.{all_columns[0]} = T.{all_columns[0]}

        -- La ligne est nouvelle → on insère
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
    """

    query_job = bq.query(merge_query)
    query_job.result()

    # @@row_count n'est pas accessible via l'API Python,
    # on retourne les stats du job
    rows_inserted = query_job.num_dml_affected_rows or 0
    print(f"Merge terminé : {rows_inserted} nouvelle(s) ligne(s) insérée(s)")
    return rows_inserted


def delete_staging_table(bq: bigquery.Client, staging_table_id: str):
    """Supprime la table de staging après le merge."""
    bq.delete_table(staging_table_id, not_found_ok=True)
    print(f"Table staging supprimée : {staging_table_id}")


def transfer_sftp_to_gcs(request):
    creds = get_sftp_credentials()

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=creds["host"],
        port=int(creds["port"]),
        username=creds["user"],
        password=creds["password"],
    )
    sftp = ssh.open_sftp()

    gcs = storage.Client()
    bucket = gcs.bucket(GCS_BUCKET)
    bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

    remote_dir = creds["dir"]
    transferred = []

    dataset_id = f"{PROJECT_ID}.{BQ_DATASET}"
    target_table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    log_table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_LOG_TABLE}"

    # Ensure infra ok
    ensure_dataset_exists(bq, dataset_id)
    ensure_table_exists(bq, target_table_id, get_sylius_schema())
    ensure_table_exists(bq, log_table_id, get_pipeline_logs_schema())

    # ─── 1. Transfert SFTP → GCS ────────────────────────────────────────────
    for filename in sftp.listdir(remote_dir):
        if not filename.endswith(".csv"):
            continue

        job_id     = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        blob       = bucket.blob(filename)

        if blob.exists():
            print(f"Skipped (déjà dans GCS) : {filename}")
            continue

        log_to_bq(bq, "sftp_to_gcs", job_id, filename, "started", started_at)

        with sftp.open(f"{remote_dir}{filename}", "rb") as f:
            try:
                blob.upload_from_file(f)
                ended_at = datetime.now(timezone.utc)
                print(f"Uploaded : {filename}")
                log_to_bq(bq, "sftp_to_gcs", job_id, filename, 
                          "success", started_at, ended_at)
                transferred.append(filename)

            except Exception as e:
                ended_at = datetime.now(timezone.utc)
                print(f"Erreur upload {filename} : {e}")
                log_to_bq(bq, "sftp_to_gcs", job_id, filename, 
                          "error", started_at, ended_at, error=str(e))

    sftp.close()
    ssh.close()

    # ─── 2. Chargement GCS → BigQuery (via staging + merge) ─────────────────
    errors_list = []

    for filename in transferred:
        job_id     = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        gcs_uri    = f"gs://{GCS_BUCKET}/{filename}"

        staging_table_id = get_staging_table_id(filename)

        log_to_bq(bq, "gcs_to_bq", job_id, filename, "started", started_at)

        try:
            # Étape A : charger dans la staging
            load_to_staging(bq, gcs_uri, staging_table_id)

            # Étape B : merger dans la table cible (dédoublonnage)
            rows_inserted = merge_staging_to_target(bq, staging_table_id)

            ended_at = datetime.now(timezone.utc)
            log_to_bq(bq, "gcs_to_bq", job_id, filename, 
                      "success", started_at, ended_at, rows_inserted)
            print(f"BigQuery OK : {filename} — {rows_inserted} nouvelle(s) ligne(s)")

        except Exception as e:
            ended_at = datetime.now(timezone.utc)
            print(f"BigQuery ERROR {filename} : {e}")
            log_to_bq(bq, "gcs_to_bq", job_id, filename, 
                      "error", started_at, ended_at, error=str(e))
            errors_list.append(filename)

        finally:
            # Étape C : nettoyage de la staging dans tous les cas
            delete_staging_table(bq, staging_table_id)

    status = "partial" if errors_list else "success"
    return f"{len(transferred)} fichier(s) transféré(s), statut: {status}", 200
