import paramiko
import os
import json
import uuid
from google.cloud import storage, bigquery
from datetime import datetime, timezone

PROJECT_ID        = os.environ.get("PROJECT_ID", "sfx-reworld-media")
GCS_BUCKET        = os.environ.get("GCS_BUCKET")
BQ_DATASET        = os.environ.get("BQ_DATASET", "import_data")
BQ_TABLE          = os.environ.get("BQ_TABLE", "sylius_imports")
BQ_LOG_TABLE      = os.environ.get("BQ_LOG_TABLE", "pipeline_logs")

def get_sftp_credentials():
    return json.loads(os.environ["SFTP_CREDENTIALS"])

def log_to_bq(bq, job_id, file_name, status, started_at, ended_at=None, rows=None, error=None):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_LOG_TABLE}"
    rows_to_insert = [{
        "job_id":        job_id,
        "file_name":     file_name,
        "status":        status,
        "started_at":    started_at.isoformat(),
        "ended_at":      ended_at.isoformat() if ended_at else None,
        "rows_inserted": rows,
        "error_message": error,
    }]
    errors = bq.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Log BQ error: {errors}")

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
    bq = bigquery.Client()

    remote_dir = creds["dir"]
    transferred = []

# Transfert SFTP → GCS
    for filename in sftp.listdir(remote_dir):
        if not filename.endswith(".csv"):
            continue

        blob = bucket.blob(f"{filename}")

        if blob.exists():
            print(f"Skipped: {filename}")
            continue

        with sftp.open(f"{remote_dir}{filename}", "rb") as f:
            blob.upload_from_file(f)
            print(f"Uploaded: {filename}")
            transferred.append(filename)

    sftp.close()
    ssh.close()

    # Chargement GCS → BigQuery

    errors_list = []
    
    for filename in transferred:
        job_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
        gcs_uri = f"gs://{GCS_BUCKET}/{filename}"


        #Log START
        log_to_bq(bq, job_id, filename, "started", started_at)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            field_delimiter=";",                              # séparateur point-virgule
            quote_character='"',                              # valeurs entre guillemets
            allow_quoted_newlines=True,                       # gère les sauts de ligne dans les champs quotés
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # append
            autodetect=False,                                 # schéma existant → pas d'autodetect
        )

        try:
            load_job = bq.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
            load_job.result()

            ended_at = datetime.now(timezone.utc)
            rows = bq.get_table(table_id).num_rows

            # Log SUCCESS
            log_to_bq(bq, job_id, filename, "success", started_at, ended_at, rows)
            print(f"BigQuery OK: {table_id} — {rows} lignes total")
        
        except Exception as e:
            ended_at = datetime.now(timezone.utc)

            # Log ERROR
            log_to_bq(bq, job_id, filename, "error", started_at, ended_at, error=str(e))
            print(f"BigQuery ERROR {filename}: {e}")
            errors_list.append(filename)

    status = "partial" if errors_list else "success"
    return f"{len(transferred)} fichier(s) transféré(s), statut: {status}", 200