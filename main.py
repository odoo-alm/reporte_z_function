# =============================================================================
# main.py  v3 — con circuit breaker y label PROCESADO
# =============================================================================

import logging
import os
import uuid
import time
import threading
from datetime import datetime, timezone

import google.auth
import google.auth.transport.requestsgcloud 
from flask import Flask, jsonify, request
from google.auth import iam
from google.auth.transport import requests as auth_requests
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import logging as cloud_logging
from google.oauth2 import service_account
from googleapiclient.discovery import build

from utils import circuit_is_open, get_or_create_label
from ingest import ingest_bp, init_ingest
from process import process_bp, init_process
from sync_z         import sync_z
from sync_facturas  import sync_facturas
from sync_nc        import sync_nc
from sync_megasoft  import sync_megasoft
from sync_pedidosya import sync_pedidosya
from sync_ubii      import sync_ubii
from sync_credicard import sync_credicard

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
try:
    cloud_log_client = cloud_logging.Client()
    cloud_log_client.setup_logging()
except Exception:
    pass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
BQ_PROJECT            = os.environ.get("GCP_PROJECT", "gestion-365")
IMPERSONATE_USER      = os.environ.get("IMPERSONATE_USER", "odoo@gestion365ve.com")
SERVICE_ACCOUNT_EMAIL = os.environ.get("SA_EMAIL", "233079260983-compute@developer.gserviceaccount.com")
GMAIL_SCOPES          = ["https://www.googleapis.com/auth/gmail.modify"]
ETIQUETAS             = ["BARAKO", "BRASERO", "CARBON", "OFICINA"]
LABEL_PROCESADO       = "PROCESADO"

# ---------------------------------------------------------------------------
# Cache global de labels Gmail
# ---------------------------------------------------------------------------
_label_map_cache: dict = {}
_label_map_ts: float   = 0
LABEL_MAP_TTL          = 3600
_cache_lock            = threading.Lock()


def get_label_map(service) -> dict:
    global _label_map_cache, _label_map_ts
    now = time.time()
    if _label_map_cache and (now - _label_map_ts) < LABEL_MAP_TTL:
        logger.info("Label map desde caché.")
        return _label_map_cache
    with _cache_lock:
        if _label_map_cache and (now - _label_map_ts) < LABEL_MAP_TTL:
            return _label_map_cache
        logger.info("Actualizando label map desde Gmail API…")
        results = service.users().labels().list(userId="me").execute()
        _label_map_cache = {lbl["name"].upper(): lbl["id"] for lbl in results.get("labels", [])}
        _label_map_ts = now
        logger.info(f"Label map actualizado: {len(_label_map_cache)} etiquetas.")
    return _label_map_cache


# ---------------------------------------------------------------------------
# Módulos registrados
# ---------------------------------------------------------------------------
MODULOS = [
    {"key": "z",         "fn": sync_z,         "label": "Reporte Z"},
    {"key": "facturas",  "fn": sync_facturas,  "label": "Facturas"},
    {"key": "nc",        "fn": sync_nc,        "label": "Notas de Crédito"},
    {"key": "megasoft",  "fn": sync_megasoft,  "label": "Megasoft"},
    {"key": "pedidosya", "fn": sync_pedidosya, "label": "PedidosYa"},
    {"key": "ubii",      "fn": sync_ubii,      "label": "Ubii"},
    {"key": "credicard", "fn": sync_credicard, "label": "Credicard"},
]

# ---------------------------------------------------------------------------
# Flask + BigQuery
# ---------------------------------------------------------------------------
app = Flask(__name__)
bq  = bigquery.Client(project=BQ_PROJECT)
gcs = storage.Client(project=BQ_PROJECT)

# Registrar blueprint de ingest y pasarle los clientes compartidos
app.register_blueprint(ingest_bp)
init_ingest(bq, gcs)
app.register_blueprint(process_bp)
init_process(bq, gcs)


# ---------------------------------------------------------------------------
# Autenticación Gmail
# ---------------------------------------------------------------------------
def get_gmail_service():
    credentials, _ = google.auth.default()
    credentials.refresh(google.auth.transport.requests.Request())
    signer = iam.Signer(
        request=auth_requests.Request(),
        credentials=credentials,
        service_account_email=SERVICE_ACCOUNT_EMAIL,
    )
    delegated = service_account.Credentials(
        signer=signer,
        service_account_email=SERVICE_ACCOUNT_EMAIL,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=GMAIL_SCOPES,
        subject=IMPERSONATE_USER,
    )
    delegated.refresh(auth_requests.Request())
    return build("gmail", "v1", credentials=delegated)


# ---------------------------------------------------------------------------
# POST /sync
# ---------------------------------------------------------------------------
@app.route("/sync", methods=["POST"])
def sync():
    run_id    = str(uuid.uuid4())
    inicio_ts = datetime.now(tz=timezone.utc)
    body      = request.get_json(silent=True) or {}

    keys_solicitados = [k.lower() for k in body.get("modulos", [])]
    claves_validas   = {m["key"] for m in MODULOS}
    claves_invalidas = [k for k in keys_solicitados if k not in claves_validas]

    if claves_invalidas:
        return jsonify({
            "error": f"Módulos desconocidos: {claves_invalidas}. Válidos: {sorted(claves_validas)}"
        }), 400

    modulos_a_correr = (
        [m for m in MODULOS if m["key"] in keys_solicitados]
        if keys_solicitados else MODULOS
    )

    logger.info(f"=== INICIO run_id={run_id} | modulos={[m['key'] for m in modulos_a_correr]} ===")

    # Autenticación Gmail
    try:
        service   = get_gmail_service()
        label_map = get_label_map(service)
    except Exception as e:
        logger.error(f"Auth Gmail falló: {e}")
        return jsonify({"run_id": run_id, "error": f"Auth Gmail falló: {e}"}), 500

    # Asegurar que existe el label PROCESADO
    processed_label_id = get_or_create_label(service, LABEL_PROCESADO, label_map)
    if not processed_label_id:
        logger.warning("No se pudo obtener/crear label PROCESADO — continuando sin marcar.")

    resultados       = {}
    errores_globales = []

    for i, modulo in enumerate(modulos_a_correr):
        key   = modulo["key"]
        label = modulo["label"]

        # Circuit breaker — si Gmail está bloqueado, abortar el resto
        if circuit_is_open():
            msg = f"Circuit breaker abierto — abortando módulos restantes."
            logger.error(msg)
            errores_globales.append(msg)
            for m in modulos_a_correr[i:]:
                resultados[m["key"]] = {"error": "abortado por circuit breaker"}
            break

        # Pausa entre módulos
        if i > 0:
            logger.info("Pausa 5s entre módulos…")
            time.sleep(5)

        logger.info(f"--- Iniciando: {label} ---")
        try:
            stats = modulo["fn"](service, bq, ETIQUETAS, label_map, processed_label_id)
            resultados[key] = stats
            logger.info(
                f"--- {label} OK | "
                f"procesados={stats.get('procesados', 0)} "
                f"cargados={stats.get('cargados', 0)} "
                f"errores={len(stats.get('errores', []))} ---"
            )
        except Exception as e:
            msg = f"{label}: excepción no controlada — {e}"
            logger.error(msg, exc_info=True)
            errores_globales.append(msg)
            resultados[key] = {"error": str(e)}

    duracion = int((datetime.now(tz=timezone.utc) - inicio_ts).total_seconds())
    hay_errores = errores_globales or any(
        len(r.get("errores", [])) > 0
        for r in resultados.values()
        if isinstance(r, dict)
    )
    status = "OK" if not hay_errores else "PARCIAL"

    logger.info(f"=== FIN run_id={run_id} | status={status} | {duracion}s ===")

    return jsonify({
        "run_id":     run_id,
        "status":     status,
        "duracion_s": duracion,
        "resultados": resultados,
        "errores":    errores_globales,
    }), 200 if status == "OK" else 207


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)