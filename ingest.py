# =============================================================================
# ingest.py — Gmail → GCS + Zelle parser
# Responsabilidades:
#   1. Leer correos nuevos (sin label PROCESADO) por etiqueta
#   2. Subir adjuntos a GCS: gs://gestion-365-data-lake/ETIQUETA/YYYY/MM/DD/filename
#   3. Parsear body de correos Zelle (Chase, Wells Fargo) → BQ directo
#   4. Marcar cada correo como PROCESADO en Gmail
#
# NO parsea adjuntos — eso lo hace process.py
# NO depende de parsing para marcar como procesado
# =============================================================================

import base64
import email as email_lib
import email.policy
import io
import json
import logging
import os
import re
import time
import threading
import uuid
from datetime import datetime, timezone

import google.auth
import google.auth.transport.requests
from flask import Blueprint, jsonify, request
from google.auth import iam
from google.auth.transport import requests as auth_requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
BQ_PROJECT            = os.environ.get("GCP_PROJECT", "gestion-365")
IMPERSONATE_USER      = os.environ.get("IMPERSONATE_USER", "odoo@gestion365ve.com")
SERVICE_ACCOUNT_EMAIL = os.environ.get("SA_EMAIL", "233079260983-compute@developer.gserviceaccount.com")
GMAIL_SCOPES          = ["https://www.googleapis.com/auth/gmail.modify"]
GCS_BUCKET            = "gestion-365-data-lake"
BQ_TABLE_ZELLE        = "mailer_raw.mailer_zelle_received"
ETIQUETAS             = ["BARAKO", "BRASERO", "CARBON", "OFICINA"]
LABEL_PROCESADO       = "PROCESADO"
GMAIL_THROTTLE        = 0.5
PAUSE_S               = 3  # pausa entre correos — menor que process porque no hay parsing

# Remitentes Zelle conocidos
ZELLE_SENDERS = {
    "chase":       ["no.reply.alerts@chase.com", "alertsp@chase.com"],
    "wellsfargo":  ["alerts@notify.wellsfargo.com"],
    "bofa":        ["onlinebanking@ealerts.bankofamerica.com",
                    "customerservice@ealerts.bankofamerica.com"],
}

# ---------------------------------------------------------------------------
# Circuit breaker (mismo patrón que utils.py)
# ---------------------------------------------------------------------------
_circuit_open        = False
_circuit_retry_after: float = 0
_cache_lock          = threading.Lock()


def circuit_is_open() -> bool:
    global _circuit_open, _circuit_retry_after
    if not _circuit_open:
        return False
    if time.time() >= _circuit_retry_after:
        _circuit_open = False
        logger.info("[circuit] Rate limit expirado — cerrado.")
        return False
    remaining = int(_circuit_retry_after - time.time())
    logger.warning(f"[circuit] Abierto — {remaining}s restantes.")
    return True


def circuit_trip(error_str: str):
    global _circuit_open, _circuit_retry_after
    m = re.search(r"Retry after (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.\d]*Z)", error_str)
    if m:
        try:
            dt = datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
            _circuit_retry_after = dt.timestamp() + 30  # 30s buffer
        except Exception:
            _circuit_retry_after = time.time() + 600
    else:
        _circuit_retry_after = time.time() + 600
    _circuit_open = True
    wait_min = int((_circuit_retry_after - time.time()) / 60)
    logger.error(f"[circuit] 429 — circuit abierto por ~{wait_min} min.")


def gmail_call(func, max_retries=1, base_wait=15):
    if circuit_is_open():
        raise RuntimeError("Circuit breaker abierto.")
    for attempt in range(max_retries + 1):
        try:
            result = func()
            time.sleep(GMAIL_THROTTLE)
            return result
        except HttpError as e:
            if e.resp.status == 429:
                if attempt < max_retries:
                    # Calcular espera exacta desde Retry-After
                    m = re.search(r"Retry after (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.\d]*Z)", str(e))
                    if m:
                        try:
                            dt = datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
                            wait = max(0, (dt - datetime.now(tz=timezone.utc)).total_seconds()) + 5
                            wait = min(wait, 60)
                        except Exception:
                            wait = base_wait * (2 ** attempt)
                    else:
                        wait = base_wait * (2 ** attempt)
                    logger.warning(f"[gmail] 429 — esperando {wait:.0f}s...")
                    time.sleep(wait)
                else:
                    circuit_trip(str(e))
                    raise
            else:
                raise


# ---------------------------------------------------------------------------
# Cache de labels
# ---------------------------------------------------------------------------
_label_map_cache: dict = {}
_label_map_ts: float   = 0
LABEL_MAP_TTL          = 3600


def get_label_map(service) -> dict:
    global _label_map_cache, _label_map_ts
    now = time.time()
    if _label_map_cache and (now - _label_map_ts) < LABEL_MAP_TTL:
        return _label_map_cache
    with _cache_lock:
        if _label_map_cache and (now - _label_map_ts) < LABEL_MAP_TTL:
            return _label_map_cache
        results = gmail_call(lambda: service.users().labels().list(userId="me").execute())
        _label_map_cache = {lbl["name"].upper(): lbl["id"] for lbl in results.get("labels", [])}
        _label_map_ts = now
        logger.info(f"Label map: {len(_label_map_cache)} etiquetas.")
    return _label_map_cache


def get_or_create_label(service, label_name: str, label_map: dict) -> str | None:
    lid = label_map.get(label_name.upper())
    if lid:
        return lid
    try:
        result = gmail_call(lambda: service.users().labels().create(
            userId="me",
            body={"name": label_name, "labelListVisibility": "labelHide",
                  "messageListVisibility": "hide"}
        ).execute())
        lid = result["id"]
        label_map[label_name.upper()] = lid
        logger.info(f"Label '{label_name}' creado: {lid}")
        return lid
    except Exception as e:
        logger.error(f"No se pudo crear label '{label_name}': {e}")
        return None


def mark_as_processed(service, msg_id: str, processed_label_id: str):
    try:
        gmail_call(lambda: service.users().messages().modify(
            userId="me", id=msg_id,
            body={"addLabelIds": [processed_label_id]}
        ).execute())
    except Exception as e:
        logger.warning(f"No se pudo marcar {msg_id} como procesado: {e}")


# ---------------------------------------------------------------------------
# Gmail helpers
# ---------------------------------------------------------------------------
def list_new_messages(service, label_id: str) -> list:
    """Lista solo mensajes sin label PROCESADO."""
    messages, page_token = [], None
    while True:
        if circuit_is_open():
            raise RuntimeError("Circuit breaker abierto.")
        resp = gmail_call(lambda: service.users().messages().list(
            userId="me",
            q="has:attachment -label:PROCESADO",
            labelIds=[label_id],
            pageToken=page_token
        ).execute())
        messages.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return messages


def list_zelle_messages(service, label_id: str) -> list:
    """Lista correos Zelle nuevos (no necesitan adjunto)."""
    messages, page_token = [], None
    # Construir query de remitentes Zelle
    senders = [s for ss in ZELLE_SENDERS.values() for s in ss]
    from_q  = " OR ".join(f"from:{s}" for s in senders)
    query   = f"({from_q}) -label:PROCESADO"
    while True:
        if circuit_is_open():
            raise RuntimeError("Circuit breaker abierto.")
        resp = gmail_call(lambda: service.users().messages().list(
            userId="me", q=query, labelIds=[label_id], pageToken=page_token
        ).execute())
        messages.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return messages


def get_full_message(service, msg_id: str) -> dict:
    return gmail_call(lambda: service.users().messages().get(
        userId="me", id=msg_id, format="full"
    ).execute())


def get_message_date(msg: dict) -> str:
    """Extrae la fecha del mensaje en formato YYYY/MM/DD para la ruta GCS."""
    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    date_str = headers.get("date", "")
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y/%m/%d")
    except Exception:
        return datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")


def get_message_sender(msg: dict) -> str:
    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    return headers.get("from", "").lower()


# ---------------------------------------------------------------------------
# GCS upload
# ---------------------------------------------------------------------------
def upload_to_gcs(gcs_client, raw_bytes: bytes, gcs_path: str) -> bool:
    """Sube archivo a GCS. Retorna True si se subió, False si ya existía."""
    try:
        bucket = gcs_client.bucket(GCS_BUCKET)
        blob   = bucket.blob(gcs_path)
        if blob.exists():
            logger.info(f"  GCS ya existe: {gcs_path}")
            return False
        blob.upload_from_string(raw_bytes)
        logger.info(f"  GCS: {gcs_path}")
        return True
    except Exception as e:
        logger.error(f"  GCS error en {gcs_path}: {e}")
        return False


def walk_parts(parts):
    """Iterador recursivo sobre partes del mensaje."""
    for part in parts:
        yield part
        if "parts" in part:
            yield from walk_parts(part["parts"])


# ---------------------------------------------------------------------------
# Parser Zelle — Chase
# ---------------------------------------------------------------------------
def parse_zelle_chase(body_text: str, msg_id: str, etiqueta: str, msg_date: str) -> dict | None:
    """
    Parsea body de Chase Zelle.
    Formato: 'NAME sent you money ... Amount $XX.XX ... Sent on Mon DD, YYYY ... Transaction number NNNN'
    """
    # Nombre del emisor
    sender_m = re.search(r"(.+?)\s+sent you money", body_text, re.IGNORECASE)
    # Monto
    amount_m = re.search(r"Amount\s*\$\s*([\d,]+\.?\d*)", body_text, re.IGNORECASE)
    # Fecha
    date_m   = re.search(r"Sent on\s+([A-Za-z]{3}\s+\d{1,2},\s+\d{4})", body_text, re.IGNORECASE)
    # Transaction number
    txn_m    = re.search(r"Transaction number\s+(\S+)", body_text, re.IGNORECASE)
    # Memo
    memo_m   = re.search(r"Memo\s+(.+?)(?:\n|$)", body_text, re.IGNORECASE)

    if not sender_m or not amount_m:
        return None

    # Parsear fecha
    fecha_str = None
    if date_m:
        try:
            dt = datetime.strptime(date_m.group(1).strip(), "%b %d, %Y")
            fecha_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    monto_str = amount_m.group(1).replace(",", "")
    try:
        monto = float(monto_str)
    except ValueError:
        return None

    return {
        "id":          str(uuid.uuid4()),
        "banco":       "CHASE",
        "emisor":      sender_m.group(1).strip().upper(),
        "monto_usd":   monto,
        "fecha":       fecha_str,
        "referencia":  txn_m.group(1).strip() if txn_m else None,
        "memo":        (memo_m.group(1).strip() if memo_m else None) or None,
        "etiqueta":    etiqueta,
        "gmail_msg_id":msg_id,
        "processed_at":datetime.now(tz=timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Parser Zelle — Wells Fargo
# ---------------------------------------------------------------------------
def parse_zelle_wellsfargo(body_text: str, msg_id: str, etiqueta: str, msg_date: str) -> dict | None:
    """
    Parsea body de Wells Fargo Zelle.
    Formato: 'NOMBRE le envió $XX.XX ... Fecha: MM/DD/YYYY ... Confirmación: XXXXX ... Notas: ...'
    """
    # Nombre + monto en una sola línea
    sender_m = re.search(r"(.+?)\s+le envi[oó]\s+\$\s*([\d,]+\.?\d*)", body_text, re.IGNORECASE)
    # Fecha
    date_m   = re.search(r"Fecha:\s*(\d{2}/\d{2}/\d{4})", body_text, re.IGNORECASE)
    # Confirmación
    conf_m   = re.search(r"Confirmaci[oó]n:\s*(\S+)", body_text, re.IGNORECASE)
    # Notas
    notes_m  = re.search(r"Notas:\s*(.+?)(?:\n|$)", body_text, re.IGNORECASE)

    if not sender_m:
        return None

    monto_str = sender_m.group(2).replace(",", "")
    try:
        monto = float(monto_str)
    except ValueError:
        return None

    fecha_str = None
    if date_m:
        try:
            # Formato MM/DD/YYYY
            dt = datetime.strptime(date_m.group(1).strip(), "%m/%d/%Y")
            fecha_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return {
        "id":          str(uuid.uuid4()),
        "banco":       "WELLS_FARGO",
        "emisor":      sender_m.group(1).strip().upper(),
        "monto_usd":   monto,
        "fecha":       fecha_str,
        "referencia":  conf_m.group(1).strip() if conf_m else None,
        "memo":        (notes_m.group(1).strip() if notes_m else None) or None,
        "etiqueta":    etiqueta,
        "gmail_msg_id":msg_id,
        "processed_at":datetime.now(tz=timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Parser Zelle — Bank of America
# ---------------------------------------------------------------------------
def parse_zelle_bofa(body_text: str, msg_id: str, etiqueta: str, msg_date: str) -> dict | None:
    """
    Parsea body de BofA Zelle.
    Formato: 'NOMBRE le envió $XX.XX' (en el heading del HTML, sin número de confirmación).
    La fecha se toma del header Date del correo (msg_date = YYYY/MM/DD).
    """
    sender_m = re.search(r"(.+?)\s+le envi[oó]\s+\$\s*([\d,]+\.?\d*)", body_text, re.IGNORECASE)
    if not sender_m:
        return None

    monto_str = sender_m.group(2).replace(",", "")
    try:
        monto = float(monto_str)
    except ValueError:
        return None

    fecha_str = None
    if msg_date:
        try:
            dt = datetime.strptime(msg_date, "%Y/%m/%d")
            fecha_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return {
        "id":           str(uuid.uuid4()),
        "banco":        "BOFA",
        "emisor":       sender_m.group(1).strip().upper(),
        "monto_usd":    monto,
        "fecha":        fecha_str,
        "referencia":   None,  # BofA Zelle alerts no incluyen número de confirmación
        "memo":         None,
        "etiqueta":     etiqueta,
        "gmail_msg_id": msg_id,
        "processed_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def parse_zelle_body(body_text: str, sender_email: str, msg_id: str, etiqueta: str, msg_date: str) -> dict | None:
    """Dispatcher por banco según el remitente."""
    sender_lower = sender_email.lower()
    if any(s in sender_lower for s in ZELLE_SENDERS["chase"]):
        return parse_zelle_chase(body_text, msg_id, etiqueta, msg_date)
    elif any(s in sender_lower for s in ZELLE_SENDERS["wellsfargo"]):
        return parse_zelle_wellsfargo(body_text, msg_id, etiqueta, msg_date)
    elif any(s in sender_lower for s in ZELLE_SENDERS["bofa"]):
        return parse_zelle_bofa(body_text, msg_id, etiqueta, msg_date)
    return None


def extract_body_text(msg: dict) -> str:
    """Extrae texto plano del mensaje, limpiando HTML si es necesario."""
    try:
        from bs4 import BeautifulSoup
        bs4_available = True
    except ImportError:
        bs4_available = False

    body_plain = ""
    body_html  = ""

    for part in walk_parts(msg.get("payload", {}).get("parts", [msg.get("payload", {})])):
        ct = part.get("mimeType", "")
        data = part.get("body", {}).get("data", "")
        if not data:
            continue
        decoded = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
        if ct == "text/plain" and not body_plain:
            body_plain = decoded
        elif ct == "text/html" and not body_html:
            body_html = decoded

    if body_plain.strip():
        return body_plain

    # Fallback a HTML
    if body_html and bs4_available:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(body_html, "html.parser")
        text = soup.get_text(separator="\n")
        return re.sub(r'\n{3,}', '\n\n', text).strip()

    return re.sub(r'<[^>]+>', ' ', body_html)


# ---------------------------------------------------------------------------
# Carga Zelle a BQ
# ---------------------------------------------------------------------------
def load_zelle_to_bq(records: list, bq_client) -> int:
    if not records:
        return 0
    buffer = io.BytesIO()
    for r in records:
        buffer.write((json.dumps(r, default=str, ensure_ascii=False) + "\n").encode())
    buffer.seek(0)
    job = bq_client.load_table_from_file(
        buffer, f"{BQ_PROJECT}.{BQ_TABLE_ZELLE}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        )
    )
    job.result()
    logger.info(f"[zelle] {job.output_rows} registros cargados en BQ.")
    return job.output_rows


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
# Blueprint — registrado en main.py
# ---------------------------------------------------------------------------
ingest_bp = Blueprint("ingest", __name__)

# Clientes GCP — inyectados desde main.py via init_ingest()
_bq  = None
_gcs = None


def init_ingest(bq_client, gcs_client):
    """Llamado desde main.py para inyectar los clientes compartidos."""
    global _bq, _gcs
    _bq  = bq_client
    _gcs = gcs_client


@ingest_bp.route("/ingest", methods=["POST"])
def ingest():
    run_id    = str(uuid.uuid4())
    inicio_ts = datetime.now(tz=timezone.utc)
    body      = request.get_json(silent=True) or {}
    etiquetas = body.get("etiquetas", ETIQUETAS)

    logger.info(f"=== INGEST run_id={run_id} | etiquetas={etiquetas} ===")

    try:
        service   = get_gmail_service()
        label_map = get_label_map(service)
    except Exception as e:
        logger.error(f"Auth Gmail falló: {e}")
        return jsonify({"run_id": run_id, "error": str(e)}), 500

    processed_label_id = get_or_create_label(service, LABEL_PROCESADO, label_map)

    stats = {
        "archivos_subidos": 0,
        "archivos_ya_existian": 0,
        "zelle_parseados": 0,
        "zelle_cargados": 0,
        "correos_procesados": 0,
        "errores": [],
    }
    zelle_records = []

    for etiqueta in etiquetas:
        if circuit_is_open():
            stats["errores"].append("Circuit breaker abierto — abortado.")
            break

        label_id = label_map.get(etiqueta.upper())
        if not label_id:
            logger.warning(f"Label '{etiqueta}' no encontrada.")
            continue

        # ── 1. Adjuntos fiscales → GCS ──────────────────────────────────
        try:
            messages = list_new_messages(service, label_id)
        except Exception as e:
            stats["errores"].append(f"{etiqueta} list_messages: {e}")
            continue

        logger.info(f"[{etiqueta}] {len(messages)} correos con adjuntos nuevos.")

        for msg_meta in messages:
            if circuit_is_open():
                stats["errores"].append("Circuit breaker — abortado.")
                break

            msg_id = msg_meta["id"]
            try:
                msg      = get_full_message(service, msg_id)
                msg_date = get_message_date(msg)
                uploaded = False

                for part in walk_parts(msg.get("payload", {}).get("parts", [])):
                    filename = part.get("filename", "")
                    if not filename:
                        continue
                    fu  = filename.upper()
                    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""

                    # Solo procesar extensiones conocidas
                    if ext not in ("txt", "xlsx", "xls", "json"):
                        continue

                    att_id = part.get("body", {}).get("attachmentId")
                    if not att_id:
                        continue

                    att = gmail_call(lambda: service.users().messages().attachments().get(
                        userId="me", messageId=msg_id, id=att_id
                    ).execute())
                    raw_bytes = base64.urlsafe_b64decode(att["data"])

                    # Ruta GCS: ETIQUETA/YYYY/MM/DD/filename
                    gcs_path = f"{etiqueta}/{msg_date}/{filename}"
                    result   = upload_to_gcs(_gcs, raw_bytes, gcs_path)
                    if result:
                        stats["archivos_subidos"] += 1
                        uploaded = True
                    else:
                        stats["archivos_ya_existian"] += 1

                # Marcar como procesado solo si hubo adjuntos relevantes
                if uploaded and processed_label_id:
                    mark_as_processed(service, msg_id, processed_label_id)
                    stats["correos_procesados"] += 1

            except Exception as e:
                stats["errores"].append(f"msg_id={msg_id}: {e}")

            time.sleep(PAUSE_S)

        # ── 2. Correos Zelle → BQ ────────────────────────────────────────
        try:
            zelle_msgs = list_zelle_messages(service, label_id)
        except Exception as e:
            stats["errores"].append(f"{etiqueta} zelle list: {e}")
            continue

        logger.info(f"[{etiqueta}] {len(zelle_msgs)} correos Zelle nuevos.")

        for msg_meta in zelle_msgs:
            if circuit_is_open():
                stats["errores"].append("Circuit breaker — abortado.")
                break

            msg_id = msg_meta["id"]
            try:
                msg          = get_full_message(service, msg_id)
                msg_date     = get_message_date(msg)
                sender_email = get_message_sender(msg)
                body_text    = extract_body_text(msg)

                record = parse_zelle_body(body_text, sender_email, msg_id, etiqueta, msg_date)
                if record:
                    zelle_records.append(record)
                    stats["zelle_parseados"] += 1
                    logger.info(f"  Zelle: {record['banco']} | {record['emisor']} | ${record['monto_usd']} | {record['fecha']}")

                # Marcar siempre como procesado (aunque no se parsee)
                if processed_label_id:
                    mark_as_processed(service, msg_id, processed_label_id)
                    stats["correos_procesados"] += 1

            except Exception as e:
                stats["errores"].append(f"zelle msg_id={msg_id}: {e}")

            time.sleep(PAUSE_S)

    # ── 3. Cargar Zelle a BQ ─────────────────────────────────────────────
    try:
        stats["zelle_cargados"] = load_zelle_to_bq(zelle_records, _bq)
    except Exception as e:
        stats["errores"].append(f"Zelle load BQ: {e}")

    duracion = int((datetime.now(tz=timezone.utc) - inicio_ts).total_seconds())
    status   = "OK" if not stats["errores"] else "PARCIAL"

    logger.info(
        f"=== FIN INGEST run_id={run_id} | status={status} | {duracion}s | "
        f"subidos={stats['archivos_subidos']} zelle={stats['zelle_cargados']} ==="
    )

    return jsonify({
        "run_id": run_id, "status": status,
        "duracion_s": duracion, **stats,
    }), 200 if status == "OK" else 207