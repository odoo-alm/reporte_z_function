# =============================================================================
# sync_credicard.py — Gmail → BQ
# Adjunto: JSON exportado del portal BANPLUS/Credicard (mat-table-exporter.json)
# Tabla BQ: mailer_raw.mailer_credicard_raw
# =============================================================================

import base64
import io
import json
import logging
import time
from datetime import datetime, timezone

from google.cloud import bigquery
from utils import list_messages, mark_as_processed, circuit_is_open, gmail_call

logger = logging.getLogger(__name__)

BQ_PROJECT         = "gestion-365"
BQ_TABLE_CREDICARD = "mailer_raw.mailer_credicard_raw"
PAUSE_S            = 5

COL_MAP = {
    "0": "afiliado",
    "1": "fecha",
    "2": "lote",
    "3": "terminal",
    "4": "producto",
    "5": "banco",
    "6": "tarjeta",
    "7": "autorizacion",
    "8": "monto",
    "9": "comision",
    "10": "islr",
    "11": "abonado",
}


def _parse_bs(raw):
    """Convierte "17.668,15 Bs" → 17668.15"""
    if not raw:
        return None
    try:
        return float(str(raw).replace(" Bs", "").replace(".", "").replace(",", ".").strip())
    except ValueError:
        return None


def _parse_fecha(raw):
    """Convierte "28/02/2026" → "2026-02-28" """
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw).strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _get_json_attachments(service, msg_id):
    """Extrae adjuntos .json del mensaje sin filtro de nombre."""
    msg = gmail_call(lambda: service.users().messages().get(
        userId="me", id=msg_id, format="full"
    ).execute())

    def _walk(parts):
        for part in parts:
            fn = part.get("filename", "")
            if fn.lower().endswith(".json"):
                att_id = part.get("body", {}).get("attachmentId")
                if att_id:
                    att = gmail_call(lambda: service.users().messages().attachments().get(
                        userId="me", messageId=msg_id, id=att_id
                    ).execute())
                    yield {
                        "filename": fn,
                        "raw_bytes": base64.urlsafe_b64decode(att["data"]),
                        "msg_id": msg_id,
                    }
            if "parts" in part:
                yield from _walk(part["parts"])

    return list(_walk(msg.get("payload", {}).get("parts", [])))


def parse_credicard(raw_bytes, filename, msg_id, etiqueta):
    """
    Parsea el JSON exportado del portal BANPLUS/Credicard.
    Cada fila tiene claves "0"–"11". La última fila es el renglón de totales
    (afiliado vacío, tarjeta == "Total") y se descarta.
    """
    try:
        data = json.loads(raw_bytes.decode("utf-8", errors="replace"))
    except Exception as e:
        logger.error(f"[sync_credicard] {filename}: JSON inválido — {e}")
        return []

    records = []
    for row in data:
        # Descartar fila de totales
        if not row.get("0") or str(row.get("6", "")).strip() == "Total":
            continue
        try:
            records.append({
                "afiliado":     str(row["0"]).strip(),
                "fecha":        _parse_fecha(row.get("1")),
                "lote":         int(row["2"]) if row.get("2") else None,
                "terminal":     str(row["3"]).strip() if row.get("3") else None,
                "producto":     str(row["4"]).strip() if row.get("4") else None,
                "banco":        str(row["5"]).strip() if row.get("5") else None,
                "tarjeta":      str(row["6"]).strip() if row.get("6") else None,
                "autorizacion": str(row["7"]).strip() if row.get("7") else None,
                "monto":        _parse_bs(row.get("8")),
                "comision":     _parse_bs(row.get("9")),
                "islr":         _parse_bs(row.get("10")),
                "abonado":      _parse_bs(row.get("11")),
                "etiqueta":     etiqueta,
                "filename":     filename,
                "gmail_msg_id": msg_id,
                "processed_at": datetime.now(tz=timezone.utc).isoformat(),
            })
        except Exception as e:
            logger.warning(f"[sync_credicard] {filename}: fila omitida — {e}")

    logger.info(f"[sync_credicard] {filename}: {len(records)} filas.")
    return records


def _load_to_bq(records, bq_client):
    if not records:
        return 0
    buffer = io.BytesIO()
    for r in records:
        buffer.write((json.dumps(r, default=str, ensure_ascii=False) + "\n").encode())
    buffer.seek(0)
    job = bq_client.load_table_from_file(
        buffer, f"{BQ_PROJECT}.{BQ_TABLE_CREDICARD}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        )
    )
    job.result()
    logger.info(f"[sync_credicard] {job.output_rows} filas cargadas en BQ.")
    return job.output_rows


def sync_credicard(service, bq_client, etiquetas, label_map=None, processed_label_id=None):
    stats = {"procesados": 0, "cargados": 0, "errores": []}
    todos = []

    for etiqueta in etiquetas:
        if circuit_is_open():
            stats["errores"].append("Circuit breaker abierto — abortado.")
            break
        label_id = (label_map or {}).get(etiqueta.upper())
        if not label_id:
            continue
        messages = list_messages(service, label_id)
        logger.info(f"[sync_credicard][{etiqueta}] {len(messages)} correos nuevos.")

        for msg_meta in messages:
            if circuit_is_open():
                stats["errores"].append("Circuit breaker abierto — abortado.")
                break
            msg_id = msg_meta["id"]
            try:
                atts = _get_json_attachments(service, msg_id)
                for att in atts:
                    try:
                        records = parse_credicard(att["raw_bytes"], att["filename"], att["msg_id"], etiqueta)
                        todos.extend(records)
                        stats["procesados"] += len(records)
                    except Exception as e:
                        stats["errores"].append(f"{att['filename']}: {e}")
                if atts and processed_label_id:
                    mark_as_processed(service, msg_id, processed_label_id)
            except Exception as e:
                stats["errores"].append(f"msg_id={msg_id}: {e}")
            time.sleep(PAUSE_S)

    try:
        stats["cargados"] = _load_to_bq(todos, bq_client)
    except Exception as e:
        stats["errores"].append(f"Load BQ: {e}")

    logger.info(f"[sync_credicard] procesados={stats['procesados']} cargados={stats['cargados']} errores={len(stats['errores'])}")
    return stats
