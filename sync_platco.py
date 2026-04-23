# =============================================================================
# sync_platco.py — Banco Mercantil / Platco
# CSV: TransaccionesRealizadas*.csv
# Tabla BQ: mailer_raw.mailer_platco_raw
#
# Estructura CSV (encoding latin-1):
#   Row 0: "Banco Mercantil - Transacciones realizadas,<RIF>"
#   Row 1: "<Empresa>"
#   Row 2: (vacío)
#   Row 3: columnas (Terminal, Lote/Resumen, Secuencia, Fecha sesión,
#           Tipo transacción, Tarjeta, Número Autorización, Fecha transacción,
#           Tipo captura, Importe transacción, Importe Total Dscto,
#           Retención ISLR, Estado, Fecha abono, Importe abonado)
#   Rows 4..n-1: datos
#   Row n: "Totales,..."
# =============================================================================

import csv
import io
import json
import logging
import time
from datetime import datetime, timezone

from google.cloud import bigquery
from utils import list_messages, get_attachments, mark_as_processed, circuit_is_open

logger = logging.getLogger(__name__)

BQ_PROJECT      = "gestion-365"
BQ_TABLE_PLATCO = "mailer_raw.mailer_platco_raw"
PAUSE_S         = 5


def _parse_fecha(raw):
    """'DD/MM/YYYY' → 'YYYY-MM-DD'. None si falla."""
    if not raw:
        return None
    raw = str(raw).strip()
    try:
        return datetime.strptime(raw, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return raw


def _sf(v):
    """String → float, None si vacío o no numérico."""
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def parse_platco(raw_bytes, filename, msg_id, etiqueta):
    """Parsea CSV de Platco/Mercantil. Retorna lista de dicts."""
    # Intentar latin-1; fallback utf-8 con replace
    try:
        text = raw_bytes.decode("latin-1")
    except Exception:
        text = raw_bytes.decode("utf-8", errors="replace")

    reader = csv.reader(io.StringIO(text))
    rows   = list(reader)

    records = []
    # Filas de datos: índice 4 en adelante, saltar última si empieza con "Totales"
    data_rows = rows[4:]
    if data_rows and str(data_rows[-1][0]).strip().lower().startswith("totales"):
        data_rows = data_rows[:-1]

    for i, row in enumerate(data_rows):
        if not row or not row[0].strip():
            continue
        try:
            records.append({
                "terminal":          str(row[0]).strip(),
                "nro_lote":          str(row[1]).strip(),
                "secuencia":         str(row[2]).strip(),
                "fecha_sesion":      _parse_fecha(row[3]),
                "tipo_transaccion":  str(row[4]).strip() if len(row) > 4 else None,
                "tarjeta":           str(row[5]).strip().lstrip("'") if len(row) > 5 else None,
                "nro_autorizacion":  str(row[6]).strip() if len(row) > 6 else None,
                "fecha_transaccion": _parse_fecha(row[7]) if len(row) > 7 else None,
                "tipo_captura":      str(row[8]).strip() if len(row) > 8 else None,
                "monto":             _sf(row[9])  if len(row) > 9  else None,
                "descuento":         _sf(row[10]) if len(row) > 10 else None,
                "retencion_islr":    _sf(row[11]) if len(row) > 11 else None,
                "estado":            str(row[12]).strip() if len(row) > 12 else None,
                "fecha_abono":       _parse_fecha(row[13]) if len(row) > 13 else None,
                "monto_abonado":     _sf(row[14]) if len(row) > 14 else None,
                "etiqueta":          etiqueta,
                "filename":          filename,
                "gmail_msg_id":      msg_id,
                "processed_at":      datetime.now(tz=timezone.utc).isoformat(),
            })
        except Exception as e:
            logger.warning(f"[sync_platco] {filename} fila {i}: {e}")

    logger.info(f"[sync_platco] {filename}: {len(records)} filas.")
    return records


def _load_to_bq(records, bq_client):
    if not records:
        return 0
    buffer = io.BytesIO()
    for r in records:
        buffer.write((json.dumps(r, default=str, ensure_ascii=False) + "\n").encode())
    buffer.seek(0)
    job = bq_client.load_table_from_file(
        buffer, f"{BQ_PROJECT}.{BQ_TABLE_PLATCO}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        )
    )
    job.result()
    logger.info(f"[sync_platco] {job.output_rows} filas cargadas.")
    return job.output_rows


def sync_platco(service, bq_client, etiquetas, label_map=None, processed_label_id=None):
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
        logger.info(f"[sync_platco][{etiqueta}] {len(messages)} correos nuevos.")

        for msg_meta in messages:
            if circuit_is_open():
                stats["errores"].append("Circuit breaker abierto — abortado.")
                break
            msg_id = msg_meta["id"]
            try:
                atts = get_attachments(
                    service, msg_id,
                    keyword="transaccionesrealizadas",
                    extensions=(".csv",)
                )
                for att in atts:
                    try:
                        records = parse_platco(
                            att["raw_bytes"], att["filename"], att["msg_id"], etiqueta
                        )
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

    logger.info(
        f"[sync_platco] procesados={stats['procesados']} "
        f"cargados={stats['cargados']} errores={len(stats['errores'])}"
    )
    return stats
