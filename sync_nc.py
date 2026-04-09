# =============================================================================
# sync_nc.py  v4 — usa utils.py
# =============================================================================

import io
import json
import logging
import re
import time
from datetime import datetime, timezone

from google.cloud import bigquery
from utils import gmail_call, list_messages, get_attachments, mark_as_processed, circuit_is_open

logger = logging.getLogger(__name__)

BQ_PROJECT  = "gestion-365"
BQ_TABLE_NC = "mailer_raw.mailer_nc_received"
PAUSE_S     = 5

MEDIOS_CONOCIDOS = sorted([
    "MEGA - MAN", "MEGA", "UBII", "EFEC - DOLAR", "EFEC - BS",
    "ZELLE", "CXC", "BANESCO - PM", "BANESCO - DEB",
    "BANPLUS - CRED", "BANPLUS - DEB", "MERCAN - PM", "SALDO",
], key=len, reverse=True)

def _search(pattern, text, group=1):
    m = re.search(pattern, text, re.MULTILINE)
    return m.group(group).strip() if m else None

def _parse_bs(raw):
    if raw is None:
        return None
    try:
        return float(str(raw).replace(".", "").replace(",", "."))
    except ValueError:
        return None

def _bs(pattern, text):
    return _parse_bs(_search(pattern, text))

def _extract_medios_pago(block):
    medios = []
    for line in block.split("\n"):
        ls = line.strip()
        if not ls:
            continue
        for medio in MEDIOS_CONOCIDOS:
            if ls.startswith(medio):
                m = re.match(rf"^{re.escape(medio)}\s+Bs\s*([\-\d.,]+)", ls)
                if m:
                    medios.append({"medio": medio, "monto": _parse_bs(m.group(1))})
                    break
    return str(medios)

def _parse_nc(block, filename, msg_id, etiqueta):
    num_documento = _search(r"NOTA DE CREDITO:\s+(\d+)", block)
    fecha_raw     = _search(r"FECHA:\s+(\d{2}-\d{2}-\d{4})", block)
    if not num_documento or not fecha_raw:
        return None
    try:
        fecha_doc = datetime.strptime(fecha_raw, "%d-%m-%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None
    bi_m = re.search(r"BI G(\d+,\d+)%\s+Bs\s*([\-\d.,]+)\s+IVA G\d+,\d+%\s+Bs\s*([\-\d.,]+)", block)
    return {
        "num_documento":      num_documento,
        "tipo_documento":     "NOTA DE CREDITO",
        "num_factura_origen": _search(r"#FAC:\s+(\d+)", block),
        "serial_control":     _search(r"#CONTROL/SERIAL IF:\s*(\S+)", block),
        "fecha_documento":    fecha_doc,
        "hora_documento":     _search(r"HORA:\s+(\d{2}:\d{2})", block),
        "rif_cliente":        (_search(r"RIF/C\.I\.:\s*(.+)", block) or "").strip() or None,
        "razon_social":       (_search(r"RAZON SOCIAL:\s*(.+)", block) or "").strip() or None,
        "serial_impresora":   _search(r"^MH\s+(\S+)", block),
        "subtotal":           _bs(r"SUBTTL\s+Bs\s*([\-\d.,]+)", block),
        "base_imponible_16":  _parse_bs(bi_m.group(2)) if bi_m else None,
        "iva_16":             _parse_bs(bi_m.group(3)) if bi_m else None,
        "medios_pago":        _extract_medios_pago(block),
        "cambio":             _bs(r"CAMBIO\s+Bs\s*([\-\d.,]+)", block),
        "igtf":               _bs(r"IGTF\d+,\d+%\s+Bs\s*([\-\d.,]+)", block),
        "total":              _bs(r"TOTAL\s+Bs\s*([\-\d.,]+)", block),
        "etiqueta":           etiqueta,
        "filename":           filename,
        "gmail_msg_id":       msg_id,
        "processed_at":       datetime.now(tz=timezone.utc).isoformat(),
    }

def parse_notas_credito(content, filename, msg_id, etiqueta):
    bloques = [b for b in re.split(r"(?=\s+SENIAT\s+)", content)
               if b.strip() and re.search(r"NOTA DE CREDITO:", b)]
    records = []
    for bloque in bloques:
        r = _parse_nc(bloque, filename, msg_id, etiqueta)
        if r:
            records.append(r)
    logger.info(f"[sync_nc] {filename}: {len(records)} NC.")
    return records

def _load_to_bq(records, bq_client):
    if not records:
        return 0
    buffer = io.BytesIO()
    for r in records:
        buffer.write((json.dumps(r, default=str, ensure_ascii=False) + "\n").encode())
    buffer.seek(0)
    job = bq_client.load_table_from_file(
        buffer, f"{BQ_PROJECT}.{BQ_TABLE_NC}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        )
    )
    job.result()
    logger.info(f"[sync_nc] {job.output_rows} filas cargadas.")
    return job.output_rows

def sync_nc(service, bq_client, etiquetas, label_map=None, processed_label_id=None):
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
        logger.info(f"[sync_nc][{etiqueta}] {len(messages)} correos nuevos.")

        for msg_meta in messages:
            if circuit_is_open():
                stats["errores"].append("Circuit breaker abierto — abortado.")
                break
            msg_id = msg_meta["id"]
            try:
                atts = get_attachments(service, msg_id, prefix="NDC", extensions=(".txt",))
                if not atts:
                    atts = get_attachments(service, msg_id, prefix="NC", extensions=(".txt",))
                for att in atts:
                    try:
                        records = parse_notas_credito(att["content"], att["filename"], att["msg_id"], etiqueta)
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

    logger.info(f"[sync_nc] procesados={stats['procesados']} cargados={stats['cargados']} errores={len(stats['errores'])}")
    return stats