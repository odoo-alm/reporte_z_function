# =============================================================================
# sync_ubii.py  v3 — usa utils.py
# =============================================================================

import io
import json
import logging
import time
from datetime import datetime, timezone

import openpyxl
from google.cloud import bigquery
from utils import list_messages, get_attachments, mark_as_processed, circuit_is_open

logger = logging.getLogger(__name__)

BQ_PROJECT    = "gestion-365"
BQ_TABLE_UBII = "mailer_raw.mailer_ubii_raw"
PAUSE_S       = 5

def parse_ubii(raw_bytes, filename, msg_id, etiqueta):
    sf = lambda v: float(v) if v is not None else None
    ss = lambda v: str(v).strip() if v is not None else None
    wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), read_only=True)
    ws = wb[wb.sheetnames[0]]
    records = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0 or row[0] is None:
            continue
        try:
            fecha_raw = row[0]
            records.append({
                "fecha_cierre": fecha_raw.strftime("%Y-%m-%d") if hasattr(fecha_raw, "strftime") else ss(fecha_raw),
                "terminal": ss(row[1]), "oficina": ss(row[2]),
                "nro_lote": int(row[3]) if row[3] is not None else None,
                "tipo": ss(row[4]), "inmediata": ss(row[5]),
                "monto": sf(row[6]), "monto_tdc": sf(row[7]), "monto_tdd": sf(row[8]),
                "comision_tdc": sf(row[9]), "comision_islr": sf(row[10]), "comision_tdd": sf(row[11]),
                "comision_transferencia": sf(row[12]), "comision_inmediata": sf(row[13]),
                "comision_fee": sf(row[14]), "monto_cuota_credito": sf(row[15]),
                "monto_gastos_admin_credito": sf(row[16]), "monto_gastos_reactivacion_credito": sf(row[17]),
                "monto_nota_debito": sf(row[18]), "monto_ajuste_liquidacion": sf(row[19]),
                "monto_a_liquidar": sf(row[20]), "monto_tdc_visa": sf(row[21]),
                "comision_tdc_visa": sf(row[22]), "islr_tdc_visa": sf(row[23]),
                "comision_transferencia_visa": sf(row[24]), "monto_a_liquidar_visa": sf(row[25]),
                "etiqueta": etiqueta, "filename": filename,
                "gmail_msg_id": msg_id, "processed_at": datetime.now(tz=timezone.utc).isoformat(),
            })
        except Exception as e:
            logger.warning(f"[sync_ubii] {filename}: fila {i} omitida — {e}")
    wb.close()
    logger.info(f"[sync_ubii] {filename}: {len(records)} filas.")
    return records

def _load_to_bq(records, bq_client):
    if not records:
        return 0
    buffer = io.BytesIO()
    for r in records:
        buffer.write((json.dumps(r, default=str, ensure_ascii=False) + "\n").encode())
    buffer.seek(0)
    job = bq_client.load_table_from_file(
        buffer, f"{BQ_PROJECT}.{BQ_TABLE_UBII}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        )
    )
    job.result()
    logger.info(f"[sync_ubii] {job.output_rows} filas cargadas.")
    return job.output_rows

def sync_ubii(service, bq_client, etiquetas, label_map=None, processed_label_id=None):
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
        logger.info(f"[sync_ubii][{etiqueta}] {len(messages)} correos nuevos.")
        for msg_meta in messages:
            if circuit_is_open():
                stats["errores"].append("Circuit breaker abierto — abortado.")
                break
            msg_id = msg_meta["id"]
            try:
                atts = get_attachments(service, msg_id, keyword="LIQUIDACION", extensions=(".xlsx", ".xls"))
                for att in atts:
                    try:
                        records = parse_ubii(att["raw_bytes"], att["filename"], att["msg_id"], etiqueta)
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
    logger.info(f"[sync_ubii] procesados={stats['procesados']} cargados={stats['cargados']} errores={len(stats['errores'])}")
    return stats