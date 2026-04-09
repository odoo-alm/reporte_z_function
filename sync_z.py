# =============================================================================
# sync_z.py  v3 — usa utils.py
# =============================================================================

import io
import json
import logging
import re
import time
from datetime import datetime, timezone

from google.cloud import bigquery
from utils import list_messages, get_attachments, mark_as_processed, circuit_is_open

logger = logging.getLogger(__name__)

BQ_PROJECT = "gestion-365"
BQ_TABLE_Z = "mailer_raw.mailer_z_received"
PAUSE_S    = 5

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

def _extract_doc_datetime(doc_label, content):
    pat = rf"{re.escape(doc_label)}\s+\d+\s*\r?\nFECHA:\s+(\d{{2}}-\d{{2}}-\d{{4}})\s+HORA:\s+(\d{{2}}:\d{{2}})"
    m = re.search(pat, content)
    return (m.group(1), m.group(2)) if m else (None, None)

def _extract_section(content, title, next_titles):
    start_m = re.search(rf"^\s*{re.escape(title)}\s*$", content, re.MULTILINE | re.IGNORECASE)
    if not start_m:
        return ""
    start_pos = start_m.end()
    end_pos = len(content)
    for nt in next_titles:
        end_m = re.search(rf"^\s*{re.escape(nt)}\s*$", content[start_pos:], re.MULTILINE | re.IGNORECASE)
        if end_m:
            end_pos = min(end_pos, start_pos + end_m.start())
    return content[start_pos:end_pos]

def _bs_in_section(block, label):
    return _parse_bs(_search(rf"^\s*{re.escape(label)}\s+Bs\s*([\-\d.,]+)", block))

def _parse_section(block, prefix=""):
    p = prefix
    return {
        f"{p}exento":    _bs_in_section(block, f"{p}EXENTO"),
        f"{p}bi_g16":    _bs_in_section(block, f"{p}BI G (16,00%)"),
        f"{p}iva_g16":   _bs_in_section(block, f"{p}IVA G (16,00%)"),
        f"{p}bi_r8":     _bs_in_section(block, f"{p}BI R (8,00%)"),
        f"{p}iva_r8":    _bs_in_section(block, f"{p}IVA R (8,00%)"),
        f"{p}bi_a31":    _bs_in_section(block, f"{p}BI A (31,00%)"),
        f"{p}iva_a31":   _bs_in_section(block, f"{p}IVA A (31,00%)"),
        f"{p}percibido": _bs_in_section(block, f"{p}PERCIBIDO"),
    }

def parse_repz(content, filename, msg_id, etiqueta):
    if not re.search(r"REPORTE Z:", content):
        return None
    id_reporte_z     = _search(r"REPORTE Z:\s+(\d+)", content)
    serial_impresora = _search(r"^MH\s+(\S+)", content)
    fecha_raw        = _search(r"FECHA:\s+(\d{2}-\d{2}-\d{4})", content)
    hora_raw         = _search(r"HORA:\s+(\d{2}:\d{2})", content)
    if not all([id_reporte_z, serial_impresora, fecha_raw]):
        return None
    try:
        fecha_reporte = datetime.strptime(fecha_raw, "%d-%m-%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None

    num_facturas = _search(r"#FACT DEL DIA\s+(\d+)", content)
    num_dnf      = _search(r"#DNF DEL DIA\s+(\d+)", content)
    num_nc       = _search(r"#NC DEL DIA\s+(\d+)", content)

    medios_pago = {}
    for m in re.finditer(r"^(.+?)\s*\(#(\d+)\)\s+Bs\s*([\-\d.,]+)", content, re.MULTILINE):
        medios_pago[m.group(1).strip()] = {"count": int(m.group(2)), "amount": _parse_bs(m.group(3))}

    f_uf,   h_uf   = _extract_doc_datetime("ULTIMA FACTURA",   content)
    f_und,  h_und  = _extract_doc_datetime("ULT.NOTA.DEBITO",  content)
    f_unc,  h_unc  = _extract_doc_datetime("ULT.NOTA.CREDITO", content)
    f_udnf, h_udnf = _extract_doc_datetime("ULTIMO DNF",       content)
    f_urmf, h_urmf = _extract_doc_datetime("ULTIMO RMF",       content)

    sec_recargos     = _extract_section(content,            "RECARGOS",         ["DESCUENTOS"])
    sec_descuentos   = _extract_section(content,            "DESCUENTOS",       ["ANULACIONES"])
    sec_anulaciones  = _extract_section(content,            "ANULACIONES",      ["CORRECCIONES"])
    sec_correcciones = _extract_section(content,            "CORRECCIONES",     ["VENTAS"])
    pos_corr         = content.find("CORRECCIONES")
    sec_ventas       = _extract_section(content[pos_corr:], "VENTAS",           ["NOTAS DE DEBITO"]) if pos_corr >= 0 else ""
    sec_nd           = _extract_section(content,            "NOTAS DE DEBITO",  ["NOTAS DE CREDITO"])
    sec_nc           = _extract_section(content,            "NOTAS DE CREDITO", ["ULTIMA FACTURA"])

    d_rec  = _parse_section(sec_recargos)
    d_desc = _parse_section(sec_descuentos)
    d_anul = _parse_section(sec_anulaciones)
    d_corr = _parse_section(sec_correcciones)
    d_vent = _parse_section(sec_ventas)
    d_nd   = _parse_section(sec_nd, prefix="ND.")
    d_nc   = _parse_section(sec_nc, prefix="NC.")

    return {
        "id_reporte_z": id_reporte_z, "serial_impresora": serial_impresora,
        "fecha_reporte": fecha_reporte, "hora_reporte": hora_raw,
        "total_gaveta": _bs(r"TOTAL GAVETA\s+Bs\s*([\-\d.,]+)", content),
        "num_facturas": int(num_facturas) if num_facturas else None,
        "num_dnf": int(num_dnf) if num_dnf else None,
        "num_nc":  int(num_nc)  if num_nc  else None,
        **{f"recargos_{k}": v for k, v in d_rec.items()},
        "subtotal_recargos": _bs_in_section(sec_recargos, "SUBTTL RECARGOS"),
        "iva_recargos": _bs_in_section(sec_recargos, "IVA RECARGOS"),
        "total_recargos": _bs_in_section(sec_recargos, "TOTAL RECARGOS"),
        **{f"descuentos_{k}": v for k, v in d_desc.items()},
        "subtotal_descuentos": _bs_in_section(sec_descuentos, "SUBTTL DESCUENTOS"),
        "iva_descuentos": _bs_in_section(sec_descuentos, "IVA DESCUENTOS"),
        "total_descuentos": _bs_in_section(sec_descuentos, "TOTAL DESCUENTOS"),
        **{f"anulaciones_{k}": v for k, v in d_anul.items()},
        "subtotal_anulaciones": _bs_in_section(sec_anulaciones, "SUBTTL ANULACIONES"),
        "iva_anulaciones": _bs_in_section(sec_anulaciones, "IVA ANULACIONES"),
        "total_anulaciones": _bs_in_section(sec_anulaciones, "TOTAL ANULACIONES"),
        **{f"correcciones_{k}": v for k, v in d_corr.items()},
        "subtotal_correcciones": _bs_in_section(sec_correcciones, "SUBTTL CORRECCIONES"),
        "iva_correcciones": _bs_in_section(sec_correcciones, "IVA CORRECCIONES"),
        "total_correcciones": _bs_in_section(sec_correcciones, "TOTAL CORRECCIONES"),
        **{f"ventas_{k}": v for k, v in d_vent.items()},
        "subtotal_venta": _bs_in_section(sec_ventas, "SUBTTL VENTA"),
        "iva_venta": _bs_in_section(sec_ventas, "IVA VENTA"),
        "igtf_venta": _bs_in_section(sec_ventas, "IGTF VENTA (3,00%)"),
        "total_venta": _bs_in_section(sec_ventas, "TOTAL VENTA"),
        "bi_igtf": _bs_in_section(sec_ventas, "BI IGTF (3,00%)"),
        **{f"nd_{k.removeprefix('ND.')}": v for k, v in d_nd.items()},
        "subtotal_nota_debito": _bs_in_section(sec_nd, "SUBTTL NOTA DEBITO"),
        "iva_nota_debito": _bs_in_section(sec_nd, "IVA NOTA DEBITO"),
        "igtf_nota_debito": _bs_in_section(sec_nd, "IGTF NOTA DEBITO (3,00%)"),
        "total_nota_debito": _bs_in_section(sec_nd, "TOTAL NOTA DEBITO"),
        "nd_bi_igtf": _bs_in_section(sec_nd, "ND.BI IGTF (3,00%)"),
        **{f"nc_{k.removeprefix('NC.')}": v for k, v in d_nc.items()},
        "subtotal_nota_credito": _bs_in_section(sec_nc, "SUBTTL NOTA CREDITO"),
        "iva_nota_credito": _bs_in_section(sec_nc, "IVA NOTA CREDITO"),
        "igtf_nota_credito": _bs_in_section(sec_nc, "IGTF NOTA CREDITO (3,00%)"),
        "total_nota_credito": _bs_in_section(sec_nc, "TOTAL NOTA CREDITO"),
        "nc_bi_igtf": _bs_in_section(sec_nc, "NC.BI IGTF (3,00%)"),
        "ultima_factura": _search(r"ULTIMA FACTURA\s+(\d+)", content),
        "fecha_ultima_factura": f_uf, "hora_ultima_factura": h_uf,
        "ult_nota_debito": _search(r"ULT\.NOTA\.DEBITO\s+(\d+)", content),
        "fecha_ult_nota_debito": f_und, "hora_ult_nota_debito": h_und,
        "ult_nota_credito": _search(r"ULT\.NOTA\.CREDITO\s+(\d+)", content),
        "fecha_ult_nota_credito": f_unc, "hora_ult_nota_credito": h_unc,
        "ultimo_dnf": _search(r"ULTIMO DNF\s+(\d+)", content),
        "fecha_ultimo_dnf": f_udnf, "hora_ultimo_dnf": h_udnf,
        "ultimo_rmf": _search(r"ULTIMO RMF\s+(\d+)", content),
        "fecha_ultimo_rmf": f_urmf, "hora_ultimo_rmf": h_urmf,
        "medios_pago": str(medios_pago),
        "etiqueta": etiqueta, "filename": filename,
        "gmail_msg_id": msg_id,
        "processed_at": datetime.now(tz=timezone.utc).isoformat(),
    }

def _load_to_bq(records, bq_client):
    if not records:
        return 0
    buffer = io.BytesIO()
    for r in records:
        buffer.write((json.dumps(r, default=str, ensure_ascii=False) + "\n").encode())
    buffer.seek(0)
    job = bq_client.load_table_from_file(
        buffer, f"{BQ_PROJECT}.{BQ_TABLE_Z}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        )
    )
    job.result()
    logger.info(f"[sync_z] {job.output_rows} filas cargadas.")
    return job.output_rows

def sync_z(service, bq_client, etiquetas, label_map=None, processed_label_id=None):
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
        logger.info(f"[sync_z][{etiqueta}] {len(messages)} correos nuevos.")

        for msg_meta in messages:
            if circuit_is_open():
                stats["errores"].append("Circuit breaker abierto — abortado.")
                break
            msg_id = msg_meta["id"]
            try:
                atts = get_attachments(service, msg_id, prefix="REPZ", extensions=(".txt",))
                for att in atts:
                    try:
                        r = parse_repz(att["content"], att["filename"], att["msg_id"], etiqueta)
                        if r:
                            todos.append(r)
                            stats["procesados"] += 1
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

    logger.info(f"[sync_z] procesados={stats['procesados']} cargados={stats['cargados']} errores={len(stats['errores'])}")
    return stats