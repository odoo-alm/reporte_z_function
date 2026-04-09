import re
import logging
from google.cloud import storage, bigquery
from datetime import datetime, timezone

# === CONFIGURACIÓN EXPLÍCITA ===
PROJECT_ID = "gestion-365"
BUCKET_NAME = "gestion-365-mailer-raw"
DATASET_TABLE = "mailer_raw.mailer_fact_received"

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def extraer_datos_robusto(block, filename):
    # Regex mejoradas para capturar después de los dos puntos y espacios
    patterns = {
        "num_factura": r"FACTURA:[\s]*(\d+)",
        "fecha_factura": r"FECHA:[\s]*(\d{2}-\d{2}-\d{4})",
        "hora_factura": r"HORA:[\s]*(\d{2}:\d{2})",
        "rif_cliente": r"RIF/C\.I\.:[\s]*([^\n\r]+)",
        "razon_social": r"RAZON SOCIAL:[\s]*([^\n\r]+)",
        "serial_impresora": r"SERIAL:[\s]*([^\n\r ]+)"
    }
    
    res = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, block, re.IGNORECASE)
        res[key] = match.group(1).strip() if match else None

    if not res["num_factura"] or not res["fecha_factura"]:
        return None

    try:
        f_bq = datetime.strptime(res["fecha_factura"], "%d-%m-%Y").strftime("%Y-%m-%d")
    except:
        return None

    return {
        "num_factura": res["num_factura"],
        "num_documento": res["num_factura"],
        "num_factura_origen": res["num_factura"],
        "fecha_factura": f_bq,
        "fecha_documento": f_bq,
        "hora_factura": res["hora_factura"],
        "hora_documento": res["hora_factura"],
        "rif_cliente": res["rif_cliente"] or "V-0",
        "razon_social": res["razon_social"] or "SIN NOMBRE",
        "serial_impresora": res["serial_impresora"],
        "tipo_documento": "FACTURA",
        "filename": filename,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "etiqueta": "CONTINGENCIA_FINAL_LOCAL",
        "total": 0.0, "subtotal": 0.0, "iva_16": 0.0, "igtf": 0.0
    }

def ejecutar_proceso():
    # PASAMOS EL PROJECT_ID EXPLÍCITAMENTE
    try:
        st_client = storage.Client(project=PROJECT_ID)
        bq_client = bigquery.Client(project=PROJECT_ID)
        bucket = st_client.bucket(BUCKET_NAME)
    except Exception as e:
        logger.error(f"Error al conectar con Google Cloud: {e}")
        return

    all_records = []
    logger.info(f"Escaneando archivos en gs://{BUCKET_NAME}...")
    
    blobs = bucket.list_blobs()
    for blob in blobs:
        if not blob.name.endswith(".txt"): continue
        
        try:
            content = blob.download_as_bytes().decode("utf-8", errors="replace")
            # Separación robusta por factura
            chunks = re.split(r"(?=FACTURA:)", content, flags=re.IGNORECASE)
            
            for chunk in chunks:
                if "FACTURA:" in chunk.upper():
                    record = extraer_datos_robusto(chunk, blob.name)
                    if record:
                        all_records.append(record)
            
            print(f"✅ Procesado: {blob.name} ({len(all_records)} registros acumulados)")

        except Exception as e:
            logger.error(f"Fallo en archivo {blob.name}: {e}")

    if all_records:
        logger.info(f"Insertando {len(all_records)} registros en BigQuery...")
        errors = bq_client.insert_rows_json(DATASET_TABLE, all_records)
        if not errors:
            print("\n" + "⭐" * 30 + "\n¡CARGA COMPLETADA CON ÉXITO!\n" + "⭐" * 30)
        else:
            logger.error(f"Errores en BQ: {errors}")
    else:
        logger.warning("No se extrajeron datos. Revisa el formato de los TXT.")

if __name__ == "__main__":
    ejecutar_proceso()