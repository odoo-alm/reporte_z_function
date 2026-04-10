# Contexto del Proyecto — reporte_z_function

## ¿Qué hace esto?

Servicio en **Cloud Run** (`mailer-pipeline`, `us-central1`, proyecto `gestion-365`) que extrae datos de correos Gmail, los almacena en GCS y los carga en BigQuery.

## Arquitectura

### Flujo principal
```
Gmail → /ingest (GCS) → /process (BigQuery)
Gmail → /sync (BigQuery directo)
```

### Endpoints

| Endpoint | Descripción |
|----------|-------------|
| `POST /ingest` | Lee correos por etiqueta Gmail, sube adjuntos a GCS |
| `POST /process` | Lee archivos de GCS, parsea e inserta en BQ |
| `POST /sync` | Lee correos Gmail, parsea e inserta en BQ directamente |
| `GET /health` | Health check |

### Etiquetas Gmail
`BARAKO`, `BRASERO`, `CARBON`, `OFICINA`

Cada correo procesado recibe el label `PROCESADO` en Gmail.

## Integraciones

| Módulo | Tipo archivo | Keyword clasificación | Tabla BQ |
|--------|-------------|----------------------|----------|
| Reporte Z | `.txt` (empieza con `REPZ`) | `startswith REPZ` | `mailer_raw.mailer_z_received` |
| Facturas | `.txt` (empieza con `FAC`) | `startswith FAC` | `mailer_raw.mailer_fact_received` |
| Notas de Crédito | `.txt` (empieza con `NC/NDC`) | `startswith NC/NDC` | `mailer_raw.mailer_nc_received` |
| Ubii | `.xlsx` | contiene `LIQUIDACION` | `mailer_raw.mailer_ubii_raw` |
| PedidosYa | `.xlsx` | contiene `ORDERDETAILS` | `mailer_raw.mailer_pdya_raw` |
| Megasoft | `.xlsx` | contiene `TRANSACCIONES` | `mailer_raw.mailer_megasoft_raw` |
| Credicard | `.json` | cualquier `.json` | `mailer_raw.mailer_credicard_raw` |

> **Nota PedidosYa**: El archivo se llamaba `pedidosya.xlsx` y cambió a `orderDetails.xlsx` en abril 2026.

## Infraestructura GCP

- **Cloud Run**: `mailer-pipeline` — `us-central1`
- **Service Account**: `233079260983-compute@developer.gserviceaccount.com`
- **GCS Bucket**: `gestion-365-data-lake`
  - Estructura: `ETIQUETA/YYYY/MM/DD/filename`
- **BigQuery**: proyecto `gestion-365`, dataset `mailer_raw`
- **Artifact Registry**: `us-central1-docker.pkg.dev/gestion-365/mailer-pipeline/mailer-pipeline`
- **Gmail**: impersonado como `odoo@gestion365ve.com`

## Deploy

### CI/CD (automático)
Push a `master` → Cloud Build → build Docker image → push a Artifact Registry → deploy Cloud Run.

### Manual
```bash
deploy  # alias local al gcloud run deploy
```

### Primer deploy desde cero
```bash
# Crear repo en Artifact Registry (solo una vez)
gcloud artifacts repositories create mailer-pipeline \
  --repository-format=docker --location=us-central1 --project=gestion-365

gcloud builds submit --config=cloudbuild.yaml --project=gestion-365
```

## Llamadas de prueba

```bash
# Procesar todos los archivos en GCS
curl -X POST https://mailer-pipeline-233079260983.us-central1.run.app/process \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{}'

# Procesar solo PedidosYa
curl -X POST https://mailer-pipeline-233079260983.us-central1.run.app/process \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"tipo": "pedidosya"}'

# Sincronizar desde Gmail (todos los módulos)
curl -X POST https://mailer-pipeline-233079260983.us-central1.run.app/sync \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"modulos": ["pedidosya"]}'
```

Tipos válidos para `/process`: `pedidosya`, `ubii`, `megasoft`, `z`, `facturas`, `nc`, `credicard`

## Archivos clave

| Archivo | Rol |
|---------|-----|
| `main.py` | Flask app, autenticación Gmail, endpoint `/sync` |
| `ingest.py` | Endpoint `/ingest`, Gmail → GCS |
| `process.py` | Endpoint `/process`, GCS → BQ, parsers de todos los tipos |
| `sync_*.py` | Módulos Gmail → BQ directo (uno por integración) |
| `utils.py` | Gmail helpers, circuit breaker, `get_attachments` |
| `cloudbuild.yaml` | Pipeline CI/CD |
| `Dockerfile` | Imagen Docker con cache de dependencias |
