# =============================================================================
# utils.py — Utilidades compartidas para todos los módulos sync
# =============================================================================

import logging
import time
from datetime import datetime, timezone

from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

GMAIL_THROTTLE = 0.5

# ---------------------------------------------------------------------------
# Circuit breaker global — se activa con el primer 429 no recuperable
# ---------------------------------------------------------------------------
_circuit_open = False
_circuit_retry_after: float = 0  # epoch seconds


def circuit_is_open() -> bool:
    """Retorna True si el circuit breaker está abierto (hay rate limit activo)."""
    global _circuit_open, _circuit_retry_after
    if not _circuit_open:
        return False
    # Verificar si ya pasó el tiempo de espera
    if time.time() >= _circuit_retry_after:
        _circuit_open = False
        logger.info("[circuit] Rate limit expirado — circuit breaker cerrado.")
        return False
    remaining = int(_circuit_retry_after - time.time())
    logger.warning(f"[circuit] Circuit abierto — {remaining}s restantes para retry.")
    return True


def circuit_trip(retry_after_str: str):
    """Abre el circuit breaker hasta el tiempo indicado por Gmail."""
    global _circuit_open, _circuit_retry_after
    try:
        # Parsear "Retry after 2026-03-28T00:58:37.859Z"
        ts_str = retry_after_str.strip()
        # Extraer solo la parte ISO si viene con texto antes
        if "Retry after" in ts_str:
            ts_str = ts_str.split("Retry after")[-1].strip()
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        _circuit_retry_after = dt.timestamp()
        # Agregar 30s de buffer para no intentar justo en el límite
        _circuit_retry_after += 30
    except Exception:
        # Si no podemos parsear, esperar 10 minutos por defecto
        _circuit_retry_after = time.time() + 600

    _circuit_open = True
    wait_min = int((_circuit_retry_after - time.time()) / 60)
    logger.error(f"[circuit] 429 detectado — circuit abierto por ~{wait_min} minutos.")


def gmail_call(func, max_retries=1, base_wait=15):
    """
    Ejecuta una llamada Gmail API con:
    - Retry exacto respetando el Retry-After de Google
    - Circuit breaker global
    """
    if circuit_is_open():
        raise HttpError(
            resp=type("r", (), {"status": 429})(),
            content=b"Circuit breaker open"
        )

    for attempt in range(max_retries + 1):
        try:
            result = func()
            time.sleep(GMAIL_THROTTLE)
            return result
        except HttpError as e:
            if e.resp.status == 429:
                # Extraer Retry-After del mensaje de error
                retry_after = _extract_retry_after(str(e))

                if attempt < max_retries:
                    # Un solo retry respetando el tiempo exacto
                    wait = _seconds_until(retry_after) if retry_after else base_wait * (2 ** attempt)
                    wait = min(wait, 60)  # máximo 60s de espera en retry
                    logger.warning(f"[gmail] 429 — esperando {wait}s antes de retry...")
                    time.sleep(wait)
                else:
                    # Sin más retries — activar circuit breaker
                    if retry_after:
                        circuit_trip(retry_after)
                    raise
            else:
                raise


def _extract_retry_after(error_str: str) -> str | None:
    """Extrae el timestamp del Retry-After del mensaje de error de Gmail."""
    import re
    m = re.search(r"Retry after (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.\d]*Z)", error_str)
    return m.group(1) if m else None


def _seconds_until(ts_str: str) -> float:
    """Segundos hasta el timestamp ISO dado."""
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return max(0, (dt - datetime.now(tz=timezone.utc)).total_seconds())
    except Exception:
        return 15


def list_messages(service, label_id: str) -> list:
    """Lista mensajes NO procesados (sin label PROCESADO)."""
    messages, page_token = [], None
    while True:
        if circuit_is_open():
            raise RuntimeError("Circuit breaker abierto — abortando list_messages.")
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


def get_or_create_label(service, label_name: str, label_map: dict) -> str | None:
    """
    Retorna el ID del label dado su nombre.
    Si no existe, lo crea y actualiza el label_map en memoria.
    """
    label_id = label_map.get(label_name.upper())
    if label_id:
        return label_id
    try:
        result = gmail_call(lambda: service.users().labels().create(
            userId="me",
            body={"name": label_name, "labelListVisibility": "labelHide",
                  "messageListVisibility": "hide"}
        ).execute())
        new_id = result["id"]
        label_map[label_name.upper()] = new_id
        logger.info(f"[gmail] Label '{label_name}' creado: {new_id}")
        return new_id
    except Exception as e:
        logger.error(f"[gmail] No se pudo crear label '{label_name}': {e}")
        return None


def mark_as_processed(service, msg_id: str, processed_label_id: str):
    """Aplica el label PROCESADO al mensaje."""
    try:
        gmail_call(lambda: service.users().messages().modify(
            userId="me",
            id=msg_id,
            body={"addLabelIds": [processed_label_id]}
        ).execute())
    except Exception as e:
        logger.warning(f"[gmail] No se pudo marcar {msg_id} como procesado: {e}")


def get_attachments(service, msg_id: str, prefix: str = None,
                    keyword: str = None, extensions: tuple = (".txt",)) -> list:
    """
    Extrae adjuntos recursivamente.
    - prefix: el filename debe empezar con este string (ej: "FAC", "REPZ")
    - keyword: el filename debe contener este string (ej: "LIQUIDACION")
    - extensions: tupla de extensiones válidas
    """
    msg = gmail_call(lambda: service.users().messages().get(
        userId="me", id=msg_id, format="full"
    ).execute())

    import base64

    def _walk(parts):
        for part in parts:
            filename = part.get("filename", "")
            fu = filename.upper()
            ext_ok = any(filename.lower().endswith(ext) for ext in extensions)
            name_ok = (
                (prefix and fu.startswith(prefix.upper())) or
                (keyword and keyword.upper() in fu)
            )
            if ext_ok and name_ok:
                att_id = part["body"].get("attachmentId")
                if att_id:
                    att = gmail_call(lambda: service.users().messages().attachments().get(
                        userId="me", messageId=msg_id, id=att_id
                    ).execute())
                    yield {
                        "filename": filename,
                        "raw_bytes": base64.urlsafe_b64decode(att["data"]),
                        "content": base64.urlsafe_b64decode(att["data"]).decode("utf-8", errors="replace")
                        if any(ext in (".txt",) for ext in extensions) else None,
                        "msg_id": msg_id,
                    }
            if "parts" in part:
                yield from _walk(part["parts"])

    return list(_walk(msg.get("payload", {}).get("parts", [])))