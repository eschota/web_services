"""Stateless HTTP entry point for the Abris drawing workspace."""

import json
import logging
import os
from urllib.parse import unquote, urlsplit
from dwg import convert_dxf, is_available, DwgConversionError, DwgUnavailable


MAX_SCAN_BYTES = 24 * 1024 * 1024
MAX_DXF_BYTES = 2 * 1024 * 1024
LOGGER = logging.getLogger("abris")


def respond(start_response, status, body, content_type="application/json; charset=utf-8"):
    if not isinstance(body, bytes):
        body = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
    headers = [
        ("Content-Type", content_type),
        ("Content-Length", str(len(body))),
        ("Cache-Control", "no-store"),
        ("X-Content-Type-Options", "nosniff"),
    ]
    start_response(status, headers)
    return [body]


def application(environ, start_response):
    route = environ.get("PATH_INFO", "").rstrip("/")
    method = environ.get("REQUEST_METHOD", "")
    if route == "/abris/api/health" and method == "GET":
        return respond(start_response, "200 OK", {"status": "ok", "service": "abris", "dwg": is_available()})
    if route not in ("/abris/api/recognize", "/abris/api/dwg"):
        return respond(start_response, "404 Not Found", {"error": "Адрес не найден."})
    if method != "POST":
        return respond(start_response, "405 Method Not Allowed", {"error": "Требуется POST."})
    origin = environ.get("HTTP_ORIGIN", "")
    if origin and urlsplit(origin).netloc != environ.get("HTTP_HOST"):
        return respond(start_response, "403 Forbidden", {"error": "Откройте инструмент на сайте Абрис."})
    limit = MAX_DXF_BYTES if route.endswith("dwg") else MAX_SCAN_BYTES
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        length = 0
    if length <= 0:
        return respond(start_response, "400 Bad Request", {"error": "Файл пуст."})
    if length > limit:
        return respond(start_response, "413 Payload Too Large", {"error": "Файл превышает допустимый размер."})
    try:
        data = environ["wsgi.input"].read(length)
        if len(data) != length:
            raise ValueError("Передача файла прервалась.")
        if route.endswith("dwg"):
            return respond(start_response, "200 OK", convert_dxf(data), "application/octet-stream")
        from recognition import recognize
        filename = os.path.basename(unquote(environ.get("HTTP_X_ABRIS_FILENAME", "scan")))[:180]
        result = recognize(data, filename)
        return respond(start_response, "200 OK", result)
    except (DwgConversionError, DwgUnavailable) as exc:
        return respond(start_response, "503 Service Unavailable", {"error":str(exc)})
    except (ValueError, OSError) as exc:
        # Only the recognition module's explicit user messages may leave the service.
        msg = str(exc) if isinstance(exc, ValueError) and any("А" <= c <= "я" for c in str(exc)) else ""
        return respond(start_response, "422 Unprocessable Entity",
                       {"error": msg[:320] or "Не удалось прочитать файл. Проверьте его формат и содержимое."})
    except Exception as exc:
        LOGGER.error("Abris request failed (%s)", type(exc).__name__)
        return respond(start_response, "503 Service Unavailable",
                       {"error": "Обработка сейчас недоступна. Повторите попытку позже."})
