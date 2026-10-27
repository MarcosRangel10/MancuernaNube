"""Backend de Mancuerna IoT.

Responsabilidades:
- Flask: API para el dashboard.
- MQTT: comunicación bidireccional con el ESP32.
- ESP32: detección de movimiento y conteo oficial de repeticiones.
- MySQL: persistencia de mediciones e historial de sesiones.

Las repeticiones NO se vuelven a detectar aquí: el ESP32 es la fuente de verdad.
"""
from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import time
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, render_template, request
import mysql.connector
from mysql.connector import Error as MySQLError
import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Aplicación y logging
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
log = logging.getLogger("mancuerna-iot")

# ---------------------------------------------------------------------------
# Configuración. En producción, define estos valores como variables de entorno.
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv(
    "MQTT_BROKER", "d3befa5909cf4595a75259012d836398.s1.eu.hivemq.cloud"
)
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USER = os.getenv("MQTT_USER", "mancuerna_esp32")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "12345678")
DEVICE_ID = os.getenv("DEVICE_ID", "Mancuerna01")
MQTT_ROOT = f"mancuerna/{DEVICE_ID}"
TOPIC_DATOS = f"{MQTT_ROOT}/datos"
TOPIC_CONFIG = f"{MQTT_ROOT}/config"
TOPIC_COMANDO = f"{MQTT_ROOT}/comando"
TOPIC_STATUS = f"{MQTT_ROOT}/status"
TOPIC_EVENTOS = f"{MQTT_ROOT}/eventos"
TOPIC_RESUMEN = f"{MQTT_ROOT}/resumen"

DB_HOST = os.getenv("MYSQLHOST", "localhost")
DB_PORT = int(os.getenv("MYSQLPORT", "3306"))
DB_USER = os.getenv("MYSQLUSER", "root")
DB_PASSWORD = os.getenv("MYSQLPASSWORD", "12345678")
DB_NAME = os.getenv("MYSQLDATABASE", "mancuerna_db")

# ---------------------------------------------------------------------------
# Estado compartido. Todas las modificaciones se protegen con STATE_LOCK.
# ---------------------------------------------------------------------------
STATE_LOCK = threading.RLock()
MQTT_LOCK = threading.Lock()
mqtt_client: mqtt.Client | None = None
mqtt_connected = False
last_mqtt_message_at = 0.0
last_device_data_at = 0.0
last_db_save_at = 0.0

EXERCISES = {
    "curl_martillo": {"name": "Curl martillo", "aliases": {"biceps", "bíceps", "curl", "curl_martillo", "curl martillo", "martillo"}},
    "elevaciones_laterales": {"name": "Elevaciones laterales", "aliases": {"hombro", "hombros", "elevacion_lateral", "elevaciones_laterales", "elevaciones laterales"}},
    "remo": {"name": "Remo", "aliases": {"espalda", "remo"}},
    "extension_pecho": {"name": "Extensión de pecho", "aliases": {"pecho", "extension_pecho", "extension_de_pecho", "extensión de pecho", "extension de pecho"}},
    "press_frances": {"name": "Press francés", "aliases": {"triceps", "tríceps", "press_frances", "press francés", "extension_triceps"}},
}

rutina_config: dict[str, Any] = {
    "ejercicio": "curl_martillo",
    "repeticiones_objetivo": 10,
    "tiempo_subida_ideal": 1.0,
    "tiempo_bajada_ideal": 2.0,
    "tolerancia_tiempo_pct": 8.0,
    "activa": False,
}

estado_mancuerna: dict[str, Any] = {
    "conectado": False,
    "mqtt_conectado": False,
    "ultima_actualizacion": None,
    "datos": {
        "dispositivo": DEVICE_ID,
        "ejercicio": "ninguno",
        "nombre_ejercicio": "Sin ejercicio",
        "angulo": 0.0,
        "angulo_deteccion": 0.0,
        "movimiento": False,
        "repeticiones": 0,
        "objetivo": 10,
        "correctas": 0,
        "rapidas": 0,
        "lentas": 0,
        "mixtas": 0,
        "bruscas": 0,
        "incompletas": 0,
        "estado": "ESPERA",
        "subida": 0.0,
        "bajada": 0.0,
        "aceleracion": 0.0,
        "aceleracion_dinamica": 0.0,
        "velocidad_angular": 0.0,
        "giro_eje": 0.0,
        "calibracion_bajo": False,
        "calibracion_alto": False,
    },
    "evento": None,
    "resumen": None,
    "ultimo_error": None,
}

# ---------------------------------------------------------------------------
# Utilidades de validación y base de datos
# ---------------------------------------------------------------------------
def normalize_exercise(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_")
    normalized = raw.replace(" ", "_")
    for exercise_id, info in EXERCISES.items():
        aliases = {a.lower().replace(" ", "_").replace("-", "_") for a in info["aliases"]}
        if normalized == exercise_id or normalized in aliases:
            return exercise_id
    raise ValueError(f"Ejercicio no válido: {value!r}. Opciones: {', '.join(EXERCISES)}")


def as_float(payload: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = payload.get(key, default)
    try:
        result = float(value)
        if result != result or result in (float("inf"), float("-inf")):
            return default
        return result
    except (TypeError, ValueError):
        return default


def as_int(payload: dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(float(payload.get(key, default)))
    except (TypeError, ValueError, OverflowError):
        return default


def conectar_mysql():
    """Abre una conexión por operación; retorna None si MySQL no está disponible."""
    try:
        config: dict[str, Any] = {
            "host": DB_HOST,
            "port": DB_PORT,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "database": DB_NAME,
            "connection_timeout": 5,
            "autocommit": False,
        }
        # MySQL administrado suele exigir TLS. En localhost no lo forzamos.
        if DB_HOST not in ("localhost", "127.0.0.1"):
            config["ssl_disabled"] = False
        return mysql.connector.connect(**config)
    except MySQLError:
        log.exception("No fue posible conectar con MySQL (%s:%s/%s)", DB_HOST, DB_PORT, DB_NAME)
        return None


def guardar_medicion(datos: dict[str, Any]) -> bool:
    """Guarda una muestra en datos_mancuerna. La tabla debe existir previamente."""
    conn = conectar_mysql()
    if conn is None:
        return False
    cursor = None
    try:
        cursor = conn.cursor()
        sql = """INSERT INTO datos_mancuerna
            (dispositivo, ejercicio, angulo, movimiento, repeticiones, objetivo,
             correctas, rapidas, lentas, estado, subida, bajada, aceleracion, velocidad_angular)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(sql, (
            datos.get("dispositivo", DEVICE_ID), datos.get("ejercicio", "ninguno"),
            as_float(datos, "angulo"), int(bool(datos.get("movimiento", False))),
            as_int(datos, "repeticiones"), as_int(datos, "objetivo"),
            as_int(datos, "correctas"), as_int(datos, "rapidas"), as_int(datos, "lentas"),
            str(datos.get("estado", "ESPERA")), as_float(datos, "subida"),
            as_float(datos, "bajada"), as_float(datos, "aceleracion"),
            as_float(datos, "velocidad_angular"),
        ))
        conn.commit()
        return True
    except MySQLError:
        log.exception("No se pudo guardar la medición en datos_mancuerna")
        try:
            conn.rollback()
        except MySQLError:
            pass
        return False
    finally:
        if cursor:
            cursor.close()
        conn.close()


def guardar_resumen(resumen: dict[str, Any]) -> bool:
    """Guarda un resumen final usando el esquema historial_sesiones existente."""
    conn = conectar_mysql()
    if conn is None:
        return False
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("""INSERT INTO historial_sesiones
            (dispositivo, ejercicio, reps_solicitadas, reps_totales, reps_optimas,
             reps_rapidas, reps_lentas, eficiencia)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (
            resumen.get("dispositivo", DEVICE_ID), resumen.get("ejercicio", "ninguno"),
            as_int(resumen, "solicitadas"), as_int(resumen, "realizadas"),
            as_int(resumen, "correctas"), as_int(resumen, "rapidas"),
            as_int(resumen, "lentas"), as_float(resumen, "eficiencia"),
        ))
        conn.commit()
        log.info("Resumen de sesión guardado en historial_sesiones")
        return True
    except MySQLError:
        log.exception("No se pudo guardar el resumen de sesión")
        try:
            conn.rollback()
        except MySQLError:
            pass
        return False
    finally:
        if cursor:
            cursor.close()
        conn.close()

# ---------------------------------------------------------------------------
# MQTT: estado de conexión y recepción de datos del ESP32
# ---------------------------------------------------------------------------
def mqtt_publish(topic: str, payload: dict[str, Any] | str) -> bool:
    with MQTT_LOCK:
        client = mqtt_client
        connected = mqtt_connected
    if client is None or not connected:
        return False
    message = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    try:
        info = client.publish(topic, message, qos=1, retain=False)
        return info.rc == mqtt.MQTT_ERR_SUCCESS
    except Exception:
        log.exception("Error publicando MQTT en %s", topic)
        return False


def on_connect(client, userdata, flags, reason_code, properties=None):
    global mqtt_connected
    # Compatible con Paho v1 (rc int) y v2 (ReasonCode).
    try:
        success = int(reason_code) == 0
    except (TypeError, ValueError):
        success = str(reason_code).lower() in ("success", "0")
    with MQTT_LOCK:
        mqtt_connected = success
    with STATE_LOCK:
        estado_mancuerna["mqtt_conectado"] = success
    if success:
        topics = [(TOPIC_DATOS, 1), (TOPIC_STATUS, 1), (TOPIC_EVENTOS, 1), (TOPIC_RESUMEN, 1)]
        client.subscribe(topics)
        log.info("Conectado a HiveMQ; suscripciones activas para %s", MQTT_ROOT)
    else:
        log.error("Conexión MQTT rechazada: %s", reason_code)


def on_disconnect(client, userdata, disconnect_flags, reason_code=None, properties=None):
    global mqtt_connected
    with MQTT_LOCK:
        mqtt_connected = False
    with STATE_LOCK:
        estado_mancuerna["mqtt_conectado"] = False
    log.warning("Cliente MQTT desconectado: %s", reason_code)


def procesar_datos_dispositivo(payload: dict[str, Any]) -> None:
    """Actualiza telemetría desde el ESP32; no ejecuta un segundo detector de reps."""
    global last_device_data_at, last_db_save_at
    now = time.time()
    if payload.get("dispositivo") and str(payload["dispositivo"]) != DEVICE_ID:
        # El wildcard de temas no se usa: ignorar dispositivos inesperados.
        log.warning("Se ignoró paquete de dispositivo inesperado: %s", payload.get("dispositivo"))
        return

    # Normalizar campos numéricos sin descartar métricas desconocidas del firmware.
    numeric_fields = (
        "angulo", "angulo_deteccion", "aceleracion", "aceleracion_dinamica",
        "velocidad_angular", "giro_eje", "subida", "bajada",
        "tiempo_subida_objetivo", "tiempo_bajada_objetivo", "tolerancia_tiempo_pct",
    )
    counter_fields = ("repeticiones", "objetivo", "correctas", "rapidas", "lentas", "mixtas", "bruscas", "incompletas")
    with STATE_LOCK:
        d = estado_mancuerna["datos"]
        for key in numeric_fields:
            if key in payload:
                d[key] = as_float(payload, key, as_float(d, key))
        for key in counter_fields:
            if key in payload:
                d[key] = max(0, as_int(payload, key, as_int(d, key)))
        for key in ("dispositivo", "ejercicio", "nombre_ejercicio", "estado"):
            if key in payload and payload[key] is not None:
                d[key] = str(payload[key])
        if "movimiento" in payload:
            d["movimiento"] = payload["movimiento"] is True or str(payload["movimiento"]).lower() in ("1", "true", "sí", "si")
        for key in ("calibracion_bajo", "calibracion_alto"):
            if key in payload:
                d[key] = bool(payload[key])
        estado_mancuerna["conectado"] = True
        estado_mancuerna["mqtt_conectado"] = mqtt_connected
        estado_mancuerna["ultima_actualizacion"] = datetime.now(timezone.utc).isoformat()
        estado_mancuerna["ultimo_error"] = None
        # No inferir si la rutina sigue activa a partir de la fase: ESPERA también
        # es una fase válida entre repeticiones. El inicio/parada se controlan por API
        # y el resumen final del firmware confirma la finalización automática.
    last_device_data_at = now

    # Guardado limitado a una muestra por segundo para no saturar MySQL.
    if now - last_db_save_at >= 1.0:
        with STATE_LOCK:
            snapshot = dict(estado_mancuerna["datos"])
        if guardar_medicion(snapshot):
            last_db_save_at = now


def procesar_mensaje_mqtt(client, userdata, msg):
    global last_mqtt_message_at
    try:
        raw = msg.payload.decode("utf-8", errors="strict").strip()
        payload: Any = json.loads(raw) if raw else {}
        if not isinstance(payload, dict):
            payload = {"valor": payload}
        last_mqtt_message_at = time.time()
        topic = msg.topic
        log.debug("MQTT recibido (%s): %s", topic, payload)

        if topic == TOPIC_DATOS:
            procesar_datos_dispositivo(payload)
        elif topic == TOPIC_EVENTOS:
            with STATE_LOCK:
                estado_mancuerna["evento"] = payload
            log.info("Evento del ESP32: %s", payload.get("tipo", "evento desconocido"))
        elif topic == TOPIC_RESUMEN:
            # El firmware usa solicitadas/realizadas/correctas/rapidas/lentas.
            resumen = dict(payload)
            with STATE_LOCK:
                estado_mancuerna["resumen"] = resumen
                estado_mancuerna["evento"] = {"tipo": "RESUMEN_FINAL", **resumen}
                rutina_config["activa"] = False
            guardar_resumen(resumen)
        elif topic == TOPIC_STATUS:
            with STATE_LOCK:
                estado_mancuerna["status_dispositivo"] = payload
        else:
            log.debug("Mensaje recibido en tópico no gestionado: %s", topic)
    except (UnicodeDecodeError, json.JSONDecodeError):
        log.warning("Mensaje MQTT inválido en %s", msg.topic, exc_info=True)
    except Exception:
        log.exception("Error procesando mensaje MQTT en %s", msg.topic)


def iniciar_mqtt():
    global mqtt_client
    try:
        # Callback API v2 en versiones nuevas; fallback para Paho antiguo.
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"Flask_{DEVICE_ID}_{os.getpid()}")
        except (AttributeError, TypeError):
            client = mqtt.Client(client_id=f"Flask_{DEVICE_ID}_{os.getpid()}")
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = procesar_mensaje_mqtt
        with MQTT_LOCK:
            mqtt_client = client
        while True:
            try:
                log.info("Conectando a HiveMQ %s:%s", MQTT_BROKER, MQTT_PORT)
                client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                client.loop_forever(retry_first_connection=True)
            except Exception:
                with MQTT_LOCK:
                    globals()["mqtt_connected"] = False
                with STATE_LOCK:
                    estado_mancuerna["mqtt_conectado"] = False
                log.exception("MQTT no disponible; nuevo intento en 5 segundos")
                time.sleep(5)
    except Exception:
        log.exception("No se pudo iniciar el cliente MQTT")


threading.Thread(target=iniciar_mqtt, name="mqtt-client", daemon=True).start()

# ---------------------------------------------------------------------------
# API REST. Se conservan las rutas principales para no exigir cambios en index.
# ---------------------------------------------------------------------------
@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/estado")
def api_estado():
    with STATE_LOCK:
        snapshot = json.loads(json.dumps(estado_mancuerna, ensure_ascii=False))
    # Estado físico derivado del último paquete de datos, no solo de la conexión MQTT.
    snapshot["conectado"] = bool(last_device_data_at and time.time() - last_device_data_at <= 5.0)
    snapshot["mqtt_conectado"] = mqtt_connected
    snapshot["configuracion"] = dict(rutina_config)
    return jsonify(snapshot)


@app.post("/api/configurar")
def api_configurar():
    data = request.get_json(silent=True) or {}
    try:
        ejercicio = normalize_exercise(data.get("ejercicio", rutina_config["ejercicio"]))
        reps = int(data.get("repeticiones", data.get("objetivo", 10)))
        subida = float(data.get("tiempo_subida", 1.0))
        bajada = float(data.get("tiempo_bajada", 2.0))
        tolerancia = float(data.get("tolerancia_tiempo_pct", 8.0))
        if not 1 <= reps <= 1000:
            raise ValueError("Las repeticiones deben estar entre 1 y 1000.")
        if not 0.2 <= subida <= 10 or not 0.2 <= bajada <= 10:
            raise ValueError("Los tiempos de subida y bajada deben estar entre 0.2 y 10 segundos.")
        if not 0 <= tolerancia <= 40:
            raise ValueError("La tolerancia debe estar entre 0 y 40 por ciento.")
    except (ValueError, TypeError) as exc:
        return jsonify({"status": "ERROR", "mensaje": str(exc)}), 400

    with STATE_LOCK:
        if rutina_config["activa"]:
            return jsonify({"status": "ERROR", "mensaje": "Detén la rutina actual antes de cambiar su configuración."}), 409
        rutina_config.update({
            "ejercicio": ejercicio,
            "repeticiones_objetivo": reps,
            "tiempo_subida_ideal": subida,
            "tiempo_bajada_ideal": bajada,
            "tolerancia_tiempo_pct": tolerancia,
            "activa": False,
        })
        estado_mancuerna["datos"]["ejercicio"] = ejercicio
        estado_mancuerna["datos"]["nombre_ejercicio"] = EXERCISES[ejercicio]["name"]
        estado_mancuerna["datos"]["objetivo"] = reps
        estado_mancuerna["resumen"] = None
        estado_mancuerna["evento"] = None
    # No se publica aquí: el firmware inicia automáticamente al recibir /config.
    # La configuración se envía justo antes de INICIAR para que no arranque al guardar.
    log.info("Configuración preparada: %s, objetivo=%s", ejercicio, reps)
    return jsonify({"status": "OK", "mensaje": "Sesión configurada", "configuracion": dict(rutina_config)}), 200


@app.post("/api/comando")
def api_comando():
    data = request.get_json(silent=True) or {}
    comando = str(data.get("comando", "")).strip().upper()
    allowed = {"INICIAR", "DETENER", "REINICIAR", "CALIBRAR_INICIO", "CALIBRAR_FINAL"}
    if comando not in allowed:
        return jsonify({"status": "ERROR", "mensaje": "Comando no reconocido.", "permitidos": sorted(allowed)}), 400

    if comando == "INICIAR":
        with STATE_LOCK:
            config_payload = {
                "ejercicio": rutina_config["ejercicio"],
                "repeticiones": rutina_config["repeticiones_objetivo"],
                "tiempo_subida": rutina_config["tiempo_subida_ideal"],
                "tiempo_bajada": rutina_config["tiempo_bajada_ideal"],
                "tolerancia_tiempo_pct": rutina_config["tolerancia_tiempo_pct"],
            }
        if not mqtt_publish(TOPIC_CONFIG, config_payload):
            return jsonify({"status": "ERROR", "mensaje": "No hay conexión MQTT con el broker; no se envió la configuración."}), 503
        # El orden de publicación conserva config antes de INICIAR.
        if not mqtt_publish(TOPIC_COMANDO, {"comando": "INICIAR"}):
            return jsonify({"status": "ERROR", "mensaje": "Se envió la configuración, pero falló el comando INICIAR."}), 503
        with STATE_LOCK:
            rutina_config["activa"] = True
            estado_mancuerna["resumen"] = None
            estado_mancuerna["evento"] = None
    else:
        if not mqtt_publish(TOPIC_COMANDO, {"comando": comando}):
            return jsonify({"status": "ERROR", "mensaje": "No hay conexión MQTT con el broker; el comando no se envió."}), 503
        with STATE_LOCK:
            if comando in ("DETENER", "REINICIAR"):
                rutina_config["activa"] = False
            if comando == "REINICIAR":
                d = estado_mancuerna["datos"]
                for key in ("repeticiones", "correctas", "rapidas", "lentas", "mixtas", "bruscas", "incompletas"):
                    d[key] = 0
                d["subida"] = d["bajada"] = 0.0
                d["estado"] = "ESPERA"
                estado_mancuerna["resumen"] = None
                estado_mancuerna["evento"] = None
    log.info("Comando MQTT enviado: %s", comando)
    return jsonify({"status": "OK", "comando": comando, "mensaje": "Comando enviado al dispositivo."}), 200


@app.get("/api/historial")
def api_historial():
    conn = conectar_mysql()
    if conn is None:
        # Conserva el formato de lista esperado por dashboards sencillos.
        return jsonify([])
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""SELECT ejercicio, reps_solicitadas, reps_totales,
                   reps_optimas, reps_rapidas, reps_lentas, eficiencia, fecha
            FROM historial_sesiones ORDER BY fecha DESC LIMIT 20""")
        rows = cursor.fetchall()
        # Fechas convertidas para serialización JSON estable.
        for row in rows:
            if isinstance(row.get("fecha"), datetime):
                row["fecha"] = row["fecha"].isoformat()
        return jsonify(rows)
    except MySQLError:
        log.exception("Error consultando historial_sesiones")
        return jsonify([])
    finally:
        if cursor:
            cursor.close()
        conn.close()


@app.get("/api/salud")
def api_salud():
    """Endpoint de diagnóstico para comprobar backend, MQTT y MySQL."""
    conn = conectar_mysql()
    db_ok = conn is not None
    if conn is not None:
        conn.close()
    with STATE_LOCK:
        device_ok = bool(last_device_data_at and time.time() - last_device_data_at <= 5.0)
    return jsonify({
        "backend": "ok",
        "mqtt": mqtt_connected,
        "mysql": db_ok,
        "dispositivo": device_ok,
        "ultimo_paquete_segundos": round(time.time() - last_device_data_at, 2) if last_device_data_at else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


if __name__ == "__main__":
    from waitress import serve
    port = int(os.environ.get("PORT", 5000))
    log.info("Servidor Mancuerna IoT iniciado en puerto %s", port)
    serve(app, host="0.0.0.0", port=port, threads=8)
