import os
import json
import ssl
import time
import threading
from flask import Flask, render_template, jsonify, request
import paho.mqtt.client as mqtt
import mysql.connector

app = Flask(__name__)

# Lock para seguridad en concurrencia de hilos
estado_lock = threading.Lock()

# =====================================================
# CONFIGURACIÓN MQTT - HIVEMQ CLOUD (ENV VARIABLES)
# =====================================================
MQTT_BROKER = os.getenv("MQTT_BROKER", "d3befa5909cf4595a75259012d836398.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))

# Tópicos principales
MQTT_TOPIC_DATOS = "mancuerna/+/datos"
MQTT_TOPIC_EVENTOS = "mancuerna/+/eventos"
MQTT_TOPIC_RESUMEN = "mancuerna/+/resumen"
MQTT_TOPIC_STATUS = "mancuerna/+/status"

MQTT_USER = os.getenv("MQTT_USER", "mancuerna_esp32")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "12345678")

# =====================================================
# CONFIGURACIÓN MYSQL (AIVEN CLOUD / LOCAL)
# =====================================================
DB_HOST = os.getenv("MYSQLHOST", "localhost")
DB_PORT = int(os.getenv("MYSQLPORT", "3306"))
DB_USER = os.getenv("MYSQLUSER", "root")
DB_PASSWORD = os.getenv("MYSQLPASSWORD", "12345678")
DB_NAME = os.getenv("MYSQLDATABASE", "mancuerna_db")

# Referencia global al cliente MQTT para publicación bidireccional
mqtt_client_global = None

# =====================================================
# ESTADO GLOBAL DEL SISTEMA
# =====================================================
estado_mancuerna = {
    "conectado": False,
    "datos": {
        "dispositivo": "Mancuerna01",
        "ejercicio": "curl_martillo",
        "nombre_ejercicio": "Curl martillo",
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
        "aceleracion": 9.81,
        "aceleracion_dinamica": 0.0,
        "velocidad_angular": 0.0,
        "giro_eje": 0.0,
        "bateria_pct": 100,  # Soporte para nivel de batería
        "voltaje_bateria": 4.20
    },
    "evento": None,
    "resumen": None
}

ultima_recepcion_mqtt = 0.0
ultimo_guardado_db = 0.0

def conectar_mysql():
    """Conexión compatible con Aiven SSL y MySQL Local"""
    try:
        config = {
            "host": DB_HOST,
            "port": DB_PORT,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "database": DB_NAME,
            "connect_timeout": 5
        }
        if DB_HOST not in ["localhost", "127.0.0.1"]:
            config["ssl_disabled"] = False
        return mysql.connector.connect(**config)
    except Exception as err:
        print(">>> Error Conexión MySQL:", err)
        return None

# =====================================================
# WATCHDOG DE CONEXIÓN FÍSICA
# =====================================================
def verificar_conexion_esp32():
    global estado_mancuerna, ultima_recepcion_mqtt
    while True:
        time.sleep(1)
        with estado_lock:
            if estado_mancuerna["conectado"]:
                if time.time() - ultima_recepcion_mqtt > 5.0:
                    estado_mancuerna["conectado"] = False
                    print(">>> ALERTA: ESP32 Desconectado (Sin señal por más de 5s)")

threading.Thread(target=verificar_conexion_esp32, daemon=True).start()

# =====================================================
# GESTIÓN MQTT (CLIENTE Y CALLBACKS)
# =====================================================
def al_conectar(client, userdata, flags, rc):
    if rc == 0:
        print("========================================")
        print("   MANCUERNA IoT CONECTADO A HIVEMQ")
        print("========================================")
        client.subscribe("mancuerna/#")
    else:
        print(f">>> Error conexión MQTT. Código: {rc}")

def al_recibir(client, userdata, msg):
    global estado_mancuerna, ultima_recepcion_mqtt, ultimo_guardado_db
    
    topic = msg.topic
    payload_str = msg.payload.decode('utf-8', errors='ignore')
    
    try:
        # Manejo de status en texto plano o JSON
        if "status" in topic:
            with estado_lock:
                if payload_str == "ONLINE":
                    estado_mancuerna["conectado"] = True
                    ultima_recepcion_mqtt = time.time()
                elif payload_str == "OFFLINE":
                    estado_mancuerna["conectado"] = False
            return

        payload = json.loads(payload_str)
        
        with estado_lock:
            estado_mancuerna["conectado"] = True
            ultima_recepcion_mqtt = time.time()

            if "datos" in topic:
                # Sincronización directa con la telemetría oficial del ESP32
                estado_mancuerna["datos"].update(payload)
                
                # Persistencia periódica en base de datos (cada 2s máximo)
                ahora = time.time()
                if ahora - ultimo_guardado_db >= 2.0:
                    guardar_telemetria_db(payload)
                    ultimo_guardado_db = ahora

            elif "eventos" in topic:
                estado_mancuerna["evento"] = payload
                print(f">>> EVENTO ESP32: {payload.get('tipo')} | Rep: {payload.get('repeticion')}")

            elif "resumen" in topic:
                estado_mancuerna["resumen"] = payload
                print(">>> RESUMEN RECIBIDO DE FIN DE RUTINA")
                guardar_resumen_db(payload)

    except Exception as e:
        print(f">>> Error procesando mensaje MQTT en tópico {topic}: {e}")

def guardar_telemetria_db(d):
    conn = conectar_mysql()
    if conn:
        try:
            cursor = conn.cursor()
            sql = """INSERT INTO datos_mancuerna 
                     (dispositivo, ejercicio, angulo, movimiento, repeticiones, objetivo, 
                      correctas, rapidas, lentas, estado, subida, bajada, aceleracion, velocidad_angular) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (
                d.get("dispositivo", "Mancuerna01"),
                d.get("ejercicio", "curl_martillo"),
                d.get("angulo", 0.0),
                d.get("movimiento", False),
                d.get("repeticiones", 0),
                d.get("objetivo", 10),
                d.get("correctas", 0),
                d.get("rapidas", 0),
                d.get("lentas", 0),
                d.get("estado", "ESPERA"),
                d.get("subida", 0.0),
                d.get("bajada", 0.0),
                d.get("aceleracion", 0.0),
                d.get("velocidad_angular", 0.0)
            ))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception:
            pass

def guardar_resumen_db(res):
    conn = conectar_mysql()
    if conn:
        try:
            cursor = conn.cursor()
            d = estado_mancuerna["datos"]
            sql = """INSERT INTO historial_sesiones 
                     (dispositivo, ejercicio, reps_solicitadas, reps_totales, reps_optimas, reps_rapidas, reps_lentas, eficiencia) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (
                d.get("dispositivo", "Mancuerna01"),
                res.get("ejercicio", "curl_martillo"),
                res.get("solicitadas", d.get("objetivo", 10)),
                res.get("realizadas", d.get("repeticiones", 0)),
                res.get("correctas", d.get("correctas", 0)),
                res.get("rapidas", d.get("rapidas", 0)),
                res.get("lentas", d.get("lentas", 0)),
                res.get("eficiencia", 0.0)
            ))
            conn.commit()
            cursor.close()
            conn.close()
            print(">>> ÉXITO: Sesión almacenada en historial de MySQL")
        except Exception as e:
            print(">>> Error al guardar historial en MySQL:", e)

def iniciar_mqtt():
    global mqtt_client_global
    client = mqtt.Client(client_id=f"RenderFlask_{int(time.time())}")
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.on_connect = al_conectar
    client.on_message = al_recibir
    
    mqtt_client_global = client
    
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            print(">>> Reintentando conexión MQTT en 5s:", e)
            time.sleep(5)

threading.Thread(target=iniciar_mqtt, daemon=True).start()

# =====================================================
# ENDPOINTS API REST (DASHBOARD)
# =====================================================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/estado', methods=['GET'])
def api_estado():
    with estado_lock:
        return jsonify(estado_mancuerna)

@app.route('/api/configurar', methods=['POST'])
def api_configurar():
    data = request.get_json(force=True) or {}
    dispositivo = estado_mancuerna["datos"].get("dispositivo", "Mancuerna01")
    
    # Publicar configuración vía MQTT al ESP32 para sincronización total
    if mqtt_client_global and mqtt_client_global.is_connected():
        topic_cfg = f"mancuerna/{dispositivo}/config"
        mqtt_client_global.publish(topic_cfg, json.dumps(data))
        print(f">>> CONFIGURACIÓN ENVIADA A MQTT [{topic_cfg}]: {data}")
    
    with estado_lock:
        d = estado_mancuerna["datos"]
        d["ejercicio"] = data.get("ejercicio", d["ejercicio"])
        d["objetivo"] = int(data.get("repeticiones", d["objetivo"]))
        d["repeticiones"] = 0
        d["correctas"] = 0
        d["rapidas"] = 0
        d["lentas"] = 0
        d["subida"] = 0.0
        d["bajada"] = 0.0
        estado_mancuerna["resumen"] = None
        estado_mancuerna["evento"] = None

    return jsonify({"status": "OK", "mensaje": "Configuración enviada al dispositivo"}), 200

@app.route('/api/comando', methods=['POST'])
def api_comando():
    data = request.get_json(force=True) or {}
    cmd = data.get("comando", "").upper()
    dispositivo = estado_mancuerna["datos"].get("dispositivo", "Mancuerna01")
    
    # Publicar comando vía MQTT al ESP32
    if mqtt_client_global and mqtt_client_global.is_connected():
        topic_cmd = f"mancuerna/{dispositivo}/comando"
        payload_cmd = json.dumps({"comando": cmd})
        mqtt_client_global.publish(topic_cmd, payload_cmd)
        print(f">>> COMANDO ENVIADO A MQTT [{topic_cmd}]: {cmd}")

    with estado_lock:
        d = estado_mancuerna["datos"]
        if cmd == "REINICIAR":
            d["repeticiones"] = 0
            d["correctas"] = 0
            d["rapidas"] = 0
            d["lentas"] = 0
            estado_mancuerna["resumen"] = None
            estado_mancuerna["evento"] = None
        elif cmd == "DETENER":
            estado_mancuerna["resumen"] = {
                "ejercicio": d.get("ejercicio"),
                "solicitadas": d.get("objetivo"),
                "realizadas": d.get("repeticiones"),
                "correctas": d.get("correctas"),
                "rapidas": d.get("rapidas"),
                "lentas": d.get("lentas"),
                "eficiencia": round((d.get("correctas", 0) / max(1, d.get("repeticiones", 1))) * 100, 1)
            }

    return jsonify({"status": "OK", "comando": cmd}), 200

@app.route('/api/historial', methods=['GET'])
def api_historial():
    conn = conectar_mysql()
    if not conn:
        return jsonify([])
    
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT ejercicio, reps_solicitadas, reps_totales, 
                   reps_optimas, reps_rapidas, reps_lentas, eficiencia, fecha 
            FROM historial_sesiones 
            ORDER BY fecha DESC LIMIT 20
        """)
        filas = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(filas)
    except Exception as e:
        print(">>> Error al consultar historial:", e)
        return jsonify([])

if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get("PORT", 5000))
    print(f">>> Servidor iniciado con Waitress en el puerto {port}")
    serve(app, host='0.0.0.0', port=port)
