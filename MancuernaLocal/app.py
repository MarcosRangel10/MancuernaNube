import os
import json
import ssl
import time
import threading
from flask import Flask, render_template, jsonify, request
import paho.mqtt.client as mqtt
import mysql.connector

app = Flask(__name__)

# =====================================================
# CONFIGURACIÓN MQTT - HIVE MQ CLOUD
# =====================================================
MQTT_BROKER = "d3befa5909cf4595a75259012d836398.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_TOPIC = "mancuerna/+/datos"

MQTT_USER = os.getenv("MQTT_USER", "mancuerna_esp32")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "12345678")

# =====================================================
# CONFIGURACIÓN MYSQL LOCAL
# =====================================================
DB_HOST = os.getenv("MYSQLHOST", "localhost")
DB_PORT = int(os.getenv("MYSQLPORT", "3306"))
DB_USER = os.getenv("MYSQLUSER", "root")
DB_PASSWORD = os.getenv("MYSQLPASSWORD", "12345678")
DB_NAME = os.getenv("MYSQLDATABASE", "mancuerna_db")

# =====================================================
# ESTADO DE RUTINA Y CALIBRACIÓN
# =====================================================
rutina_config = {
    "ejercicio": "curl_martillo",
    "repeticiones_objetivo": 10,
    "tiempo_subida_ideal": 1.0,
    "tiempo_bajada_ideal": 2.0,
    "activa": False  # Estricto: No cuenta nada si no se presiona INICIAR
}

calibracion = {
    "angulo_inicio": 0.0,
    "angulo_final": 90.0,
    "inicio_calibrado": False,
    "final_calibrado": False
}

estado_mancuerna = {
    "conectado": False,
    "datos": {
        "dispositivo": "Mancuerna01",
        "ejercicio": "ninguno",
        "angulo": 0.0,
        "movimiento": False,
        "repeticiones": 0,
        "objetivo": 10,
        "correctas": 0,
        "rapidas": 0,
        "lentas": 0,
        "estado": "ESPERA",
        "subida": 0.0,
        "bajada": 0.0,
        "aceleracion": 0.0,
        "velocidad_angular": 0.0
    },
    "evento": None,
    "resumen": None
}

# Control interno de la máquina de estados de movimiento
fase_movimiento = "ESPERA"
t_inicio_subida = 0.0
t_fin_subida = 0.0
ultimo_guardado_db = 0.0

def conectar_mysql():
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    except Exception as err:
        print("Error MySQL:", err)
        return None

# =====================================================
# PROCESAMIENTO BIOMECÁNICO DE ALTA PRECISIÓN
# =====================================================
def procesar_lectura_mancuerna(payload):
    global estado_mancuerna, fase_movimiento, t_inicio_subida, t_fin_subida

    ahora = time.time()
    
    dispositivo = payload.get("dispositivo", "Mancuerna01")
    angulo = float(payload.get("angulo", 0.0))
    movimiento = bool(payload.get("movimiento", False))
    aceleracion = float(payload.get("aceleracion", 0.0))
    velocidad_angular = float(payload.get("velocidad_angular", 0.0))

    d = estado_mancuerna["datos"]
    d["dispositivo"] = dispositivo
    d["angulo"] = angulo
    d["movimiento"] = movimiento
    d["aceleracion"] = aceleracion
    d["velocidad_angular"] = velocidad_angular

    # Si la rutina no está activa desde el Dashboard, no procesar el conteo
    if not rutina_config["activa"]:
        d["estado"] = "ESPERA"
        fase_movimiento = "ESPERA"
        return

    # Definición de rangos según la calibración
    ang_min = calibracion["angulo_inicio"] if calibracion["inicio_calibrado"] else 25.0
    ang_max = calibracion["angulo_final"] if calibracion["final_calibrado"] else 80.0
    
    rango_total = abs(ang_max - ang_min)
    if rango_total < 20.0:
        rango_total = 50.0  # Rango de seguridad mínimo

    # Umbrales dinámicos
    umbral_inicio = min(ang_min, ang_max) + (rango_total * 0.25)
    umbral_top = min(ang_min, ang_max) + (rango_total * 0.70)

    # ----------------------------------------------------
    # MÁQUINA DE ESTADOS PRECISA
    # ----------------------------------------------------
    if fase_movimiento == "ESPERA":
        # Iniciar cronómetro inmediatamente al despegar con velocidad positiva
        if angulo >= umbral_inicio and (movimiento or velocidad_angular > 5.0):
            fase_movimiento = "SUBIENDO"
            t_inicio_subida = ahora

    elif fase_movimiento == "SUBIENDO":
        # Congelar tiempo en la cresta de la flexión
        if angulo >= umbral_top:
            fase_movimiento = "ARRIBA"
            t_fin_subida = ahora

        # Reset por intento fallido/incompleto si pasan más de 4.0s
        elif (ahora - t_inicio_subida) > 4.0:
            fase_movimiento = "ESPERA"

    elif fase_movimiento == "ARRIBA":
        # Detección del retorno
        if angulo <= umbral_top - 5.0:
            fase_movimiento = "BAJANDO"

    elif fase_movimiento == "BAJANDO":
        # Retorno completo a la posición inicial
        if angulo <= umbral_inicio:
            t_fin_bajada = ahora
            
            # Tiempos netos sin arrastrar milisegundos muertos
            dur_subida = round(max(0.3, t_fin_subida - t_inicio_subida), 2)
            dur_bajada = round(max(0.3, t_fin_bajada - t_fin_subida), 2)
            
            t_sub_ideal = rutina_config["tiempo_subida_ideal"]

            # Clasificación calibrada con tolerancias reales
            if dur_subida < (t_sub_ideal * 0.60):
                tipo_evt = "RAPIDA"
                d["rapidas"] += 1
            elif dur_subida > (t_sub_ideal * 1.75):
                tipo_evt = "LENTA"
                d["lentas"] += 1
            else:
                tipo_evt = "CORRECTA"
                d["correctas"] += 1

            d["repeticiones"] += 1
            d["subida"] = dur_subida
            d["bajada"] = dur_bajada

            # Objeto exacto que renderiza la tarjeta del Frontend
            estado_mancuerna["evento"] = {
                "tipo": tipo_evt,
                "repeticion": d["repeticiones"],
                "subida": dur_subida,
                "bajada": dur_bajada
            }

            print(f">>> REPETICIÓN {d['repeticiones']}: {tipo_evt} | Subida: {dur_subida}s | Bajada: {dur_bajada}s")

            # Finalizar al llegar a la meta
            if d["repeticiones"] >= rutina_config["repeticiones_objetivo"]:
                finalizar_y_resetear_rutina()

            fase_movimiento = "ESPERA"

    d["estado"] = fase_movimiento

def finalizar_y_resetear_rutina():
    global rutina_config, fase_movimiento
    d = estado_mancuerna["datos"]
    totales = d["repeticiones"]
    
    if totales > 0:
        eficiencia = round((d["correctas"] / totales) * 100, 1)
        resumen = {
            "ejercicio": rutina_config["ejercicio"],
            "eficiencia": eficiencia,
            "correctas": d["correctas"],
            "rapidas": d["rapidas"],
            "lentas": d["lentas"]
        }
        estado_mancuerna["resumen"] = resumen

        # Insertar sesión terminada en MySQL
        conn = conectar_mysql()
        if conn:
            try:
                cursor = conn.cursor()
                sql = """INSERT INTO historial_sesiones 
                         (dispositivo, ejercicio, reps_solicitadas, reps_totales, reps_optimas, reps_rapidas, reps_lentas, eficiencia) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql, (
                    d["dispositivo"],
                    rutina_config["ejercicio"],
                    rutina_config["repeticiones_objetivo"],
                    totales,
                    d["correctas"],
                    d["rapidas"],
                    d["lentas"],
                    eficiencia
                ))
                conn.commit()
                cursor.close()
                conn.close()
                print(">>> ÉXITO: Rutina registrada en el historial de MySQL")
            except Exception as e:
                print("Error al guardar rutina:", e)

    # Bloquear y limpiar estado
    rutina_config["activa"] = False
    fase_movimiento = "ESPERA"

# =====================================================
# SUSCRIPCIÓN MQTT (HIVEMQ CLOUD)
# =====================================================
def al_conectar(client, userdata, flags, rc):
    if rc == 0:
        print("========================================")
        print("   MANCUERNA IoT CONECTADO A HIVEMQ")
        print("========================================")
        client.subscribe(MQTT_TOPIC)

def al_recibir(client, userdata, msg):
    global estado_mancuerna, ultimo_guardado_db
    try:
        payload = json.loads(msg.payload.decode())
        estado_mancuerna["conectado"] = True
        
        procesar_lectura_mancuerna(payload)

        # Muestreo continuo a la tabla datos_mancuerna solo durante la rutina
        ahora = time.time()
        if rutina_config["activa"] and (ahora - ultimo_guardado_db >= 1.0):
            conn = conectar_mysql()
            if conn:
                try:
                    cursor = conn.cursor()
                    d = estado_mancuerna["datos"]
                    sql = """INSERT INTO datos_mancuerna 
                             (dispositivo, ejercicio, angulo, movimiento, repeticiones, objetivo, 
                              correctas, rapidas, lentas, estado, subida, bajada, aceleracion, velocidad_angular) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    cursor.execute(sql, (
                        d["dispositivo"], d["ejercicio"], d["angulo"], d["movimiento"],
                        d["repeticiones"], d["objetivo"], d["correctas"], d["rapidas"],
                        d["lentas"], d["estado"], d["subida"], d["bajada"],
                        d["aceleracion"], d["velocidad_angular"]
                    ))
                    conn.commit()
                    cursor.close()
                    conn.close()
                except Exception as e:
                    pass
                ultimo_guardado_db = ahora
    except Exception as e:
        print("Error en MQTT:", e)

def iniciar_mqtt():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.on_connect = al_conectar
    client.on_message = al_recibir
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            time.sleep(3)

threading.Thread(target=iniciar_mqtt, daemon=True).start()

# =====================================================
# ENDPOINTS API REST (DASHBOARD)
# =====================================================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/estado', methods=['GET'])
def api_estado():
    return jsonify(estado_mancuerna)

@app.route('/api/configurar', methods=['POST'])
def api_configurar():
    data = request.get_json(force=True) or {}
    rutina_config["ejercicio"] = data.get("ejercicio", "curl_martillo")
    rutina_config["repeticiones_objetivo"] = int(data.get("repeticiones", 10))
    rutina_config["tiempo_subida_ideal"] = float(data.get("tiempo_subida", 1.0))
    rutina_config["tiempo_bajada_ideal"] = float(data.get("tiempo_bajada", 2.0))
    
    d = estado_mancuerna["datos"]
    d["ejercicio"] = rutina_config["ejercicio"]
    d["objetivo"] = rutina_config["repeticiones_objetivo"]
    d["repeticiones"] = 0
    d["correctas"] = 0
    d["rapidas"] = 0
    d["lentas"] = 0
    d["subida"] = 0.0
    d["bajada"] = 0.0
    
    estado_mancuerna["resumen"] = None
    estado_mancuerna["evento"] = None
    
    print(f">>> CONFIGURACIÓN GUARDADA: {rutina_config['ejercicio']} ({rutina_config['repeticiones_objetivo']} reps)")
    return jsonify({"status": "OK", "mensaje": "Sesión configurada"}), 200

@app.route('/api/comando', methods=['POST'])
def api_comando():
    data = request.get_json(force=True) or {}
    cmd = data.get("comando", "")
    d = estado_mancuerna["datos"]
    
    if cmd == "INICIAR":
        rutina_config["activa"] = True
        d["repeticiones"] = 0
        d["correctas"] = 0
        d["rapidas"] = 0
        d["lentas"] = 0
        estado_mancuerna["resumen"] = None
        estado_mancuerna["evento"] = None
        print(">>> COMANDO: INICIAR RUTINA")

    elif cmd == "CALIBRAR_INICIO":
        calibracion["angulo_inicio"] = d["angulo"]
        calibracion["inicio_calibrado"] = True
        print(f">>> ÁNGULO INICIAL CALIBRADO: {calibracion['angulo_inicio']}°")

    elif cmd == "CALIBRAR_FINAL":
        calibracion["angulo_final"] = d["angulo"]
        calibracion["final_calibrado"] = True
        print(f">>> ÁNGULO FINAL CALIBRADO: {calibracion['angulo_final']}°")

    elif cmd == "DETENER":
        finalizar_y_resetear_rutina()
        print(">>> COMANDO: DETENER RUTINA")

    elif cmd == "REINICIAR":
        d["repeticiones"] = 0
        d["correctas"] = 0
        d["rapidas"] = 0
        d["lentas"] = 0
        rutina_config["activa"] = False
        estado_mancuerna["resumen"] = None
        estado_mancuerna["evento"] = None
        print(">>> COMANDO: REINICIAR VALORES")

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
        print("Error al consultar historial:", e)
        return jsonify([])

if __name__ == '__main__':
    print("Servidor corriendo en http://127.0.0.1:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)