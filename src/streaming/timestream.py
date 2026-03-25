#!/usr/bin/env python3
"""
Prueba minima de escritura en Amazon Timestream.
Escribe un registro de prueba en las tablas de Timestream:
- btc_quotes_raw -> En esta tabla vamos a meter el parámetro de la cotización o el valor "close"
- btc_vwap_5m -> n esta tabla vamos a meter el kpi "vwap"
"""

# Importación de librerías necesarias
import json                  # Para formatear la salida por pantalla de forma legible (pretty print)
import time                  # Para generar un timestamp actual para la versión del registro
from datetime import datetime, timezone # Para el manejo y conversión de fechas

import boto3                 # El SDK de AWS para Python. Nos permite interactuar con los servicios de AWS (como Timestream)

# ==========================================
# PARÁMETROS DE CONFIGURACIÓN Y PRUEBA
# ==========================================
# Aquí se definen las variables globales que le dicen al script dónde escribir y qué datos usar.
REGION = "eu-west-1"         # La región de AWS donde está alojada tu base de datos (Irlanda)
DATABASE = "imat3a_crypto_rt"# El nombre de tu base de datos en Amazon Timestream
QUOTES_TABLE = "btc_quotes_raw" # Tabla para las cotizaciones en crudo
VWAP_TABLE = "btc_vwap_5m"   # Tabla para el cálculo del Precio Medio Ponderado por Volumen (VWAP)
SYMBOL = "BTCUSDT"           # El par de criptomonedas que estamos simulando (Bitcoin vs Tether)
TEST_CLOSE = 74240.12        # Valor simulado del precio de cierre
TEST_VWAP = 74229.99         # Valor simulado del VWAP
WINDOW_START = "2026-03-25T15:10:00.000Z" # Inicio de la ventana temporal del dato (formato ISO 8601)
WINDOW_END = "2026-03-25T15:15:00.000Z"   # Fin de la ventana temporal del dato

# ==========================================
# FUNCIONES AUXILIARES
# ==========================================

def now_epoch_ms() -> str:
    """
    Devuelve el momento actual exacto en milisegundos desde la época Unix (1 enero 1970).
    Timestream requiere que las marcas de tiempo (timestamps) se envíen en este formato numérico o en segundos.
    """
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))

def iso_to_epoch_ms(value: str) -> str:
    """
    Convierte una fecha en formato texto ISO (ej. "2026-03-25T15:15:00.000Z") 
    a milisegundos desde la época Unix. Reemplaza la 'Z' por el offset UTC (+00:00) 
    para que Python pueda parsearlo correctamente.
    """
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))

# ==========================================
# FUNCIÓN PRINCIPAL
# ==========================================

def main() -> None:
    # 1. Crear el cliente de AWS para escribir en Timestream
    # Boto3 buscará tus credenciales de AWS automáticamente (en variables de entorno o en ~/.aws/credentials)
    ts = boto3.client("timestream-write", region_name=REGION)

    # 2. Preparar el registro para la tabla de Cotizaciones (btc_quotes_raw)
    # Timestream usa un modelo de datos basado en "Dimensiones" (metadatos) y "Medidas" (el valor real).
    quote_record = {
        "Dimensions": [ # Las dimensiones sirven para filtrar y agrupar datos después (ej. WHERE symbol = 'BTCUSDT')
            {"Name": "symbol", "Value": SYMBOL},
            {"Name": "source_topic", "Value": "imat3a-BTC"},
            {"Name": "window_start", "Value": WINDOW_START},
            {"Name": "window_end", "Value": WINDOW_END},
            {"Name": "event_ts", "Value": WINDOW_END},
        ],
        "MeasureName": "close",                # El nombre de la métrica que estamos guardando
        "MeasureValue": str(float(TEST_CLOSE)),# El valor de la métrica (debe enviarse como texto en la API)
        "MeasureValueType": "DOUBLE",          # Especificamos que es un número decimal
        "Time": iso_to_epoch_ms(WINDOW_END),   # El momento exacto en el que ocurrió esta lectura
        "TimeUnit": "MILLISECONDS",            # La unidad de tiempo del campo anterior
        "Version": int(time.time() * 1000),    # La versión ayuda a actualizar datos. Si envías el mismo dato con mayor versión, Timestream lo actualiza.
    }

    # 3. Preparar el registro para la tabla VWAP (btc_vwap_5m)
    # Estructura idéntica a la anterior, pero con diferentes dimensiones y métrica.
    vwap_record = {
        "Dimensions": [
            {"Name": "symbol", "Value": SYMBOL},
            {"Name": "window_start", "Value": WINDOW_START},
            {"Name": "window_end", "Value": WINDOW_END},
            {"Name": "source_topic", "Value": "imat3a-BTC-VWAP-test"},
        ],
        "MeasureName": "vwap",                 # Ahora la métrica es el VWAP
        "MeasureValue": str(float(TEST_VWAP)), # El valor del VWAP
        "MeasureValueType": "DOUBLE",
        "Time": iso_to_epoch_ms(WINDOW_END),
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000) + 1, # Se suma 1 a la versión para asegurar que sea única respecto a la ejecución anterior si fuera en el mismo milisegundo
    }

    # 4. Enviar los datos a AWS Timestream
    # Llamamos a la API write_records pasándole la base de datos, la tabla y la lista de registros a insertar.
    
    # Escribir en la primera tabla
    quote_resp = ts.write_records(
        DatabaseName=DATABASE,
        TableName=QUOTES_TABLE,
        Records=[quote_record],
    )
    
    # Escribir en la segunda tabla
    vwap_resp = ts.write_records(
        DatabaseName=DATABASE,
        TableName=VWAP_TABLE,
        Records=[vwap_record],
    )

    # 5. Imprimir resultados por pantalla
    # Si las funciones anteriores fallan, el script lanzará una excepción. 
    # Si llega aquí, es que todo ha ido bien.
    print("Write OK (2 tablas)")
    print(f"Database={DATABASE} Region={REGION}")
    
    # Muestra el registro enviado y la respuesta del servidor de AWS para la tabla de cotizaciones
    print(f"Tabla {QUOTES_TABLE} record:")
    print(json.dumps(quote_record, indent=2))
    print("Response:")
    print(json.dumps(quote_resp, default=str, indent=2))
    
    # Muestra el registro enviado y la respuesta del servidor de AWS para la tabla VWAP
    print(f"Tabla {VWAP_TABLE} record:")
    print(json.dumps(vwap_record, indent=2))
    print("Response:")
    print(json.dumps(vwap_resp, default=str, indent=2))


# Este es el punto de entrada de Python. Asegura que la función main() 
# solo se ejecute si ejecutas este archivo directamente (no si lo importas desde otro script).
if __name__ == "__main__":
    main()