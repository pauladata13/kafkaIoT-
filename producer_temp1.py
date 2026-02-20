import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuración
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'temperature-sensors'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

base_temp = 18.0
start_time = datetime.now()  # Referencia para el incremento lineal
sensor_id = "sensor_temp_1"
locations = ['A Coruña-Cuatro Caminos', 'A Coruña-Monelos']

print(f"📡 Producer 1: {sensor_id} activo. Tendencia: +0.5°C/h")

try:
    while True:
        now = datetime.now()
        # Calculamos horas transcurridas desde el inicio
        hours_elapsed = (now - start_time).total_seconds() / 3600
        
        # Fórmula: Base + (0.5 * horas) + ruido aleatorio
        temp = base_temp + (hours_elapsed * 0.5) + random.uniform(-0.2, 0.2)
        
        data = {
            "sensor_id": sensor_id,
            "temperature": round(temp, 2),
            "timestamp": now.isoformat(),
            "location": random.choice(locations)
        }
        
        producer.send(KAFKA_TOPIC, value=data)
        print(f"✅ {sensor_id} | {data['temperature']}°C | Trend: +{round(hours_elapsed*0.5, 2)}°C")
        time.sleep(5)  # 1 mensaje cada 5 segundos

except KeyboardInterrupt:
    print("\nDeteniendo Producer 1...")
finally:
    producer.close()