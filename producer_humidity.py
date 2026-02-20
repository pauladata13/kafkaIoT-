import json
import time
import random
import math
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'humidity-sensors'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

base_hum = 65.0
sensor_id = "sensor_hum_1"
location = "A Coruña-Monte Alto"

print(f"📡 Producer 3: {sensor_id} activo. Tendencia: Alta de noche (18h-06h)")

try:
    while True:
        now = datetime.now()
        hour = now.hour + (now.minute / 60)
        
        # Ajuste de fase (hour + 3) para que el pico de humedad sea a las 03:00 AM
        cycle = 20 * math.sin(2 * math.pi * (hour + 3) / 24)
        hum = base_hum + cycle + random.uniform(-1.5, 1.5)
        
        # Asegurar límites de 0-100%
        hum = max(0, min(100, hum))
        
        data = {
            "sensor_id": sensor_id,
            "humidity": round(hum, 2),
            "timestamp": now.isoformat(),
            "location": location
        }
        
        producer.send(KAFKA_TOPIC, value=data)
        print(f"✅ {sensor_id} | {data['humidity']}% HR | Hora: {now.strftime('%H:%M')}")
        time.sleep(5)

except KeyboardInterrupt:
    print("\nDeteniendo Producer 3...")
finally:
    producer.close()