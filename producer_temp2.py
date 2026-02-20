import json
import time
import random
import math
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'temperature-sensors'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

base_temp = 25 
sensor_id = "sensor_temp_2"
location = "A Coruña-Riazor"

print(f"📡 Producer 2: {sensor_id} activo. Tendencia: Ciclo 24h")

try:
    while True:
        now = datetime.now()
        hour = now.hour + (now.minute / 60)
        
        # Ajuste de fase (hour - 9) para que el pico (sin=1) sea a las 15:00
        cycle = 5 * math.sin(2 * math.pi * (hour - 9) / 24)
        temp = base_temp + cycle + random.uniform(-0.3, 0.3)
        
        data = {
            "sensor_id": sensor_id,
            "temperature": round(temp, 2),
            "timestamp": now.isoformat(),
            "location": location
        }
        
        producer.send(KAFKA_TOPIC, value=data)
        print(f"✅ {sensor_id} | {data['temperature']}°C | Hora: {now.strftime('%H:%M')}")
        time.sleep(5)

except KeyboardInterrupt:
    print("\nDeteniendo Producer 2...")
finally:
    producer.close()