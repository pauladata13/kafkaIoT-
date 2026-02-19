"""
Producer 1: sensor_temp_1 → temperature-sensors
Tendencia: +0.5°C por hora durante el día
"""
import json, time, random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'temperature-sensors'
MESSAGES_PER_SECOND = 0.125 

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all', retries=3
)

base_temp = 18.0
start_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
sensor_id = "sensor_temp_1"
locations = ['A Coruña-Cuatro Caminos', 'A Coruña-Monelos']

print("📡 Producer 1: Calentamiento +0.5°C/h → temperature-sensors")
mensaje_count = 0

try:
    while True:
        now = datetime.now()
        hours_elapsed = (now - start_day).total_seconds() / 3600
        temperature = base_temp + (hours_elapsed * 0.5) + random.uniform(-0.3, 0.3)
        
        data = {
            "sensor_id": sensor_id,
            "temperature": round(temperature, 2),
            "timestamp": now.isoformat(),
            "location": random.choice(locations)
        }
        
        future = producer.send(KAFKA_TOPIC, value=data)
        future.get(timeout=10)
        mensaje_count += 1
        
        if mensaje_count % 10 == 0:
            print(f"✅ {mensaje_count} msgs | {data['temperature']}°C | {data['timestamp'][:16]}")
        
        time.sleep(1.0 / MESSAGES_PER_SECOND)
except KeyboardInterrupt:
    print(f"\n {mensaje_count} mensajes enviados")
finally:
    producer.flush()
    producer.close()
