
import json, time, random, math
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'temperature-sensors'  
MESSAGES_PER_SECOND =  5 

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all', retries=3
)

base_temp = 21.0  
sensor_id = "sensor_temp_2"
locations = ['A Coruña-Monte Alto', 'A Coruña-Riazor']

print("📡 Producer 2: Ciclo 24h estable → temperature-sensors")
print(f"📍 Locations: {locations}")
print("⚡ ~8s/msg | Ctrl+C para parar\n")

mensaje_count = 0

try:
    while True:
        now = datetime.now()
        hour = now.hour
        
        cycle_temp = base_temp + 4 * math.sin(2 * math.pi * hour / 24) + random.uniform(-0.5, 0.5)
        
        data = {
            "sensor_id": sensor_id,
            "temperature": round(cycle_temp, 2),
            "timestamp": now.isoformat(),
            "location": random.choice(locations)
        }
        
        future = producer.send(KAFKA_TOPIC, value=data)
        future.get(timeout=10)
        mensaje_count += 1
        
        if mensaje_count % 10 == 0:
            print(f"✅ {mensaje_count} msgs | {data['temperature']}°C | "
                  f"{data['timestamp'][:16]} | {data['location']}")
        
        time.sleep(1.0 / MESSAGES_PER_SECOND)
        
except KeyboardInterrupt:
    print(f"\n Detenido | {mensaje_count} mensajes enviados")
finally:
    producer.flush()
    producer.close()
