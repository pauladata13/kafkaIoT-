
import json, time, random, math
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configurações
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'humidity-sensors'  # Tópico específico para umidade
MESSAGES_PER_SECOND = 5         # Um pouco mais rápido: 1 msg a cada 5s

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all', retries=3
)

base_humidity = 60.0  # Umidade base (60%)
sensor_id = "sensor_hum_1"
locations = ['A Coruña-Monte Alto', 'A Coruña-Riazor']

print("📡 Producer Humedad:python3 producer_temp1.py Ciclo 24h estable → humidity-sensors")
print(f"📍 Locations: {locations}")
print("⚡ ~5s/msg | Ctrl+C para parar\n")

mensaje_count = 0

try:
    while True:
        now = datetime.now()
        hour = now.hour
        
        # Ciclo de umidade (geralmente inverso à temperatura: mais alta à noite/madrugada)
        # Usamos -math.sin para que o pico seja oposto ao da temperatura
        cycle_hum = base_humidity - 15 * math.sin(2 * math.pi * hour / 24) + random.uniform(-2.0, 2.0)
        
        # Garantir que não ultrapasse 0-100%
        cycle_hum = max(0, min(100, cycle_hum))
        
        data = {
            "sensor_id": sensor_id,
            "humidity": round(cycle_hum, 2),
            "timestamp": now.isoformat(),
            "location": random.choice(locations)
        }
        
        future = producer.send(KAFKA_TOPIC, value=data)
        future.get(timeout=10)
        mensaje_count += 1
        
        if mensaje_count % 10 == 0:
            print(f"✅ {mensaje_count} msgs | {data['humidity']}% HR | "
                  f"{data['timestamp'][:16]} | {data['location']}")
        
        time.sleep(1.0 / MESSAGES_PER_SECOND)
        
except KeyboardInterrupt:
    print(f"\n Detenido | {mensaje_count} mensagens enviadas")
finally:
    producer.flush()
    producer.close()