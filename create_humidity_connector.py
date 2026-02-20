
import requests
import json
import time

CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "humidity-connector"

connector_config = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "topics": "humidity-sensors",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "s3.bucket.name": "humidity-data",
        "s3.region": "us-west-2",
        
        "s3.endpoint": "http://minio:9000",
        "store.url": "http://minio:9000",
        
       
        "s3.path.style.access": "true", 
        
        "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        
 
        "flush.size": "1",        
        "rotate.interval.ms": "1000",
        
        "aws.access.key.id": "minioadmin",
        "aws.secret.access.key": "minioadmin",
        "schema.compatibility": "NONE"
    }
}

def wait_for_connect(): 
    print("=" * 60)
    print("Esperando a que Kafka Connect esté disponible...")
    for i in range(30):
        try:
            response = requests.get(f"{CONNECT_URL}/connectors")
            if response.status_code == 200:
                print("Kafka Connect detectado!")
                return True
        except:
            pass
        print(f"Intento {i+1}/30 - Reintentando en 5s...")
        time.sleep(5)
    return False

def delete_connector_if_exists():
    try:
        r = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        if r.status_code == 200:
            requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
            print("Conector antiguo eliminado")
            time.sleep(2)
    except Exception as e:
        print(f"Error al intentar borrar: {e}")

def create_connector():
    headers = {"Content-Type": "application/json"}
    print(f"Creando conector: {CONNECTOR_NAME}...")
    r = requests.post(
        f"{CONNECT_URL}/connectors", 
        headers=headers, 
        data=json.dumps(connector_config)
    )
    
    if r.status_code == 201:
        print("CONECTOR DE HUMEDAD CREADO CON ÉXITO")
        return True
    else:
        print(f" ERROR AL CREAR CONECTOR: {r.status_code}")
        print(f"Detalle: {r.text}")
        return False

def main():
    print("=" * 60)
    print("IoT DATA PIPELINE: HUMIDITY SINK SETUP")
    print("=" * 60)
    
    if wait_for_connect():
        delete_connector_if_exists()
        if create_connector():
            print("\nDestino: bucket 'humidity-data'")
            print("Estructura: topics/humidity-sensors/partition=0/...")
            print("\nComprueba MinIO en: http://localhost:9001")

if __name__ == "__main__":
    main()