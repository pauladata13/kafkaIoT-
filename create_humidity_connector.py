"""
IoT HUMEDAD Kafka → MinIO Connector (JSON sin partición)
ENUNCIADO: No partitioning - all data in single location
"""
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
           "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        "flush.size": "1",
        "rotate.interval.ms": "5000",
        "aws.access.key.id": "minioadmin",
        "aws.secret.access.key": "minioadmin",
        "schema.compatibility": "NONE"
    }
}


def wait_for_connect(): 
    print("Waiting for Kafka Connect...")
    for i in range(30):
        try:
            if requests.get(f"{CONNECT_URL}/connectors").status_code == 200:
                print("Kafka Connect ready!")
                return True
        except: pass
        print(f"Wait {i+1}/30...")
        time.sleep(5)
    return False

def delete_connector_if_exists():
    try:
        r = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        if r.status_code == 200:
            requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
            print("Old connector deleted")
            time.sleep(5)
    except: pass

def create_connector():
    headers = {"Content-Type": "application/json"}
    r = requests.post(f"{CONNECT_URL}/connectors", headers=headers, data=json.dumps(connector_config))
    if r.status_code == 201:
        print("✓ Humidity connector OK!")
        return True
    print(f"✗ Error {r.status_code}: {r.text}")
    return False

def main():
    print("=" * 60)
    print("IoT HUMIDITY → MinIO (JSON no partition)")
    print("=" * 60)
    
    if wait_for_connect():
        delete_connector_if_exists()
        create_connector()
        print("\n✅ humidity-data/topics/humidity-sensors/partition=0/")
        print("MinIO: http://localhost:9001")

if __name__ == "__main__":
    main()
