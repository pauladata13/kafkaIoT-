import requests
import json
import time

CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "temperature-connector"

connector_config = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "topics": "temperature-sensors",
        
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "s3.bucket.name": "temperature-data",
        "s3.region": "us-west-2",
        "s3.endpoint": "http://minio:9000",
        "store.url": "http://minio:9000",
        "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
        "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/",
        "partition.duration.ms": "3600000", 
        "locale": "US",
        "timezone": "UTC",
        
        "flush.size": "1",
        "rotate.interval.ms": "5000",
        
        "aws.access.key.id": "minioadmin",
        "aws.secret.access.key": "minioadmin",
        "schema.compatibility": "NONE"
    }
}
def wait_for_connect():
    """Wait for Kafka Connect to be ready"""
    print("Waiting for Kafka Connect to be ready...")
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f"{CONNECT_URL}/connectors")
            if response.status_code == 200:
                print("Kafka Connect is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"Attempt {i+1}/{max_retries}... waiting 5 seconds")
        time.sleep(5)
    
    print("ERROR: Kafka Connect did not become ready")
    return False

def delete_connector_if_exists():
    """Delete connector if it already exists"""
    try:
        response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        if response.status_code == 200:
            print(f"Connector {CONNECTOR_NAME} exists, deleting it...")
            delete_response = requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
            if delete_response.status_code == 204:
                print("Connector deleted successfully")
                time.sleep(5)
    except requests.exceptions.RequestException as e:
        print(f"Error checking/deleting connector: {e}")

def create_connector():
    """Create the S3 sink connector"""
    print(f"Creating connector: {CONNECTOR_NAME}")
    
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{CONNECT_URL}/connectors",
        headers=headers,
        data=json.dumps(connector_config)
    )
    
    if response.status_code == 201:
        print("Temperature connector created successfully!")
        print(json.dumps(response.json(), indent=2))
        return True
    else:
        print(f" Failed to create connector. Status: {response.status_code}")
        print(response.text)
        return False

def check_connector_status():
    """Check the status of the connector"""
    print(f"\nChecking {CONNECTOR_NAME} status...")
    response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
    
    if response.status_code == 200:
        status = response.json()
        print(json.dumps(status, indent=2))
    else:
        print(f"Failed to get connector status: {response.status_code}")

def main():
    """Main function to setup Temperature S3 connector"""
    print("=" * 60)
    print("IoT TEMPERATURE Kafka → MinIO Connector")
    print("=" * 60)
    
    if not wait_for_connect():
        return
    
    delete_connector_if_exists()
    
    if create_connector():
        time.sleep(3)
        check_connector_status()
        
        print("\n" + "=" * 60)
        print("Temperature connector setup complete!")
        print("Data from 'temperature-sensors' → temperature-data/year=YYYY/month=MM/day=DD/hour=HH/")
        print("Access MinIO: http://localhost:9001 (minioadmin/minioadmin)")
        print("=" * 60)

if __name__ == "__main__":
    main()
