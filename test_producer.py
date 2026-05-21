from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

message = {
    "vehicle_id": "test_vehicle_1",
    "time": 1,
    "link": "A-B",
    "speed": 35.5
}

producer.send("vehicle_positions", message)
producer.flush()

print("Sent message:")
print(message)

time.sleep(1)
producer.close()
