from kafka import KafkaProducer
import json
import time
import random
import argparse


def create_producer(bootstrap_servers: str) -> KafkaProducer:

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )


def generate_vehicle_record() -> dict:

    links = ["A-B", "B-C", "C-D", "D-E"]

    return {
        "vehicle_id": f"vehicle_{random.randint(1, 20)}",
        "time": int(time.time()),
        "link": random.choice(links),
        "speed": round(random.uniform(0, 80), 2)
    }


def main():
    parser = argparse.ArgumentParser(description="Mock vehicle producer for Redpanda")
    parser.add_argument("--topic", default="vehicle_positions")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--records-per-batch", type=int, default=5)

    args = parser.parse_args()

    print("Starting producer...")
    print(f"Topic: {args.topic}")
    print(f"Bootstrap server: {args.bootstrap_server}")
    print(f"Interval: {args.interval} seconds")

    try:
        producer = create_producer(args.bootstrap_server)

        while True:
            for _ in range(args.records_per_batch):
                record = generate_vehicle_record()

                if record["speed"] > 0:
                    producer.send(args.topic, record)
                    print(f"Sent: {record}")

            producer.flush()
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    except Exception as e:
        print(f"Producer error: {e}")

    finally:
        try:
            producer.close()
        except:
            pass


if __name__ == "__main__":
    main()
