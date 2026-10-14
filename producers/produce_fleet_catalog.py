
import json
import random

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fleet_catalog"

REGIONS = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]
VEHICLE_TYPES = ["van", "truck", "refrigerated_truck", "motorbike"]
PAYLOADS = [500, 1000, 1500, 3000]


def create_catalog():
    """Create reference records for 40 vehicles."""
    vehicles = []

    # creates 40 vehicles
    for i in range(1, 41):
        vehicle_id = f"V{i:04d}"

        vehicle = {
            "vehicle_id": vehicle_id,
            "plate": f"AB-{i:03d}-CD",
            "vehicle_type": random.choice(VEHICLE_TYPES),
            "home_region": REGIONS[(i - 1) % len(REGIONS)],
            "max_payload_kg": random.choice(PAYLOADS),
        }

        vehicles.append(vehicle)

    return vehicles


def delivery_callback(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered {msg.key().decode('utf-8')} "
            f"to partition {msg.partition()}"
        )


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # avoids duplicates
        "enable.idempotence": True,
        # waits for confirmation
        "acks": "all",
    })

    catalog = create_catalog()

    for vehicle in catalog:
        producer.produce(
            topic=TOPIC,
            # the identifier is used as the Kafka key
            key=vehicle["vehicle_id"],
            # converts the dictionary to json
            value=json.dumps(vehicle),
            on_delivery=delivery_callback,
        )
        producer.poll(0)

    # waits for pending messages for a maximum of 30 seconds
    remaining = producer.flush(30)

    if remaining:
        raise RuntimeError(
            f"{remaining} messages were not delivered"
        )

    print("Catalog publication completed.")


if __name__ == "__main__":
    main()
