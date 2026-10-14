import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,
    "acks": "all"
})

vehicle = {
    "vehicle_id": "V0001",
    "plate": "AB-001-CD",
    "vehicle_type": "van",
    "home_region": "Lyon",
    "max_payload_kg": 1000
}

producer.produce(
    topic="fleet_catalog",
    key=vehicle["vehicle_id"],
    value=json.dumps(vehicle)
)

remaining = producer.flush(30)

if remaining:
    raise RuntimeError("Message non livré")

print("V0001 est mis à jour")