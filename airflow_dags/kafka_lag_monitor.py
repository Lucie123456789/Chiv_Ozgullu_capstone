import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import (
    Producer,
    TopicPartition,
    ConsumerGroupTopicPartitions,
)
from confluent_kafka.admin import AdminClient, OffsetSpec

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = "alerts-notifier"
TOPIC = "alerts"
LAG_THRESHOLD = 100

def monitor_kafka_lag():
    admin = AdminClient({
        "bootstrap.servers": KAFKA_SERVER
    })

    metadata = admin.list_topics(timeout=10)

    if TOPIC not in metadata.topics:
        raise RuntimeError(f"Topic {TOPIC} does not exist")

    partitions = [
        TopicPartition(TOPIC, partition_id)
        for partition_id in metadata.topics[TOPIC].partitions
    ]
    request = ConsumerGroupTopicPartitions(GROUP_ID, partitions)

    result = admin.list_consumer_group_offsets(
        [request],
        request_timeout=10
    )[GROUP_ID].result(timeout=15)

    committed_offsets = {
        tp.partition: tp
        for tp in result.topic_partitions
        if tp.topic == TOPIC
    }
    latest = admin.list_offsets(
        {tp: OffsetSpec.latest() for tp in partitions},
        request_timeout=10
    )

    producer = Producer({
        "bootstrap.servers": KAFKA_SERVER
    })

    alerts_sent = 0

    for tp in partitions:
        committed = committed_offsets.get(tp.partition)

        if committed is None or committed.offset < 0:
            raise RuntimeError(
                f"No committed offset for {GROUP_ID}, "
                f"partition {tp.partition}"
            )

        if committed.error is not None:
            raise RuntimeError(str(committed.error))

        end_offset = latest[tp].result(timeout=15).offset

        lag = max(0, end_offset - committed.offset)

        print(f"Partition {tp.partition}: lag = {lag}")
        if lag > LAG_THRESHOLD:
            alert = {
                "type": "consumer_lag_high",
                "consumer_group": GROUP_ID,
                "topic": TOPIC,
                "partition": tp.partition,
                "lag": lag,
                "threshold": LAG_THRESHOLD,
                "timestamp": datetime.utcnow().isoformat()
            }

            producer.produce(
                TOPIC,
                key=f"{GROUP_ID}-{tp.partition}".encode(),
                value=json.dumps(alert).encode()
            )

            alerts_sent += 1

    if producer.flush(timeout=15) > 0:
        raise RuntimeError("Some Kafka alerts were not delivered")

    print(f"Monitoring finished. Alerts sent: {alerts_sent}")


default_args = {
    "owner": "fleet_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="kafka_lag_monitor",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["fleet", "kafka", "monitoring"]
) as dag:

    check_consumer_lag = PythonOperator(
        task_id="check_consumer_lag",
        python_callable=monitor_kafka_lag
    )