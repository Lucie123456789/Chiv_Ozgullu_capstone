import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

ROOT = Path(__file__).resolve().parents[1]
KAFKA_SERVER = "localhost:9092"

TOPICS = [
    "vehicle_events",
    "fleet_catalog",
    "alerts",
    "region_kpis_1min",
    "vehicle_events_dlq",
    "daily_reports",
]

def files_available(day):
    folder = ROOT / "output" / "query1"

    if not folder.exists():
        return False

    for batch in folder.glob("batch_id=*"):
        if not batch.is_dir():
            continue

        if not batch.name.split("=")[-1].isdigit():
            continue

        date_folder = batch / f"event_date={day}"

        if date_folder.exists() and any(date_folder.glob("*.parquet")):
            return True

    return False

def check_topics():
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": KAFKA_SERVER})
    metadata = admin.list_topics(timeout=10)

    existing = set(metadata.topics.keys())
    missing = set(TOPICS) - existing

    if missing:
        raise RuntimeError(f"Missing Kafka topics: {sorted(missing)}")

    print("All six Kafka topics exist.")

def choose_branch(day):
    path = ROOT / "output" / "quality" / f"{day}.json"

    with open(path, "r", encoding="utf-8") as f:
        verdict = json.load(f)

    if verdict.get("passed") is True:
        return "run_report"

    return "publish_alert"

def send_to_kafka(topic, key, message):
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": KAFKA_SERVER})

    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(message).encode("utf-8"),
    )

    remaining = producer.flush(timeout=15)

    if remaining:
        raise RuntimeError("Kafka message was not delivered")

    print(f"Message published to {topic}")

def publish_daily_report(day):
    path = ROOT / "output" / "daily_reports" / f"{day}.json"

    with open(path, "r", encoding="utf-8") as f:
        report = json.load(f)

    send_to_kafka(
        topic="daily_reports",
        key=day,
        message=report,
    )

def publish_quality_alert(day):
    path = ROOT / "output" / "quality" / f"{day}.json"

    with open(path, "r", encoding="utf-8") as f:
        verdict = json.load(f)

    alert = {
        "type": "data_quality_failed",
        "date": day,
        "details": verdict,
    }

    send_to_kafka(
        topic="alerts",
        key=day,
        message=alert,
    )


default_args = {
    "owner": "fleet_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="fleet_daily_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["fleet", "spark", "kafka"],
) as dag:

    wait_for_files = PythonSensor(
        task_id="wait_for_files",
        python_callable=files_available,
        op_kwargs={"day": "{{ ds }}"},
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    check_kafka_topics = PythonOperator(
        task_id="check_kafka_topics",
        python_callable=check_topics,
    )

    run_quality = BashOperator(
        task_id="run_quality",
        bash_command="spark-submit spark_jobs/quality_job.py {{ ds }}",
        cwd=str(ROOT),
    )

    check_quality_verdict = BranchPythonOperator(
        task_id="check_quality_verdict",
        python_callable=choose_branch,
        op_kwargs={"day": "{{ ds }}"},
    )

    run_report = BashOperator(
        task_id="run_report",
        bash_command="spark-submit spark_jobs/daily_report_job.py {{ ds }}",
        cwd=str(ROOT),
    )

    publish_report = PythonOperator(
        task_id="publish_report",
        python_callable=publish_daily_report,
        op_kwargs={"day": "{{ ds }}"},
    )

    publish_alert = PythonOperator(
        task_id="publish_alert",
        python_callable=publish_quality_alert,
        op_kwargs={"day": "{{ ds }}"},
    )

    finish_pipeline = PythonOperator(
        task_id="finish_pipeline",
        python_callable=lambda: print("Pipeline finished."),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    wait_for_files >> check_kafka_topics >> run_quality
    run_quality >> check_quality_verdict

    check_quality_verdict >> run_report >> publish_report
    check_quality_verdict >> publish_alert

    [publish_report, publish_alert] >> finish_pipeline