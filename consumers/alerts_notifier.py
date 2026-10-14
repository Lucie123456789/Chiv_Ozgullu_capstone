from confluent_kafka import Consumer

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "alerts-notifier",
    "enable.auto.commit": False,
    "partition.assignment.strategy": "cooperative-sticky" #cooperative-sticky assignment
}

consumer = Consumer(config)

#check
def on_assign(consumer, partitions):
    assigned = [p.partition for p in partitions]
    if assigned:
        print(f"Partitions assigned: {assigned}")
    consumer.incremental_assign(partitions)

def on_revoke(consumer, partitions):
    revoked = [p.partition for p in partitions]
    if revoked:
        print(f"Partitions revoked: {revoked}")
    consumer.incremental_unassign(partitions)

consumer.subscribe(
    ["alerts"],
    on_assign=on_assign,
    on_revoke=on_revoke
)

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    alert = msg.value().decode("utf-8")

    #Handle the alert
    print(f"Alert received: {alert}")

    #Manual offset commit after the alert is handled
    consumer.commit(message=msg, asynchronous=False)