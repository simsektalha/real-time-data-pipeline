import os
import time
import json
import requests
from typing import Dict, Any
from tenacity import retry, wait_exponential, stop_after_attempt

from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

GBFS_STATUS_URL = os.getenv("GBFS_STATUS_URL", "https://gbfs.citibikenyc.com/gbfs/en/station_status.json")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gbfs.station_status.json")
POLL_SECS = int(os.getenv("PRODUCER_POLL_SECS", "30"))


def ensure_topic(admin: AdminClient, topic: str) -> None:
    md = admin.list_topics(timeout=5)
    if topic in md.topics:
        return
    new_topic = NewTopic(topic=topic, num_partitions=1, replication_factor=1,
                         config={"retention.ms": "259200000", "cleanup.policy": "delete"})
    fs = admin.create_topics([new_topic])
    for _, f in fs.items():
        try:
            f.result()
        except KafkaException as e:
            if e.args and isinstance(e.args[0], KafkaError) and e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                pass
            else:
                raise


@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def fetch_gbfs() -> Dict[str, Any]:
    r = requests.get(GBFS_STATUS_URL, timeout=15)
    r.raise_for_status()
    return r.json()


def delivery_cb(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")
    return


def normalize(record: Dict[str, Any]) -> Dict[str, Any]:
    # normalize booleans that may arrive as 0/1
    def as_bool(x):
        if x in (0, 1):
            return bool(x)
        if isinstance(x, bool):
            return x
        return None

    return {
        "station_id": str(record.get("station_id")),
        "num_bikes_available": int(record.get("num_bikes_available", 0)),
        "num_ebikes_available": record.get("num_ebikes_available"),
        "num_docks_available": int(record.get("num_docks_available", 0)),
        "is_installed": as_bool(record.get("is_installed")),
        "is_renting": as_bool(record.get("is_renting")),
        "is_returning": as_bool(record.get("is_returning")),
        "last_reported": int(record.get("last_reported", 0)),
    }


def main() -> None:
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "linger.ms": 50,
        "acks": "all",
        "retries": 3,
    })
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    ensure_topic(admin, TOPIC)

    print(f"Producing to {TOPIC} from {GBFS_STATUS_URL} every {POLL_SECS}s...")
    while True:
        payload = fetch_gbfs()
        stations = payload.get("data", {}).get("stations", [])
        for s in stations:
            key = str(s["station_id"])
            value = json.dumps(normalize(s))
            producer.produce(topic=TOPIC, key=key, value=value, callback=delivery_cb)
        producer.flush()
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()


