import os
import time
import json
import requests
from typing import Dict, Any
from tenacity import retry, wait_exponential, stop_after_attempt

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

GBFS_STATUS_URL = os.getenv("GBFS_STATUS_URL", "https://gbfs.citibikenyc.com/gbfs/en/station_status.json")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gbfs.station_status.json")
POLL_SECS = int(os.getenv("PRODUCER_POLL_SECS", "30"))


def ensure_topic(admin: KafkaAdminClient, topic: str) -> None:
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1,
                                      topic_configs={"retention.ms": "259200000", "cleanup.policy": "delete"})])
    except TopicAlreadyExistsError:
        pass


@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def fetch_gbfs() -> Dict[str, Any]:
    r = requests.get(GBFS_STATUS_URL, timeout=15)
    r.raise_for_status()
    return r.json()


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
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="gbfs-producer-admin")
    ensure_topic(admin, TOPIC)
    admin.close()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="all",
        retries=3,
    )

    print(f"Producing to {TOPIC} from {GBFS_STATUS_URL} every {POLL_SECS}s...")
    while True:
        payload = fetch_gbfs()
        stations = payload.get("data", {}).get("stations", [])
        for s in stations:
            key = str(s["station_id"])
            producer.send(TOPIC, key=key, value=normalize(s))
        producer.flush()
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()


