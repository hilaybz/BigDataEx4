import csv
import json
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "ev_population"
CSV_PATH = "/home/alona/pyspark/input/Electric_Vehicle_Population_Data.csv"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

sent = 0
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        producer.send(TOPIC, value=row)
        sent += 1
        if sent % 10000 == 0:
            producer.flush()
            print(f"sent {sent}")

producer.flush()
print(f"Done. Sent {sent} records.")
