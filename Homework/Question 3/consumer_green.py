import json
from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    consumer_timeout_ms=10000,  # stop after 10 seconds of no new messages
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Reading from {topic_name}...")

total = 0
long_distance = 0

for message in consumer:
    trip = message.value
    total += 1
    if trip.get('trip_distance', 0) > 5.0:
        long_distance += 1
    if total % 10000 == 0:
        print(f"Processed {total} messages so far...")

consumer.close()
print(f"\nTotal trips: {total}")
print(f"Trips with trip_distance > 5.0: {long_distance}")
