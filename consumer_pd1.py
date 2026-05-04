from kafka import KafkaConsumer
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

historia = []

for msg in consumer:
    tx = msg.value
    tx_time = datetime.fromisoformat(tx['timestamp']).timestamp()
    uid = tx['user_id']
    
    historia.append((uid, tx_time))
    
    historia = [h for h in historia if h[1] > tx_time - 60]
    
    user_tx_count = sum(1 for h in historia if h[0] == uid)
    
    if user_tx_count > 3:
        print(f"ALERT! {uid} - {user_tx_count} transakcje w 60s!")