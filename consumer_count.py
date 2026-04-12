from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    msg_count += 1
    store_counts[message.value['store']] += 1
    total_amount[message.value['store']] += message.value['amount']
    if msg_count%10==0:
        #print(list(zip(store_counts, total_amount)))
        print(f"\n--- Raport po {msg_count} wiadomościach ---")
        # Nagłówek tabeli
        print(f"{'Sklep':<15} | {'Liczba transakcji':<18} | {'Suma sprzedaży':<15}")
        print("-" * 55)
        
        # Wiersze danych
        for s in store_counts:
            count = store_counts[s]
            total = total_amount[s]
            print(f"{s:<15} | {count:<18} | {total:<15.2f}")
    
# TWÓJ KOD
# Dla każdej wiadomości:
#   1. store_counts[store] += 1
#   2. total_amount[store] += amount
#   3. Co 10 wiadomości: print tabela
