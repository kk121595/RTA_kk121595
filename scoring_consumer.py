from kafka import KafkaConsumer, KafkaProducer
import json
import datetime

def score_transaction(tx):
    score = 0
    rules = []
    # R1:
    if tx['amount'] > 3000:
        score += 3
        rules.append("R1")
        
    # R2:
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append("R2")
        
    # R3: 
    dt = datetime.datetime.fromisoformat(tx['timestamp'])
    if 0 <= dt.hour < 6:
        score += 2
        rules.append("R3")
    return score, rules

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# TWÓJ KOD
# Dla każdej transakcji: scoruj, jeśli >= 3: wyślij do 'alerts' i wypisz ALERT
for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    
    if score >= 3:
        # analogicznie jak w producer
        alert_data = {
            'tx_id': tx.get('tx_id'),
            'score': score,
            'reason': rules,
            'amount': tx.get('amount')
        }
        
        # wysyłamy do alerts
        alert_producer.send('alerts', value=alert_data)
        
        # wypisujemy ALERT
        print(f"Wysłano ALERT dla transakcji {tx.get('tx_id')}")