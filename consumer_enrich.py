from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
for message in consumer:
    # przypisanie risk_level
    if message.value['amount'] > 3000:
        message.value["risk_level"] = "HIGH"
    elif message.value['amount']  > 1000:
        message.value["risk_level"] = "MEDIUM"
    else:
        message.value["risk_level"] = "LOW"
    print(message)
 

'''for message in consumer:
    print(message)
    if(message.value['amount']>1000):
        print("ALERT")'''
    
