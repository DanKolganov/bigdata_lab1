import pandas as pd
import json
import time
import random
from kafka import KafkaProducer

class FraudProducer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def stream_csv(self, file_path):
        print(f"[*] Reading data from {file_path}")
        df = pd.read_csv(file_path)
        
        for i, (_, row) in enumerate(df.iterrows()):
            message = row.to_dict()
            self.producer.send(self.topic, value=message)
            
            if i < 100:
                time.sleep(0.05) 
            else:
                time.sleep(random.uniform(0.01, 0.1)) 
                
            if i % 1000 == 0:
                print(f"[+] Sent {i} transactions")

    def close(self):
        self.producer.close()