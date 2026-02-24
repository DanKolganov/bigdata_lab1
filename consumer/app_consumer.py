import json
import joblib
import os
import pandas as pd
from kafka import KafkaConsumer

class FraudConsumer:
    def __init__(self, bootstrap_servers, topic):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='fraud-detection-group'
        )
        
        self.model = joblib.load('model.pkl')
        self.scaler = joblib.load('scaler.pkl')
        self.le_city = joblib.load('le_city.pkl')
        self.le_cat = joblib.load('le_cat.pkl')
        
        self.features = ['Amount', 'Last_Hour_Count', 'City_Enc', 'Cat_Enc', 'V1', 'V2']

    def start_consuming(self):
        print("[*] Consumer is ready. Monitoring transactions...")
        for message in self.consumer:
            data = message.value
            
            try:
                data['City_Enc'] = self.le_city.transform([data['City']])[0]
                data['Cat_Enc'] = self.le_cat.transform([data['Merchant_Category']])[0]
                
                df_input = pd.DataFrame([data])[self.features]
                
                X_scaled = self.scaler.transform(df_input)
                
                prediction = self.model.predict(X_scaled)[0]
                probability = self.model.predict_proba(X_scaled)[0][1]
                
                if prediction == 1:
                    print(f"!!! ALERT !!! Fraud detected! ID: {data['Transaction_ID']} | City: {data['City']} | Prob: {probability:.2f}")
                else:
                    print(f"OK: Transaction safe. ID: {data['Transaction_ID']} | Prob: {probability:.2f}")
                    
            except Exception as e:
                print(f"[!] Error processing message: {e}")
    

if __name__ == "__main__":
    brokers = os.getenv("KAFKA_BROKERS", "kafka-0:9092,kafka-1:9092").split(",")
    topic = os.getenv("KAFKA_TOPIC", "transactions")

    consumer_app = FraudConsumer(bootstrap_servers=brokers, topic=topic)
    consumer_app.start_consuming()