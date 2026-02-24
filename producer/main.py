import os
import time
from app_producer import FraudProducer

if __name__ == "__main__":
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
    TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
    DATA_PATH = "/app/data/transactions.csv"

    time.sleep(10) 

    producer = FraudProducer(KAFKA_BROKERS, TOPIC)
    
    try:
        producer.stream_csv(DATA_PATH)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()