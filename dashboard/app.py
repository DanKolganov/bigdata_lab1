import streamlit as st
import pandas as pd
import time
import json
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

st.set_page_config(page_title="Fraud Monitor PRO", layout="wide")

if 'total_count' not in st.session_state:
    st.session_state.total_count = 0
if 'fraud_count' not in st.session_state:
    st.session_state.fraud_count = 0
if 'total_sum' not in st.session_state:
    st.session_state.total_sum = 0.0

st.title("Детекция фрода")

m_col1, m_col2, m_col3 = st.columns(3)
total_ph = m_col1.empty()
fraud_ph = m_col2.empty()
avg_ph = m_col3.empty()

st.markdown("---")

c_col1, c_col2 = st.columns([2, 1])
line_chart_ph = c_col1.empty()
bar_chart_ph = c_col2.empty()

st.sidebar.title("Локация данных")
sidebar_ph = st.sidebar.empty()

def get_kafka_consumer():
    brokers = os.getenv("KAFKA_BROKERS", "kafka-0:9092,kafka-1:9092").split(",")
    while True:
        try:
            return KafkaConsumer(
                "transactions",
                bootstrap_servers=brokers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='latest',
                api_version=(0, 10),
                consumer_timeout_ms=1000 
            )
        except NoBrokersAvailable:
            time.sleep(2)

def run_dashboard():
    consumer = get_kafka_consumer()
    data_buffer = [] 
    
    last_update_time = time.time()

    while True:
        messages = consumer.poll(timeout_ms=500)
        
        new_data_received = False
        for _, msg_list in messages.items():
            for msg in msg_list:
                data = msg.value
                new_data_received = True
                
                st.session_state.total_count += 1
                st.session_state.total_sum += data.get('Amount', 0)
                if data.get('Amount', 0) > 1000:
                    st.session_state.fraud_count += 1
                
                data_buffer.append(data)
                if len(data_buffer) > 50:
                    data_buffer.pop(0)

        current_time = time.time()
        if new_data_received and (current_time - last_update_time > 0.5):
            df = pd.DataFrame(data_buffer)
            avg_val = st.session_state.total_sum / st.session_state.total_count
            
            total_ph.metric("Всего транзакций", st.session_state.total_count)
            fraud_ph.metric("Фрод (Сумма > 1000)", st.session_state.fraud_count)
            avg_ph.metric("Средний чек", f"{avg_val:.2f} ₽")
            
            with line_chart_ph.container():
                st.write("Суммы последних транзакций")
                st.line_chart(df['Amount'], height=300)

            with bar_chart_ph.container():
                st.write("Категории")
                cat_data = df['Merchant_Category'].value_counts()
                st.bar_chart(cat_data, height=300)

            with sidebar_ph.container():
                st.write("Топ городов в потоке:")
                st.dataframe(df['City'].value_counts(), use_container_width=True)
            
            last_update_time = current_time

if __name__ == "__main__":
    run_dashboard()