import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

def generate_business_data(n_samples=400000):   
    cities = ['Moscow', 'Saint-Petersburg', 'Novosibirsk', 'Yekaterinburg', 'Kazan', 'Krasnodar']
    categories = ['Supermarket', 'Electronics', 'Jewelry', 'Gas_Station', 'Pharmacy', 'Online_Gaming']
    user_ids = [f"USER_{i:03d}" for i in range(1000)] 
    
    data = []
    current_time = datetime.now()
    
    user_activity_window = {uid: [] for uid in user_ids}

    for i in range(n_samples):
        user = random.choice(user_ids)
        city = random.choice(cities)
        category = random.choice(categories)
        
        current_time += timedelta(seconds=random.randint(1, 30))
        
        user_activity_window[user].append(current_time)
        user_activity_window[user] = [t for t in user_activity_window[user] 
                                     if t > current_time - timedelta(hours=1)]
        last_hour_count = len(user_activity_window[user])

        is_fraud = 0
        if last_hour_count > 10:
            is_fraud = 1 if random.random() < 0.7 else 0
        
        if category in ['Jewelry', 'Online_Gaming'] and random.random() < 0.05:
            is_fraud = 1
            amount = round(np.random.uniform(1000, 5000), 2)
        else:
            amount = round(np.random.exponential(50), 2) + 1.0

        v1 = np.log(amount + 1) + np.random.normal(0, 0.5)
        v2 = cities.index(city) + np.random.normal(0, 1)

        data.append({
            'Transaction_ID': i,
            'Timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'User_ID': user,
            'City': city,
            'Merchant_Category': category,
            'Amount': amount,
            'Last_Hour_Count': last_hour_count,
            'V1': v1, 
            'V2': v2,
            'Class': is_fraud
        })

    df = pd.DataFrame(data)
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/transactions.csv', index=False)
    print("Датасет сохранен в data/transactions.csv")

if __name__ == "__main__":
    generate_business_data()