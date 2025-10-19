import pandas as pd
import psycopg2
from datetime import datetime

#Database connection
conn = psycopg2.connect(
	host = "postgres",
	database = "analytics_data",
	user = "analyst",
	password = "analytics123"
)

# EXTRACT: Read CSV
print(f"[{datetime.now()}] Extracting data from CSV...")
df = pd.read_csv('/data/sample_data.csv')
print(f"Extracted {len(df)} rows")

# TRANSFORM: Add calculated collumn
print(f"[{datetime.now()}] Transforming data...")
df['total_amount'] = df['quantity']*df['price']
df['date'] = pd.to_datetime(df['date'])
print("Added total_amount column")

# LOAD: Create table and insert data
print(f"[{datetime.now()}] Loading data to PostgresSQL...")
cursor = conn.cursor()

# Create table
cursor.execute("""
	CREATE TABLE IF NOT EXISTS sales(
		id SERIAL PRIMARY KEY,
		date DATE,
		product VARCHAR(100),
		quantity INTEGER,
		price DECIMAL(10,2),
		total_amount DECIMAL(10,2),
		loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)
""")

# Insert data
for _, row in df.iterrows():
	cursor.execute("""
		INSERT INTO sales (date, product, quantity, price, total_amount)
		VALUES (%s, %s, %s, %s, %s)
	""", (row['date'], row['product'], row['quantity'], row['price'], row['total_amount']))

conn.commit()
print(f"[{datetime.now()}] Successfully loaded {len(df)} rows to database!")

cursor.close()
conn.close
