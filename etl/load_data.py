import psycopg2
import json

# Connect to PostgreSQL
conn = psycopg2.connect("dbname=orders_db user=postgres password=postgres host=localhost")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS customer_orders (
    customer_id VARCHAR(255),
    total_spent FLOAT
)
""")
conn.commit()

with open("data/transformed_orders.json", "r") as infile:
    for line in infile:
        record = json.loads(line)
        cur.execute(
            "INSERT INTO customer_orders (customer_id, total_spent) VALUES (%s, %s)",
            (record['customer_id'], record['total_spent'])
        )
conn.commit()
cur.close()
conn.close()
