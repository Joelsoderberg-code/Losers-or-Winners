import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, dbname="stocks", user="postgres", password="password")
cur = conn.cursor()
cur.execute("SELECT version();")
version = cur.fetchone()
print("Postgres version:", version)
cur.close()
conn.close()
