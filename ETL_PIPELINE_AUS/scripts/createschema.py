import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'newdb')
DB_USER = os.getenv('DB_USER', 'newuser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'newpassword')

# Validate required environment variables
if not DB_PASSWORD:
    raise ValueError("DB_PASSWORD environment variable is required")

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cur = conn.cursor()

script_dir = os.path.dirname(os.path.abspath(__file__))
schema_path = os.path.join(script_dir, "../sql/schema_postgres.sql")

# Read the DDL file
with open(schema_path, "r") as f:
    ddl_sql = f.read()

# Execute the DDL
cur.execute(ddl_sql)
conn.commit()

cur.close()
conn.close()

print("Schema created successfully!")
print(f"Connected to database: {DB_NAME} on {DB_HOST}:{DB_PORT}")