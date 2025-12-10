# test_mongodb.py

import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient

# Load MONGO_DB_URL from .env
load_dotenv()
uri = os.getenv("MONGO_DB_URL")

if not uri:
    # Fallback (only if you really want to hardcode)
    uri = "mongodb+srv://chiragthareja234_db_user:Admin123@cluster0.miukk78.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

print("Using URI:", uri)

# Minimal client – Atlas handles TLS automatically for mongodb+srv
client = MongoClient(uri, serverSelectionTimeoutMS=5000)

try:
    client.admin.command("ping")
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"MongoDB Test Connection Failed: {e}")
finally:
    client.close()
