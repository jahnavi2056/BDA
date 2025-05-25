from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["health_assistant"]
collection = db["patient_history"]

# Fetch record
record = collection.find_one({"patient_id": "P101"})
print(record)
