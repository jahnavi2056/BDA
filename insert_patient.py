from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Access database and collection
db = client["health_assistant"]
collection = db["patient_history"]

# Patient record
patient_data = {
    "patient_id": "P101",
    "symptoms": ["cough", "fever"],
    "history": "diabetic",
    "suggestions": [
        "Drink warm fluids",
        "Monitor sugar levels",
        "Consult a physician"
    ]
}

# Insert data
collection.insert_one(patient_data)

print("✅ Patient data inserted successfully.")

