from flask import Flask, request, render_template
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType
import ast

app = Flask(__name__)

# Initialize SparkSession
spark = SparkSession.builder.appName("HealthChatbot").getOrCreate()

# Load dataset with HealthTips column
df = spark.read.option("header", True).csv("disease_symptoms.csv")


def parse_symptom_list(symptom_str):
    try:
        return ast.literal_eval(symptom_str)
    except:
        return []

parse_symptom_list_udf = udf(parse_symptom_list, ArrayType(StringType()))
df = df.withColumn("SymptomsArray", parse_symptom_list_udf(col("Symptom")))

disease_data = df.select("Disease", "SymptomsArray", "HealthTips").collect()

def find_disease_by_symptoms(user_symptoms):
    user_symptoms_set = set([sym.strip().lower() for sym in user_symptoms])
    results = []
    for row in disease_data:
        disease = row['Disease']
        symptoms = [s.lower() for s in row['SymptomsArray']]
        health_tips = row['HealthTips']
        overlap = user_symptoms_set.intersection(symptoms)
        score = len(overlap)
        if score > 0:
            results.append((disease, score, health_tips))
    results = sorted(results, key=lambda x: x[1], reverse=True)
    return results

@app.route("/", methods=["GET", "POST"])
def chatbot():
    response = ""
    if request.method == "POST":
        user_input = request.form["symptoms"]
        user_symptoms = [sym.strip() for sym in user_input.split(",")]
        matches = find_disease_by_symptoms(user_symptoms)
        if matches:
            response = "Possible diseases and health tips based on symptoms:<br><br>"
            for disease, score, tips in matches:
                response += f"<b>{disease}</b> (matched symptoms: {score})<br>Health Tips: {tips}<br><br>"
        else:
            response = "No matching diseases found."
    return render_template("index.html", response=response)

if __name__ == "__main__":
    app.run(debug=True)
