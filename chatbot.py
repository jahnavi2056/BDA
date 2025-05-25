from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("Chatbot MLlib") \
    .master("local[*]") \
    .getOrCreate()

# Step 2: Load dataset
df = spark.read.csv("D:\Health_assistant\chat_data.csv", header=True)

# Rename label column
df = df.withColumnRenamed("answer", "label")

# Step 3: Define ML pipeline
tokenizer = Tokenizer(inputCol="question", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=100)
idf = IDF(inputCol="rawFeatures", outputCol="features")
indexer = StringIndexer(inputCol="label", outputCol="labelIndex")
lr = LogisticRegression(featuresCol="features", labelCol="labelIndex", maxIter=10)

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, indexer, lr])

# Step 4: Train the model
model = pipeline.fit(df)

# Step 5: Predict user input
def chatbot_query(input_text):
    test_df = spark.createDataFrame([Row(question=input_text)])
    prediction = model.transform(test_df)
    label_index = prediction.select("prediction").collect()[0][0]
    label_map = model.stages[-2].labels
    return label_map[int(label_index)]

# Step 6: Test chatbot
print("\n=== Chatbot Ready! ===")
while True:
    user_input = input("You: ")
    if user_input.lower() in ['exit', 'quit']:
        break
    response = chatbot_query(user_input)
    print("Bot:", response)
