from pyspark.sql import SparkSession
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("MedicalLinkAnalysis") \
    .master("local[*]") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.1-s_2.12") \
    .getOrCreate()

# Vertices (resources)
vertices = spark.createDataFrame([
    ("1", "WHO"),
    ("2", "CDC"),
    ("3", "MayoClinic"),
    ("4", "WebMD"),
    ("5", "Healthline"),
], ["id", "name"])

# Edges (links)
edges = spark.createDataFrame([
    ("1", "2"),
    ("1", "3"),
    ("2", "3"),
    ("3", "4"),
    ("4", "5"),
    ("5", "1")
], ["src", "dst"])

# Build graph
g = GraphFrame(vertices, edges)

# Run PageRank
results = g.pageRank(resetProbability=0.15, maxIter=10)
results.vertices.select("id", "name", "pagerank").show()
