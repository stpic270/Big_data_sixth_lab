from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler
import matplotlib.pyplot as plt
import numpy as np

# spark = SparkSession.builder.\
#         appName("cassandra_k-means").\
#         config("spark.cassandra.connection.host", "172.17.0.3").\
#         getOrCreate()

spark = SparkSession.builder.appName("k-means").master("local[*]").getOrCreate()
df = spark.read.csv("cleaned_OFF.csv", inferSchema=True, header=True)

input_cols = df.columns

vec_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")

final_data = vec_assembler.transform(df)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=False)

scaled_df = scaler.fit(final_data).transform(final_data)

kmeans = KMeans(k=4, featuresCol="scaled_features")
model = kmeans.fit(scaled_df)
print(df.columns)

model.transform(scaled_df).groupBy("prediction").count().show()

