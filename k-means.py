import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

# Connect to spark
spark = SparkSession.builder.appName("k-means").master("local[*]").getOrCreate()

# Read data
df = pd.read_csv("/home/stepka/Downloads/github/Test_folder/cleaned_OFF.csv")

"""
Features have different magnitude.
Since K-Means is a distance-based algorithm, this difference in magnitude can create a problem.
Bring all the variables to the same magnitude with StandardScaler()
"""
scaled_df = StandardScaler().fit_transform(df)

kmeans_kwargs = {
"init": "random",
"n_init": 10,
"random_state": 1,
}

#create list to hold SSE values for each k
sse = []
for k in range(1, 11):
    kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
    kmeans.fit(scaled_df)
    sse.append(kmeans.inertia_)

#visualize results to choose better cluster number using elbow method
plt.plot(range(1, 11), sse)
plt.xticks(range(1, 11))
plt.xlabel("Number of Clusters")
plt.ylabel("SSE")
plt.show()