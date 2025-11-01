import sys
from pyspark import SparkContext
from pyspark.sql.functions import count
import time

start_time = time.time()
sc = SparkContext(appName="Top Taxis By Number Of Drivers")

lines = sc.textFile(sys.argv[1])
rows = lines.map(lambda x: x.split(","))

pairs = rows.map(lambda x: (x[0], x[1]))
unique_pairs = pairs.distinct()

# Map and reduce step in one
count_by_taxi = unique_pairs.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

sorted_counts = count_by_taxi.sortBy(lambda x: x[1], ascending=False)

for item in sorted_counts.take(10):
    print(item)

print(f"Execution time: {time.time() - start_time:.4f} seconds")