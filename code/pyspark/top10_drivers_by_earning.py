import sys
from pyspark import SparkContext
import time

start_time = time.time()

sc = SparkContext(appName="Top Drivers By Earning")

lines = sc.textFile(sys.argv[1])
rows = lines.map(lambda x: x.split(","))

# (driver_id, minutes, earnings)
earning_pairs = rows.map(lambda x: (x[1], (float(x[4])/60.0, float(x[16])))) 

# (driver_id, total_minutes, total_earnings)
total_earnings = earning_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) 

driver_earnings = total_earnings.map(lambda x: (x[0], x[1][1] / x[1][0] if x[1][0] > 0 else 0.0))
sorted_earnings = driver_earnings.sortBy(lambda x: x[1], ascending=False)

for item in sorted_earnings.take(10):
    print(item)

print(f"Execution time: {time.time() - start_time:.4f} seconds")