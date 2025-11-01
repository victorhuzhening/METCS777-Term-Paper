from pyspark import SparkContext
import sys
from datetime import datetime
import time

start_time = time.time()
def get_hour(pair):
    dt_str, (x, y) = pair
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return (dt.hour, (x, y))
    
sc = SparkContext(appName="Most Profitable Working Hours")

lines = sc.textFile(sys.argv[1])
rows = lines.map(lambda x: x.split(","))

pairs = rows.map(lambda x: (x[2], (float(x[5]), float(x[12])))) # (pickup time, (distance, surcharge))
hours_pairs = pairs.map(get_hour)

surchage_by_hour = hours_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
average_surcharge_by_hour = surchage_by_hour.map(lambda x: (x[0], x[1][1] / x[1][0] if x[1][0] > 0.0 else 0.0))

sorted_averages = average_surcharge_by_hour.sortBy(lambda x: x[1], ascending=False)

for item in sorted_averages.take(10):
    print(item)

print(f"Execution time: {time.time() - start_time:.4f} seconds")