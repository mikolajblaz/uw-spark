# Autor: Mikołaj Błaż
# Nr indeksu: 346862
# Data: 10.02.2017

# Finding all connected nodes by composing paths (method 3, DataFrame)
# coding: utf-8

import time
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext("", "Simple DF 3")
sqlContext = SQLContext(sc)

# Config
file = "data/p2p-Gnutella05_10000.txt"
max_iter = 100
num_partition = 12
debug = False


# Load the data
textFile = sc.textFile(file)
print("All lines:", textFile.count())
dataFile = textFile.filter(lambda l: len(l) > 0 and l[0] != '#')
print("Correct lines:", dataFile.count())

# Create edges RDD
def to_int_tuple(line, delim='\t'):
    strings = line.split(delim)[:2]
    return (int(strings[0]), int(strings[1]))
    
edgesRDD = dataFile.map(to_int_tuple)
edgesRDD.partitionBy(num_partition)

# Check that all rows have exactly 2 entries
assert edgesRDD.filter(lambda t: len(t) == 2).count() == dataFile.count()

# Create DataFrame
edges = edgesRDD.toDF(["fro", "to"])
edges.cache()

def compose(df1, df2):
    """ Compose 2 relations represented by PairRDDs. """
    r1 = df1.alias('r1')
    r2 = df2.alias('r2')
    return r1.join(r2, col('r1.to') == col('r2.fro')).drop(col('r1.to')).drop(col('r2.fro'))


print("############# RDD: method 3 (paths combining) ###############")
new_paths = edges
all_paths = edges

start = time.time()
true_start = start

# invariant:
###  - all_paths and new_paths are on 'num_partitions' partitions

last_count = all_paths.count()

for i in range(1, max_iter):
    print("________________________________")
    print("Iteration #%d:" % (i,))
    new_paths = compose(all_paths, all_paths)
    # Leave only really new paths
    all_paths = all_paths.union(new_paths).distinct().coalesce(num_partition)
    
    count = all_paths.count()
    diff_count = count - last_count
    last_count = count
    print("Number of new paths: %d\n" % (diff_count,))
    
    if debug:
        print(new_paths.take(1000), '\n')
        
    end = time.time()
    print("Iteration time: %f s." % (end - start,))
    start = end
    
    # Finish, when no more paths added
    if diff_count == 0:
        print("No new paths, finishing...")
        break


print("\n\n________________________________")
print("Total paths found: %d" % (count,))
print("Number of iterations: #%d" % (i,))

if debug:
    print()
    print(all_paths.take(1000), '\n')

true_end = time.time()
method3_time = true_end - true_start
print("\nCollecting time: %f s." % (true_end - start,))
print("Total time elapsed: %f s." % (method3_time,))
print("________________________________\n\n")

