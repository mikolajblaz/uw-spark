
# coding: utf-8

# In[ ]:

import time
from pyspark.sql import SparkSession


# In[ ]:

# Config
file = "data/test-graph.txt"
max_iter = 100
num_partition = 4
debug = False


# In[ ]:

# Load the data
textFile = sc.textFile(file)
print("All lines:", textFile.count())
dataFile = textFile.filter(lambda l: len(l) > 0 and l[0] != '#')
print("Correct lines:", dataFile.count())


# In[ ]:

# Create edges RDD
def to_int_tuple(line, delim='\t'):
    strings = line.split(delim)[:2]
    return (int(strings[0]), int(strings[1]))
    
edges = dataFile.map(to_int_tuple)
edges.partitionBy(num_partition)
edges.cache()

# Check that all rows have exactly 2 entries
assert edges.filter(lambda t: len(t) == 2).count() == dataFile.count()


# In[ ]:

# Helper functions
def switch_key_value(kv):
    " Switch key with value. "
    return (kv[1], kv[0])

def compose(r1, r2):
    """ Compose 2 relations represented by PairRDDs. """
    r1_flip = r1.map(switch_key_value)
    # The key is now the intermediate node
    joined = r1_flip.join(r2, num_partition)
    return joined.values()


# In[ ]:

paths = edges
all_paths = edges

start = time.time()
true_start = start

for i in range(1, max_iter):
    print("________________________________")
    print("Iteration #%d:" % (i,))
    paths = compose(paths, edges)
    # Leave only new paths
    paths = paths.subtract(all_paths, num_partition)
    paths.cache()
    print("Number of new paths: %d\n" % (paths.count(),))
    
    # Finish, when no more paths added
    if paths.isEmpty():
        print("No new paths, finishing...")
        break
    
    # Add new paths to all paths
    all_paths = all_paths.union(paths).partitionBy(num_partition)
    all_paths.cache()
    
    if debug:
        for p in paths.collect():
            print(p)
        print()
        
    end = time.time()
    print("Iteration time: %f s." % (end - start,))
    start = end

print("\n\n________________________________")
print("Total paths found: %d" % (all_paths.count(),))
print("Number of iterations: #%d" % (i + 1,))

if debug:
    print()
    for p in all_paths.collect():
        print(p)
    print()

true_end = time.time()
print("\nCollecting time: %f s." % (true_end - start,))
print("Total time elapsed: %f s." % (true_end - true_start,))
print("________________________________\n\n")


# In[ ]:



