
# coding: utf-8

# In[1]:

import time

from pyspark import SparkContext
sc = SparkContext("local", "Simple RDD")


# In[2]:

# Config
file = "data/test-graph.txt"
max_iter = 100
num_partition = 4
debug = False


# In[3]:

# Load the data
textFile = sc.textFile(file)
print("All lines:", textFile.count())
dataFile = textFile.filter(lambda l: len(l) > 0 and l[0] != '#')
print("Correct lines:", dataFile.count())


# In[4]:

# Create edges RDD
def to_int_tuple(line, delim='\t'):
    strings = line.split(delim)[:2]
    return (int(strings[0]), int(strings[1]))
    
edges = dataFile.map(to_int_tuple)
edges.partitionBy(num_partition)
edges.cache()

# Check that all rows have exactly 2 entries
assert edges.filter(lambda t: len(t) == 2).count() == dataFile.count()


# In[5]:

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


# In[6]:

print("############# RDD: method 1 (single steps) ###############")
new_paths = edges
all_paths = edges

start = time.time()
true_start = start

# invariant:
###  - all_paths and new_paths are on 'num_partitions' partitions

for i in range(1, max_iter):
    print("________________________________")
    print("Iteration #%d:" % (i,))
    new_paths = compose(new_paths, edges)
    # Leave only really new paths
    new_paths = new_paths.subtract(all_paths).distinct(num_partition)
    new_paths.cache()
    print("Number of new paths: %d\n" % (new_paths.count(),))
    
    # Finish, when no more paths added
    if new_paths.isEmpty():
        print("No new paths, finishing...")
        break
    
    # Add new paths to all paths
    all_paths = all_paths.union(new_paths).coalesce(num_partition)
    all_paths.cache()
    
    if debug:
        print(new_paths.take(1000), '\n')
        
    end = time.time()
    print("Iteration time: %f s." % (end - start,))
    start = end

print("\n\n________________________________")
print("Total paths found: %d" % (all_paths.count(),))
print("Number of iterations: #%d" % (i,))

if debug:
    print()
    print(all_paths.take(1000), '\n')

true_end = time.time()
method1_time = true_end - true_start
print("\nCollecting time: %f s." % (true_end - start,))
print("Total time elapsed: %f s." % (method1_time,))
print("________________________________\n\n")


# In[7]:

print("############# RDD: method 2 (paths combining) ###############")
new_paths = edges
all_paths = edges

start = time.time()
true_start = start

# invariant:
###  - all_paths and new_paths are on 'num_partitions' partitions

for i in range(1, max_iter):
    print("________________________________")
    print("Iteration #%d:" % (i,))
    new_paths = compose(all_paths, all_paths)
    # Leave only really new paths
    new_paths = new_paths.subtract(all_paths).distinct(num_partition)
    new_paths.cache()
    print("Number of new paths: %d\n" % (new_paths.count(),))
    
    # Finish, when no more paths added
    if new_paths.isEmpty():
        print("No new paths, finishing...")
        break
    
    # Add new paths to all paths
    all_paths = all_paths.union(new_paths).coalesce(num_partition)
    all_paths.cache()
    
    if debug:
        print(new_paths.take(1000), '\n')
        
    end = time.time()
    print("Iteration time: %f s." % (end - start,))
    start = end

print("\n\n________________________________")
print("Total paths found: %d" % (all_paths.count(),))
print("Number of iterations: #%d" % (i,))

if debug:
    print()
    print(all_paths.take(1000), '\n')

true_end = time.time()
method2_time = true_end - true_start
print("\nCollecting time: %f s." % (true_end - start,))
print("Total time elapsed: %f s." % (method2_time,))
print("________________________________\n\n")


# In[8]:

print("############# RDD: method 3 (paths combining 'optimized') ###############")

start = time.time()
true_start = start

all_paths = edges
new_paths = compose(edges, edges)
new_paths = new_paths.subtract(all_paths).distinct(num_partition)
# invariant:
###  - all_paths and new_paths are disjoint
###  - all_paths and new_paths are on 'num_partitions' partitions

for i in range(2, max_iter):
    print("________________________________")
    print("Iteration #%d:" % (i,))
    # Obtain new paths by composing old ones
    all_x_new_paths = compose(all_paths, new_paths)
    new_x_all_paths = compose(new_paths, all_paths)
    new_x_new_paths = compose(new_paths, new_paths)
    # Leave only really new paths
    all_paths = all_paths.union(new_paths).coalesce(num_partition)
    all_paths.cache()
    new_paths = all_x_new_paths.union(new_x_all_paths).union(new_x_new_paths)
    new_paths = new_paths.subtract(all_paths).distinct(num_partition)
    new_paths.cache()
    print("Number of new paths: %d\n" % (new_paths.count(),))
    
    # Finish, when no more paths added
    if new_paths.isEmpty():
        print("No new paths, finishing...")
        break
    
    if debug:
        print(new_paths.take(1000), '\n')
        
    end = time.time()
    print("Iteration time: %f s." % (end - start,))
    start = end

print("\n\n________________________________")
print("Total paths found: %d" % (all_paths.count(),))
print("Number of iterations: #%d" % (i,))

if debug:
    print()
    print(all_paths.take(1000), '\n')

true_end = time.time()
method3_time = true_end - true_start
print("\nCollecting time: %f s." % (true_end - start,))
print("Total time elapsed: %f s." % (method3_time,))
print("________________________________\n\n")


# In[10]:

print("########### Summary ############")
print("Method 1: %f s." % (method1_time,))
print("Method 2: %f s." % (method2_time,))
print("Method 3: %f s." % (method3_time,))


# In[ ]:



