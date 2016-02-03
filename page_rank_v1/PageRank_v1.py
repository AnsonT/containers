from pyspark import SparkContext, SparkConf
import sys

def my_map(line):
	### The structure of line is (vertex, ([list of outgoing edges], current_PageRank))
	### Since this function is called in a flat-map, we need to return a list of KV pairs.
	### The flat map will take all of the list of KV pairs and make a single list containing
	### all the KV pairs.
	out_edges = line[1][0]
	current_PageRank = line[1][1]
	e = len(out_edges)
	if e > 0:
		return_list = []
		for f in out_edges:
			return_list.append( (f, current_PageRank/float(e)) )
		return return_list
	else:
		return []


conf = SparkConf()
sc = SparkContext(conf=conf)
fileName = sys.argv[1]
numIterations = int(sys.argv[2])       ### The number of iterations to run PageRank.


### lines is an RDD (list) where each element of the RDD is a string (one line of the text file).
lines = sc.textFile(fileName)


### edge_list is an RDD where each element is the list of integers from a line of the text file.
### edge_list is cached because we will refer to it numerous times throughout the computation.
### Each element of edge_list is of the form (vertex, [out neighbors]), so (int, list).
edge_list = lines.map(lambda line: (int(line.split()[0]), [int(x) for x in line.split()[1:]]) ).cache()


### vertex_set is an RDD that is the list of all vertices.
vertex_set = edge_list.map(lambda row: row[0])

### N is the number of vertices in the graph.
N = vertex_set.count()

### Initialize the PageRank vector.
### Each vertex will be keyed with its initial value (1/N where N is the number of vertices)
### Elements of Last_PageRank have the form (vertex, PageRank), so (int, float).
Last_PageRank = vertex_set.map(lambda x: (x, 1.0/N) )


### For illustration, I use a fixed number of iterations now.
### This can be changed to check for a small norm (norm computation below, commented out).
for iteration in xrange(numIterations):
	New_PageRank = edge_list.join(Last_PageRank).flatMap(my_map).reduceByKey(lambda a, b: a+b)	
	### norm = New_PageRank.join(Last_PageRank).map(lambda x: (x[1][0]-x[1][1])**2).sum()**.5	### Compute 2-norm as stopping criteria.
	### print iteration, norm
	Last_PageRank = New_PageRank

print Last_PageRank.collect()

