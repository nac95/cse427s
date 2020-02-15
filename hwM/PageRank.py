import sys
import re
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import desc

if __name__ == "__main__":
  if len(sys.argv) < 4:
    print >> sys.stderr, "Usage: PageRank <input><output>"
    exit(-1)

  sconf = SparkConf().setAppName("Page Rank Spark").set("spark.ui.port","4141")
  sc = SparkContext(conf=sconf)
  sc.setCheckpointDir("/tmp")

  # given the list of neighbors for a page and that page's rank, calculate 
# what that page contributes to the rank of its neighbors
def computeContribs(neighbors, rank):
    for neighbor in neighbors: yield(neighbor, rank/len(neighbors))

# number of iterations
n = int(sys.argv[1])

# node RDD
node = sc.textFile(sys.argv[2]).filter(lambda line: line.startswith('n'))\
   .map(lambda line: line.split())\
   .map(lambda node: (node[1],node[2]))

# read in a file of page links (format: url1 url2)
links = sc.textFile(sys.argv[2]).filter(lambda line: line.startswith('e'))\
   .map(lambda line: line.split())\
   .map(lambda pages: (pages[1],pages[2]))\
   .distinct()\
   .groupByKey()

# set initial page ranks to 1.0
ranks=links.map(lambda (page,neighbors): (page,1.0))

# for n iterations, calculate new page ranks based on neighbor contribibutios
for x in xrange(n):
  contribs=links\
    .join(ranks)\
    .flatMap(lambda (page,(neighbors,rank)): \
       computeContribs(neighbors,rank)) 
  ranks=contribs\
    .reduceByKey(lambda v1,v2: v1+v2)\
    .map(lambda (page,contrib): \
         (page,contrib * 0.85 + 0.15))
  if x % 50 == 0:
     contribs.checkpoint()
     ranks.checkpoint()

result=ranks\
  .join(node)\
  .map(lambda x:(x[1][1],x[1][0]))\
  .sortBy(lambda result: result[1], False)
 
results.saveAsTextFile(sys.argv[3]);

sc.stop()
