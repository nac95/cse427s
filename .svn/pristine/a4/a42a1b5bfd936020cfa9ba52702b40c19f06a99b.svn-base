from pyspark import SparkContext
import math
import sys

if __name__ == "__main__":
	if len(sys.argv) < 3:
	  print >> sys.stderr, "Usage: MAE <Input> <Output>"
	  exit(-1)

	# map the TestingRatings into (userId, avg_ratings)
	sc = SparkContext(appName="RMSD")
	result = sc.textFile(sys.argv[1]) \
	    .map(lambda line:line.split(',')) \
	    .map(lambda x : ((x[0],x[1]),x[2]))
	
	test = sc.textFile("s3://feifan.liu-emr/input/TestingRatings.txt") \
		.map(lambda line:line.split(',')) \
		.map(lambda x : ((x[0],x[1]),x[2]))
	
	join = result.join(test) \
		.map(lambda x: ('_',math.pow(float(x[1][0]) - float(x[1][1]),2))) \
		.groupByKey() \
		.mapValues(lambda x: math.sqrt(sum(x)/len(x)))


	join.values().saveAsTextFile(sys.argv[2])
	sc.stop()
