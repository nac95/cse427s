import sys
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv)<2:
		print >> sys.stderr, "Usage: CountJPGs <file>"
		exit(-1)

# get spark context
	sc = SparkContext()

# read in file from hadoop and count number by requirement
	counts = sc.textTextFile("/loudacre/weblogs").filter(lambda line: "jpg" in line.split(" ")[6]).count()

#print result
	print counts

# stop spark
	sc.stop()
       
    
