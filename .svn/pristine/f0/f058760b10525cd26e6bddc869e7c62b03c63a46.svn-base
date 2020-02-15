from pyspark import SparkContext
from pyspark import SparkConf
import math

if __name__ == "__main__":
	if len(sys.argv) < 2:
	  print >> sys.stderr, "Usage: UUP <Top N>"
	  exit(-1)

	sconf = SparkConf().setAppName("UUP")
	sc = SparkContext(conf=sconf)

	Top_N_Similar = int(sys.argv[1])
	Preprocessed_data_path = "s3://kunpengxie-emr/Input/TestingRatings_nor/part*"
	Original_training_data_path = "s3://kunpengxie-emr/Input/TrainingRatings.txt"
	Testing_data_path = "s3://kunpengxie-emr/Input/TestingRatings.txt"
	output_path = "s3://kunpengxie-emr/output/UUP"+sys.argv[1]

	def pair(x):
		l = []
	
		for i in x:
			for j in x:
				if(j[0]>i[0]):
					l.append(((j[0],i[0]),(j[1],i[1])))

		return l

	def csim(x):
		dotProduct = 0
		rating1squaredSum = 0
		rating2squaredSum = 0

		for i in x:
			dotProduct += i[4]
			rating1squaredSum += i[1]
			rating2squaredSum+= i[3]

		if math.sqrt(rating1squaredSum*rating2squaredSum) == 0:
			return 0
		else:
			return dotProduct/math.sqrt(rating1squaredSum*rating2squaredSum)

	def topN(x):
		return sorted(x,key=lambda y: y[1],reverse=True)[:Top_N_Similar]


	# Start of user-user, Pearson correlation

	UUP = sc.textFile(Preprocessed_data_path) \
	    .map(lambda line:line.split(',')) \
	    .map(lambda splits:(splits[0],(splits[1],float(splits[2])) )) \
	    .groupByKey() \
	    .mapValues(lambda x: pair(list(x))) \
	    .flatMap(lambda x: x[1]) \
	    .map(lambda x: (x[0],(x[1][0],x[1][0]*x[1][0],x[1][1],x[1][1]*x[1][1],x[1][0]*x[1][1]))) \
	    .groupByKey() \
	    .mapValues(lambda x :csim(list(x))) \
	    .map(lambda x:(x[1],((x[0][0],x[0][1]),(x[0][1],x[0][0])))) \
	    .flatMapValues(lambda x : x) \
	    .map(lambda x:(x[1][0],(x[1][1],x[0]))) \
	    .groupByKey() \
	    .mapValues(lambda x :topN(list(x))) \
	    .flatMapValues(lambda x : x) \
	    .map(lambda x : (x[0],x[1][0])) 
	
	# End of user-user, Pearson correlation


	training = sc.textFile(Original_training_data_path) \
	    .map(lambda line:line.split(',')) \
	    .map(lambda splits:((splits[0],splits[1]),float(splits[2])) ) 


	prediction = sc.textFile(Testing_data_path) \
	    .map(lambda line:line.split(',')) \
	    .map(lambda splits:(splits[1],splits[0]) ) \
	    .join(UUP) \
	    .map(lambda x: (x[1],x[0])) \
	    .join(training) \
	    .map(lambda x: ((x[0][0],x[1][0]),x[1][1])) \
	    .groupByKey() \
	    .mapValues(lambda x: sum(x)/len(x)) \
	    .map(lambda x: x[0][0]+","+x[0][1]+","+str(x[1]))

	prediction.saveAsTextFile(output_path)
	sc.stop()
