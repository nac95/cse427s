from pyspark import SparkContext
import sys

if __name__ == "__main__":
	if len(sys.argv) < 2:
	  print >> sys.stderr, "Usage: IIJ <Top N Similar>"
	  exit(-1)

	Top_N_Similar = int(sys.argv[1])
	Preprocessed_data_path = "s3://feifan.liu-emr/JaccardPreprocess/part*"
	Original_training_data_path = "s3://feifan.liu-emr/input/TrainingRatings.txt"
	Testing_data_path = "s3://feifan.liu-emr/input/TestingRatings.txt"
	output_path = "s3://feifan.liu-emr/IIJ"+sys.argv[1]
	
	sc = SparkContext(appName="IIJ")

	  # Start of item-item, Jaccard
	def jsim(x):

	  A_Inter_B = list(set(x[0][1]) & set(x[1][1]) )
	  A_Union_B = list(set(x[0][1]) | set(x[1][1]) )
	  return len(A_Inter_B)/len(A_Union_B)

	def topN(x):
		return sorted(x,key=lambda y: y[1],reverse=True)[:Top_N_Similar]
	
	preload = sc.textFile(Preprocessed_data_path) \
	    .map(lambda line:line.split(',')) \
	    .map(lambda splits:(splits[0],splits[1])) \
	    .groupByKey() \
	    .mapValues(list) 
	    
	IIJ = preload.cartesian(preload) \
	      .filter(lambda x : x[0][0] != x[1][0]) \
	      .map(lambda x :(x[0][0],(x[1][0],jsim(x))) ) \
	      .groupByKey() \
	      .mapValues(lambda x :topN(list(x))) \
	      .flatMapValues(lambda x : x) \
	      .map(lambda x : (x[0],x[1][0])) 
	      
		
	# End of item-item, Pearson correlation


	training = sc.textFile(Original_training_data_path) \
	    .map(lambda line:line.split(',')) \
	    .map(lambda splits:((splits[1],splits[0]),float(splits[2])) ) 


	prediction = sc.textFile(Testing_data_path) \
	    .map(lambda line:line.split(',')) \
	    .map(lambda splits:(splits[0],splits[1]) ) \
	    .join(IIJ) \
	    .map(lambda x: (x[1],x[0])) \
	    .join(training) \
	    .map(lambda x: ((x[0][0],x[1][0]),x[1][1])) \
	    .groupByKey() \
	    .mapValues(lambda x: sum(x)/len(x)) \
	    .map(lambda x: x[0][1]+","+x[0][0]+","+str(x[1]))


	prediction.saveAsTextFile(output_path)
	sc.stop()
