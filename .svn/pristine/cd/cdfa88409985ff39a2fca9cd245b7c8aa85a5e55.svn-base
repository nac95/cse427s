package stubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/* 
 * To define a reduce function for MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text> {

  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * IntWritable, and a Context object.
   */
	private int N=0;
	
	/*
	 * Set up a TreeMap to save the data from mapper result file.
	 */
	private SortedMap<Integer, String> top2 = new TreeMap<Integer, String>();
	
	/*
	 * Using setup() function to set a parameter that can be got from command line.
	 */
	public void setup(Context context){
		Configuration conf= context.getConfiguration();
		N=conf.getInt("N", 2);
	}
	
  @Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	  
	  /*
	     * Convert the value, which is received as a Text object,
	     * to a String object.
	     * 
	     * The line.split(",") call uses regular expressions to split the line up by the comma.
	     * 
	     * The first part is converted to int.
	     * 
	     * The second part is put into to a string named moviename.
	     * 
	     * Put the name of the movie and sum of ratings of the movie in TreeMap.
	     * 
	     * Check the size of the TreeMap. If the size of it is more than N, then remove the first item(since the TreeMap has already sorted in ascending).
	     */
	  for(Text value:values){
		  String VALUE=value.toString().trim();
		  String[] item=VALUE.split(",");
		  int sum_of_ratings=Integer.parseInt(item[0]);
		  String moviename=item[1];
		  top2.put(sum_of_ratings, moviename);
		    if(top2.size()>N){
		    	top2.remove(top2.firstKey());
		    }
	}
  }
  
  /*
	 * Convert the output in descending order first.
	 * 
	 * Call the write method on the Context object to emit a key
	 * and a value from the reduce method. 
	 */
  @Override
  protected void cleanup(Context context) throws IOException,InterruptedException {
	    List<Integer> keys = new ArrayList<Integer>(top2.keySet());
	    for(int i=keys.size()-1; i>=0; i--){
	     context.write(new IntWritable(keys.get(i)), new Text(top2.get(keys.get(i))));
	  }	
  }
}
