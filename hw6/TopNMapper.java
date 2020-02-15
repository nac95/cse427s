package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/* 
 * To define a map function for MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class TopNMapper extends Mapper<Text, Text, NullWritable, Text> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
	private int N;
	
	/*
	 * Set up a TreeMap to save the data from job1 result file, and a HashMap to save the data from file in local.
	 */
	private SortedMap<Integer, String> top = new TreeMap<Integer, String>();
	private HashMap<String, String> movieitem = new HashMap<String, String>();
	
	/*
	 * Using setup() function to set a parameter and distribute cache that can be got from command line.
	 * 
	 * Read in the file line-by-line and split each line using regular expression ",".
	 * Put each key-value pairs which we get from each line into the HashMap
	 */
	public void setup(Context context) throws IOException{
		Configuration conf= context.getConfiguration();
		N=conf.getInt("N", 2);
		File file=new File("movie_titles.txt");
		  BufferedReader br=new BufferedReader(new FileReader(file));
		  String line;
		  while((line=br.readLine())!=null){
			  String[] movie=line.split(",");
			  String movie_id=movie[0];
			  String movie_name=movie[2];
			  movieitem.put(movie_id, movie_name);
		  }
		  br.close();
	}
				  
	
  @Override
  public void map(Text key, Text value, Context context)
      throws IOException {
	  
    /*
     * Convert the key, which is received as a Text object,
     * to a String object.
     * 
     * Convert the value, which is received as a Text object,
     * to a integer object.
     * 
     * Check the id of movie stored in the HashMap using the key we get.
     * 
     * If the id is found, put the name of the movie and sum of ratings of the movie in TreeMap.
     * 
     * Check the size of the TreeMap. If the size of it is more than N, then remove the first item(since the TreeMap has already sorted in ascending).
     */
    String KEY=key.toString();
    String VALUE=value.toString().trim();
    int sum_of_ratings=Integer.parseInt(VALUE);
    if(movieitem.containsKey(KEY)){
    	String item=sum_of_ratings+","+movieitem.get(KEY);	
    	top.put(sum_of_ratings, item);
    	if(top.size()>N){
    		top.remove(top.firstKey());
    	}
    }
  }

  /*
	 * Call the write method on the Context object to emit a key
	 * and a value from the map method. 
	 */
  @Override
  protected void cleanup(Context context) throws IOException,InterruptedException {
	  for(String result:top.values()){
		  context.write(NullWritable.get(), new Text(result));
	  }

  }
}