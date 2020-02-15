package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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

public class AggregateRatingsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line, which is received as a Text object,
     * to a String object.
     */
    String line = value.toString().trim();

    /*
     * The line.split(",") call uses regular expressions to split the
     * line up by the comma.
     * 
     * The first part is put into to a string named movieid.
     * 
     * The second part is converted to double first, and then converted to int.
     */
    	String[] record=line.split(",");
    	String movieid=record[0];
    	double number=Double.parseDouble(record[2]);
    	int rating=(int)number;

    	/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the map method. 
		 */
    	context.write(new Text(movieid), new IntWritable(rating));
  }
}