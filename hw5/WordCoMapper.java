package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCoMapper extends Mapper<LongWritable, Text, StringPairWritable, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  /*
	   * Convert the line, which is received as a Text object, to a String object
	   */
	  String line=value.toString();
	  
	  /*
	     * The line.split("\\W+") call uses regular expressions to split the
	     * line up by non-word characters.
	     */
	  String[] words = line.split("\\W+");
	  
	  if (words.length > 2) {
	        
	        /*
	         * Call the write method on the Context object to emit a key
	         * and a value from the map method.
	         */
	        context.write(new StringPairWritable(words[0],words[1]), new IntWritable(1));
	      }
    
  }
}
