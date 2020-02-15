package stubs;
import java.io.IOException;

//import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;


public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//	create the file variable caseSensitive
	private boolean caseSensitive;
//	build the setup() method to retrieve a parameter. the default value is true, which means case sensitive.
	public void setup(Context context){
		Configuration conf= context.getConfiguration();
		caseSensitive = conf.getBoolean("caseSensitive", true);
	}

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  
	  /*
	     * Convert the line, which is received as a Text object,
	     * to a String object.
	     */
	    String line = value.toString();
	    /*
	     * The line.split("\\W+") call uses regular expressions to split the
	     * line up by non-word characters.
	     * 
	     * If you are not familiar with the use of regular expressions in
	     * Java code, search the web for "Java Regex Tutorial." 
	     */
	    /*
         * Call the write method on the Context object to emit a key
         * and a value from the map method.
         */

	    for (String word : line.split("\\W+")) {
	      if (word.length() > 0) {
	        
			if(caseSensitive){
	        	context.write(new Text(word.substring(0, 1)), new IntWritable(word.length()));
	        }else{
	        	context.write(new Text(word.substring(0, 1).toLowerCase()),new IntWritable(word.length()));
	        }
	        
	      }
	    }
	}
}
