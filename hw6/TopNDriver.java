package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* 
 * MapReduce jobs are typically implemented by using a driver class.
 * The purpose of a driver class is to set up the configuration for the
 * MapReduce job and to run the job.
 * Typical requirements for a driver class include configuring the input
 * and output data formats, configuring the map and reduce classes,
 * and specifying intermediate data formats.
 * 
 * The following is the code for the driver class:
 */
public class TopNDriver extends Configured implements Tool{

  public static void main(String[] args) throws Exception {
		  int exitCode=ToolRunner.run(new Configuration(),new TopNDriver(),args);
		  System.exit(exitCode);
	  }
	  public int run(String[] args) throws Exception{

    /*
     * The expected command-line arguments are the paths containing
     * input and output data. Terminate the job if the number of
     * command-line arguments is not exactly 2.
     */
    if (args.length != 2) {
      System.out.printf(
          "Usage: WordCount <input dir> <output dir>\n");
      return -1;
    }

    /*
     * Instantiate a Job object for your job's configuration.  
     */
    Job job2 = new Job(getConf());

    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running
     * mapper and reducer tasks.
     */
    job2.setJarByClass(TopNDriver.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job2.setJobName("Top N List");

    /*
     * The InputFormat is KeyValueTextInputFormat, which is not default. 
     */
    job2.setInputFormatClass(KeyValueTextInputFormat.class);
    
    /*
     * Specify the paths to the input and output data based on the
     * command-line arguments.
     */
    FileInputFormat.setInputPaths(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

    /*
     * Specify the mapper and reducer classes.
     */
    job2.setMapperClass(TopNMapper.class);
    job2.setReducerClass(TopNReducer.class);
    
    /*
     * For the Top N List application, the input file and output 
     * files are in text format - the default format.
     * 
     * In text format files, each record is a line delineated by a 
     * by a line terminator.
     */
      
    /*
     * For the Top N List application, the mapper's output keys and
     * values have the different data types as the reducer's output keys 
     * and values: IntWritable and Text.
     * 
     * The data types of mapper's output keys and values: NullWritable and Text
     */
    job2.setMapOutputKeyClass(NullWritable.class);   
    job2.setMapOutputValueClass(Text.class);   

    /*
     * Specify the job's output key and value classes.
     */
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);

    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job2.waitForCompletion(true);
    return success ? 0 : 1;
  }
}

