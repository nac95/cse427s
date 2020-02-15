package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
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
public class AggregateRatings extends Configured implements Tool{

  public static void main(String[] args) throws Exception {
		  int exitCode=ToolRunner.run(new Configuration(),new AggregateRatings(),args);
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

    JobControl TopNList=new JobControl("TopNList");
    /*
     * Instantiate a Job object for your job's configuration.  
     */
    Job job1 = new Job(getConf());
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running
     * mapper and reducer tasks.
     */
    job1.setJarByClass(AggregateRatings.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job1.setJobName("Aggregate Ratings");

    /*
     * Specify the paths to the input and output data based on the
     * command-line arguments.
     */
    FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    /*
     * Specify the mapper, reducer and combiner classes.
     */
    job1.setMapperClass(AggregateRatingsMapper.class);
    job1.setCombinerClass(SumReducer.class);
    job1.setReducerClass(SumReducer.class);
    
    /*
     * For the Aggregate Ratings application, the input file and output 
     * files are in text format - the default format.
     * 
     * In text format files, each record is a line delineated by a 
     * by a line terminator.
     */
      
    /*
     * For the Aggregate Ratings application, the mapper's output keys and
     * values have the same data types as the reducer's output keys 
     * and values: Text and IntWritable.
     */

    /*
     * Specify the job's output key and value classes.
     */
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job1.waitForCompletion(true);
    return success ? 0 : 1;
  }
}

