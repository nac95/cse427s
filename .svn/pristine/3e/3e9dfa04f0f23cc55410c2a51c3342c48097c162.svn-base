package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class AvgWordLength extends Configured implements Tool {
	
  public static void main(String[] args) throws Exception {
	  int exitCode=ToolRunner.run(new Configuration(),new AvgWordLength(),args);
	  System.exit(exitCode);
  }
  public int run(String[] args) throws Exception{
    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: AvgWordLength <input dir> <output dir>\n");
      return -1;
    }

    
    /*
     * Instantiate a Job object for your job's configuration if only using toolrunner. 
     */

    Job job=new Job(getConf());
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(AvgWordLength.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("Average Word Length");

    /*
     * TODO implement
     */
    /*
     * Specify the paths to the input and output data based on the
     * command-line arguments.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(LetterMapper.class);
    job.setReducerClass(AverageReducer.class);

    /*
     * For the word count application, the input file and output 
     * files are in text format - the default format.
     * 
     * In text format files, each record is a line delineated by a 
     * by a line terminator.
     * 
     * When you use other input formats, you must call the 
     * SetInputFormatClass method. When you use other 
     * output formats, you must call the setOutputFormatClass method.
     */
      
    /*
     * For the word count application, the mapper's output keys and
     * values have the same data types as the reducer's output keys 
     * and values: Text and IntWritable.
     * 
     * When they are not the same data types, you must call the 
     * setMapOutputKeyClass and setMapOutputValueClass 
     * methods.
     */

    /*
     * Specify the job's output key and value classes.
     */
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    return success ? 0:1;
  }
}

