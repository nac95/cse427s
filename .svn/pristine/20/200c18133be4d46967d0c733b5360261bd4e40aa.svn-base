package stubs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCo extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: WordCo <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(WordCo.class);
    job.setJobName("Custom Writable Comparable");

    /*
     * specify the paths to the input and output data based on the command-line arguments. 
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    /*
	 * Specify the mapper and reducer classes.
	 */
	job.setMapperClass(WordCoMapper.class);
	job.setReducerClass(SumReducer.class);
	
	/*
	 * Specify the job's output key and value classes.
	 */
	job.setOutputKeyClass(StringPairWritable.class);
	job.setOutputValueClass(IntWritable.class);

	/*
	 * Start the MapReduce job and wait for it to finish. If it finishes
	 * successfully, return 0. If not, return 1.
	 */
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new WordCo(), args);
    System.exit(exitCode);
  }
}
