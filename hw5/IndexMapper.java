package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

	String filename=new String();
	
	public void setup(Context context){
		FileSplit split=(FileSplit)context.getInputSplit();
		filename=split.getPath().getName();
		
	}
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {

    /*
     * TODO implement
     */
	  String text=value.toString();
	  text=text.toLowerCase();
	  for(String line:text.split("\n")){
		  String index=line.split("\t")[0];
		  for (String word:line.split("\\w+")){
			  if(word.length()>0){
				  String Index=filename+"@"+index;
				  context.write(new Text(word),new Text(Index));
			  }
		  }
	  }
  }
}