package stubs;

import java.io.*;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();


  @Override
  public void setConf(Configuration configuration) {
    /*
     * TODO implement if necessary
     */
	  this.configuration=configuration;
	  /*
	   * access the file. 
	   * get the content from the file and split them into the two hashset, positive and negative.
	   */
	File file1=new File("positive-words.txt");
	Scanner positivefile=null;
	try {
		positivefile=new Scanner(file1);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		System.out.println(e.getMessage());
	}
	String str1;
	while(positivefile.hasNextLine()){
		str1=positivefile.nextLine();
		if(str1.charAt(0)!=';'){
		   positive.add(str1.trim());
		}
	  }
	positivefile.close();
	File file2=new File("negative-words.txt");
	Scanner negativefile=null;
	try {
		negativefile=new Scanner(file2);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		System.out.println(e.getMessage());
	} 
	String str2;
	while(negativefile.hasNextLine()){
		str2=negativefile.nextLine();
		if(str2.charAt(0)!=';'){
			negative.add(str2.trim());
			}
	  }
	negativefile.close();
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	  
	  /*
	   * check whether the word is positive or negative, and return the corresponding sentiment number.
	   */
	  if(positive.contains(key.toString())){
		  return 0;
	  }else if(negative.contains(key.toString())){
			  return 1;
	  }else{
		  return 2;
	  }
  }
}

