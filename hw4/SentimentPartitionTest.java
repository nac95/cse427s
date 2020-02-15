package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner spart;

	@Test
	public void testSentimentPartition() {

		spart=new SentimentPartitioner();
		spart.setConf(new Configuration());
		int result;		
		
		/*
		 * A test for word "beauty" with expected outcome 0 would   
		 * look like this:
		 */
		result = spart.getPartition(new Text("beauty"), new IntWritable(23), 3);
		assertEquals(result,0);	

		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
        
 		/*
		 * TODO implement
		 */          
		result = spart.getPartition(new Text("love"), new IntWritable(349), 3);
		assertEquals(result,0);	
		result = spart.getPartition(new Text("deadly"), new IntWritable(56), 3);
		assertEquals(result,1);	
		result = spart.getPartition(new Text("zodiac"), new IntWritable(1), 3);
		assertEquals(result,2);	
	}

}
