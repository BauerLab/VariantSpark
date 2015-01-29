package au.csiro;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
 
public class IndividualKeyPartitioner extends Partitioner<IntIntComposite, DoubleWritable> {
 
	HashPartitioner<IntWritable, DoubleWritable> hashPartitioner = new HashPartitioner<IntWritable, DoubleWritable>();
	IntWritable newKey = new IntWritable();
	 
	@Override
	public int getPartition(IntIntComposite key, DoubleWritable value, int numReduceTasks) {
		 
		try {
			// Execute the default partitioner over the first part of the key
			newKey =key.getIndividualId();
			return hashPartitioner.getPartition(newKey, value, numReduceTasks);
		} catch (Exception e) {
			e.printStackTrace();
			return (int) (Math.random() * numReduceTasks); // this would return a random value in the range
			// [0,numReduceTasks)
		}
	}
}