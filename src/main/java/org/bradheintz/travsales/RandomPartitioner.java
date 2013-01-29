package org.bradheintz.travsales;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * Class to send individuals to a random reducer, rather than being based on their key
 */
public class RandomPartitioner extends Partitioner<VIntWritable, Text> {

	@Override
	public int getPartition(VIntWritable key, Text value, int numReducers) {
		return (int) Math.floor(Math.random()*(numReducers-1));
	}

}
