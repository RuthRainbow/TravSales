package org.bradheintz.travsales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * Class to send individuals to a random reducer, rather than being based on their key
 */
public class RandomPartitioner extends Partitioner<DoubleWritable, Text> {

	@Override
	public int getPartition(DoubleWritable key, Text value, int numReducers) {
		// HERE IS THE PROBLEM. THERE IS ONLY A SINGLE REDUCER.

		//int toReturn = (int) Math.floor(Math.random()*(numReducers-1));
		int toReturn = (int) Math.floor(Math.random()*10);
		System.out.println("returning " + toReturn + " and num reducers is " + numReducers);
		if (numReducers > 1) {
			return toReturn;
		} else {
			return 0;
		}

	}

}
