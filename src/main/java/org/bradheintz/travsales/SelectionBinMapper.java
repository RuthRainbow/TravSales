/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

/**
 *
 * @author bradheintz
 */
public class SelectionBinMapper extends Mapper<LongWritable, Text, VIntWritable, Text> {
	// Have i misunderstood what the mapper is supposed to do? This program seems to do the scoring in the reducer.
	// Can send to different reducers using a custom partition - could use this to create hierarchy.
    private final static Logger log = Logger.getLogger(SelectionBinMapper.class);

    private int numBins = 10;

    private Random random = new Random();

    private VIntWritable outKey = new VIntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // WHERE THE SELECTION TAKES PLACE
    	// problem - how to pick best n when the pairs come in one by one :-/
    	// as suggested in the guy's blog, could use fitness proportion as it uses an equation and doesn't need all pairs
    	
    	// Use the inverse of the cost as the fitness
    	System.out.println("text is " + value);
    	// The value is the actual chromosome. The final value is the chromosome score / fitness?
    	double fitness = (key.get() == 0 ? 0 : 1/key.get());
    	double fitnessSelectionValue = fitness;
    	// TODO unhardcode the population size
    	for (int i = 0; i < 100000; i++) {
    		// somehow know the values of all the other guy's fitnesses? XD 
    	}
    	
    	
    	//outKey.set(random.nextInt(numBins));
    	outKey.set((int) fitness);
        context.write(outKey, value); // shuffle
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        numBins = context.getConfiguration().getInt("numberOfSelectionBins", 10);
    }

}
