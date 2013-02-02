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

/**
 * @author bradheintz
 *
 * The map class should work out a fitness value for each individual and write this as the value.
 */
public class InnerMapper extends Mapper<LongWritable, Text, VIntWritable, Text> {
	private int numBins = 100;

    private Random random = new Random();

    private VIntWritable outKey = new VIntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	// The key is the actual chromosome. The final value is the chromosome score / fitness
    	// No - key is 0 and value is chromosome & score...
    	/*Configuration config = context.getConfiguration();
    	ChromosomeScorer scorer = new ChromosomeScorer(config.get("cities"));
    	String[] fields = value.toString().split("\t");
    	double fitness = scorer.score(fields[0]);
    	Text finalValue = new Text(String.valueOf(key));
    	DoubleWritable finalKey = new DoubleWritable(fitness);
    	context.write(finalKey, value);
    	PROBLEM HERE. Each having his own fitness value sends him to the same reducer.
 */
    	outKey.set(random.nextInt(numBins));
        context.write(outKey, value); // shuffle
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        numBins = context.getConfiguration().getInt("numberOfSelectionBins", 10);
    }

}

