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
public class SelectionBinMapper extends Mapper<LongWritable, Text, VIntWritable, Text> {
    private Random random = new Random();
    protected double migrationChance = 0.01;
    private VIntWritable outKey = new VIntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	// The key is the subpop number, the value is the chromosome/score pair

    	ScoredChromosome sc = new ScoredChromosome(value);
    	float lowerBound = context.getConfiguration().getFloat("lowerBound", Float.MAX_VALUE);
    	// TODO actually we want to DUPLICATE the individual not just move him
    	// Like if he's really good
    	//if (random.nextDouble() < migrationChance) {
    		//outKey.set(Math.abs(random.nextInt()%10));
    	//} else
    	if (sc.score > lowerBound) {
    		//Need to somehow keep track of a high up score
    		broadcastToNeighbours(sc, key, value, context);
    	}
    	// Always send key to it's own reducer (don't remove individuals here)
    	outKey.set(Integer.valueOf(key.toString()));
        context.write(outKey, value);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    private void broadcastToNeighbours(ScoredChromosome sc, LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	int keyValue = Integer.valueOf(key.toString());
    	int numSubPopulations = context.getConfiguration().getInt("numSubPopulations", 0);
    	VIntWritable currOutKey = new VIntWritable();
    	if (keyValue > 0) {
    		currOutKey.set(keyValue - 1);
    	} else {
    		currOutKey.set(numSubPopulations-1);
    	}
    	context.write(currOutKey, value);

    	if (keyValue < numSubPopulations-1){
    		currOutKey.set(keyValue + 1);
    	} else {
    		currOutKey.set(0);
    	}
    	context.write(currOutKey, value);
    }

}
