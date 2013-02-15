/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bradheintz.travsales.TravSalesJob.Topology;

/**
 * @author bradheintz
 *
 * The map class should work out a fitness value for each individual and write this as the value.
 */
public class SelectionBinMapper extends Mapper<LongWritable, Text, VIntWritable, Text> {
    private VIntWritable outKey = new VIntWritable();
    private int numSubPopulations;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	// The key is the subpop number, the value is the chromosome/score pair

    	ScoredChromosome sc = new ScoredChromosome(value);
    	Topology topology = context.getConfiguration().getEnum("topology", Topology.RING);
    	float lowerBound = context.getConfiguration().getFloat("lowerBound", Float.MAX_VALUE);
    	numSubPopulations = context.getConfiguration().getInt("numSubPopulations", 1);
    	int migrationRate = context.getConfiguration().getInt("migrationRate", 1);
    	int generation = context.getConfiguration().getInt("generation", 1);
    	boolean isMigrate = generation%migrationRate == 0 ? true : false;

    	if (isMigrate && sc.score > lowerBound) {
    		switch (topology) {
    			case RING: ringBroadcast(sc, key, value, context); break;
    			case HYPERCUBE: hypercubeBroadcast(sc, key, value, context); break;
    		}
    	}
    	// Always send key to it's own reducer (don't remove individuals here)
    	outKey.set(Integer.valueOf(key.toString()));
        context.write(outKey, value);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    // TODO don't go outside subpops
    private void ringBroadcast(ScoredChromosome sc, LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	int keyValue = Integer.valueOf(key.toString());
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

    private void hypercubeBroadcast(ScoredChromosome sc, LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	int keyValue = Integer.valueOf(key.toString());
    	VIntWritable currOutKey = new VIntWritable();

    	// The node index in the hypercube - start form 0
    	int numBinaryDigits = (int) Math.ceil(Math.log(numSubPopulations)/Math.log(2));
    	String binaryString = Integer.toBinaryString(keyValue);
    	binaryString = addLeadingZeros(binaryString, numBinaryDigits);
    	char[] binaryDigits = binaryString.toCharArray();

    	// The neighbours we need to send to all differ by 1 binary digit.
    	for (int i = 0; i < binaryString.length(); i++) {
    		// flip the ith digit and if this is a node migrate to it
    		char ith = binaryDigits[i];
    		char flippedIth = flipDigit(ith);
    		binaryDigits[i] = flippedIth;
    		int outKeyValue = Integer.parseInt(charArrToString(binaryDigits), 2);
    		if (outKeyValue < numSubPopulations) {
    			currOutKey.set(outKeyValue);
    			context.write(currOutKey, value);
    		}
    		binaryDigits[i] = ith;
    	}
    }

    private String addLeadingZeros(String binaryString, int numBinaryDigits) {
    	int strLength = binaryString.length();
    	StringBuilder strBuilder = new StringBuilder();
    	while ((strBuilder.length() + strLength) < numBinaryDigits) {
			strBuilder.append('0');
		}
		strBuilder.append(binaryString);
		return strBuilder.toString();
	}

	private char flipDigit(int i) {
    	if (i == '0') {
    		return '1';
    	} else {
    		return '0';
    	}
    }

    private String charArrToString(char[] arr) {
    	StringBuilder strBuilder = new StringBuilder();
    	for (char c : arr) {
    		strBuilder.append(c);
    	}
    	return strBuilder.toString();
    }

}
