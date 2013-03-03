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
 *
 * This mapper handles migration between sub-populations
 */
public class SelectionBinMapper extends Mapper<LongWritable, Text, VIntWritable, Text> {
    private VIntWritable outKey = new VIntWritable();
    private int numSubPopulations;
    private int keyValue;
    // The key is the sub-pop number, the value is the chromosome/score pair
    @Override
    protected void map(LongWritable key, Text value, Context context)
    		throws IOException, InterruptedException {
    	ScoredChromosome sc = new ScoredChromosome(value);
    	keyValue = Integer.valueOf(key.toString());
    	Topology topology = context.getConfiguration().getEnum("topology", Topology.RING);
    	float lowerBound = context.getConfiguration().getFloat("lowerBound" + keyValue, Float.MAX_VALUE);
    	numSubPopulations = context.getConfiguration().getInt("numSubPopulations", 1);
    	int migrationRate = context.getConfiguration().getInt("migrationRate", 1);
    	boolean isMigrate = migrationRate == 0 ? true : false;

    	if (isMigrate && sc.score > lowerBound) {
    		switch (topology) {
    			case RING: ringBroadcast(sc, value, context); break;
    			case HYPERCUBE: hypercubeBroadcast(sc, value, context); break;
    		}
    	}
    	// Always send key to it's own reducer (don't remove individuals from sub-populations here)
    	outKey.set(Integer.valueOf(key.toString()));
        context.write(outKey, value);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    private void ringBroadcast(ScoredChromosome sc, Text value, Context context)
    		throws IOException, InterruptedException {
    	VIntWritable currOutKey = new VIntWritable();

    	int selectionBinSize = context.getConfiguration().getInt("selectionBinSize", 10);

    	// Migrate to the left sub-population
    	if (keyValue % selectionBinSize == 0) {
    		currOutKey.set(keyValue + selectionBinSize - 1);
    	} else {
    		currOutKey.set(keyValue - 1);
    	}
    	context.write(currOutKey, value);

    	// Migrate to the right sub-population
    	if ((keyValue + 1) % selectionBinSize == 0) {
    		currOutKey.set(keyValue - selectionBinSize - 1);
    	} else {
    		currOutKey.set(keyValue + 1);
    	}
    	context.write(currOutKey, value);
    }

    private void hypercubeBroadcast(ScoredChromosome sc, Text value,
    		Context context) throws IOException, InterruptedException {
    	VIntWritable currOutKey = new VIntWritable();

    	// The node index in the hypercube - start from 0
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
