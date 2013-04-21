/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.generalalgorithm;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bradheintz.generalalgorithm.InitialJob.Topology;

/**
 * Sends values to reducers based on their sub-population index. Also handles migration between
 * sub-populations based on the topology.
 *
 * @author ruthking
 */
public class SelectionBinMapper extends Mapper<LongWritable, Text, VIntWritable, Text> {
    private VIntWritable outKey = new VIntWritable();
    private int numSubPopulations;
    private int keyValue;
    private Text value;

    // The key is the subpopulation identifier, the value is the chromosome/score pair
    @Override
    protected void map(LongWritable key, Text value, Context context)
    		throws IOException, InterruptedException {
    	ScoredChromosome sc = new ScoredChromosome(value);
    	keyValue = Integer.valueOf(key.toString());
    	this.value = value;

    	// Get parameter values required from the configuration
    	Topology topology = context.getConfiguration().getEnum("topology", Topology.RING);
    	float lowerBound = context.getConfiguration().getFloat(
    			"lowerBound" + keyValue, Float.MAX_VALUE);
    	numSubPopulations = context.getConfiguration().getInt("numSubPopulations", 1);
    	int selectionBinSize = context.getConfiguration().getInt("selectionBinSize", 100);
    	boolean isMigrate = context.getConfiguration().getBoolean("isMigrate", false);

    	double random = Math.random();
    	double randomThreshold = 1/selectionBinSize;

    	// Migrate if a migration generation and score is above threshold or through random chance
    	if (isMigrate && (sc.getScore() > lowerBound || random <= randomThreshold)) {
    		switch (topology) {
    			case RING: ringBroadcast(sc, context); break;
    			case HYPERCUBE: hypercubeBroadcast(sc, context); break;
    		}
    	}

    	// Write the individual to its original subpopulation
    	outKey.set(Integer.valueOf(key.toString()));
    	context.write(outKey, value);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    // Migrate given individual in a ring topology to neighbouring subpopulations
    private void ringBroadcast(ScoredChromosome sc, Context context)
    		throws IOException, InterruptedException {
    	VIntWritable currOutKey = new VIntWritable();

    	int numSubPops = context.getConfiguration().getInt("numSubPopulations", 10);

    	// Migrate to the left subpopulation
    	if (keyValue % numSubPops == 0) {
    		currOutKey.set(keyValue + numSubPops - 1);
    	} else {
    		currOutKey.set(keyValue - 1);
    	}
    	context.write(currOutKey, value);

    	// Migrate to the right subpopulation
    	if ((keyValue + 1) % numSubPops == 0) {
    		currOutKey.set(keyValue - numSubPops + 1);
    	} else {
    		currOutKey.set(keyValue + 1);
    	}
    	context.write(currOutKey, value);
    }

    // Migrate given individual in a hypercube topology to neighbouring subpopulations
    private void hypercubeBroadcast(ScoredChromosome sc, Context context)
    		throws IOException, InterruptedException {
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

    /* Add leading zeroes to a string using a mutable StringBuilder so all strings are the same
     * number of binary digits */
    private String addLeadingZeros(String binaryString, int numBinaryDigits) {
    	int strLength = binaryString.length();
    	StringBuilder strBuilder = new StringBuilder();
    	while ((strBuilder.length() + strLength) < numBinaryDigits) {
			strBuilder.append('0');
		}
		strBuilder.append(binaryString);
		return strBuilder.toString();
	}

    // Flip a binary digit
	private char flipDigit(int i) {
    	if (i == '0') {
    		return '1';
    	} else {
    		return '0';
    	}
    }

	// Convert a character array to a string
    private String charArrToString(char[] arr) {
    	StringBuilder strBuilder = new StringBuilder();
    	for (char c : arr) {
    		strBuilder.append(c);
    	}
    	return strBuilder.toString();
    }

}
