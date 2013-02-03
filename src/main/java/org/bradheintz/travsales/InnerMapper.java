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
    protected double migrationChance = 0.01;
    private Random random = new Random();
    private VIntWritable outKey = new VIntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	// The key is the subpop number, the value is the chromosome/score pair
    	if (random.nextDouble() < migrationChance) {
    		// TODO unhardcode
    		outKey.set(Math.abs(random.nextInt()%100));
    	} else {
    		outKey.set(Integer.valueOf(key.toString()));
    	}
        context.write(outKey, value); // shuffle
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

}

