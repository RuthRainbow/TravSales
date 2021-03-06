/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.generalalgorithm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Class dedicated to scoring the initial population
 *
 * @author bradheintz
 */
public abstract class ScoringMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outKey = new Text();
    private DoubleWritable outValue = new DoubleWritable();

    protected ChromosomeScorer scorer = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String incomingValue[] = value.toString().split("\t");
        String chromosome = incomingValue[0];
        double score = 0.0;
        if (incomingValue.length > 1) {
            String scoreString = incomingValue[1];
            try {
                score = Double.parseDouble(scoreString);
            } catch(NumberFormatException nfe) {
                // weird - but let's try re-generating the score
                score = scorer.score(value.toString());
            }
        } else { // we only go through the scoring process if we don't have a map
            score = scorer.score(value.toString());
        }

        outValue.set(score);
        outKey.set(chromosome);
        context.write(outKey, outValue);
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();
        createScorer(config);
    }

	protected abstract void createScorer(Configuration config) throws InterruptedException;
}
