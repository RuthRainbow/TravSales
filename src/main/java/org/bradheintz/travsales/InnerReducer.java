package org.bradheintz.travsales;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class InnerReducer extends Reducer<VIntWritable, Text, Text, DoubleWritable> {

	private final static Logger log = Logger.getLogger(SelectionReproductionReducer.class);
	private double survivorProportion;
	private double topTierProportion;
	private int numSubsets = 5;
	private int desiredPopulationSize;
	protected double sideEffectSum = 0.0;
	protected double mutationChance = 0.01;
	protected Random random = new Random();
	protected ChromosomeScorer scorer;
	private Text outKey = new Text();
	private DoubleWritable outValue = new DoubleWritable();

	@Override
	protected void reduce(VIntWritable key, Iterable<Text> values, Context context) {
		//TreeSet<ScoredChromosome> sortedChromosomes = getSortedChromosomeSet(values);
		/*normalizeScores(sortedChromosomes);

		int survivorsWanted = (int) ((double) sortedChromosomes.size() * survivorProportion);
		Set<ScoredChromosome> survivors = new HashSet<ScoredChromosome>(survivorsWanted);

		// int topTier = (int)((double)sortedChromosomes.size() * topTierProportion);
		// while (survivors.size() < topTier) {
		// need a reverse iterator or something
		// }

		while (survivors.size() < survivorsWanted) {
			survivors.add(selectSurvivor(sortedChromosomes));
		}

		// TODO just use survivors for newPopulation - why not? avoid dupes, save making another collection
		ArrayList<ScoredChromosome> parentPool = new ArrayList<ScoredChromosome>(survivors);

		while (survivors.size() < desiredPopulationSize) {
			survivors.add(makeOffspring(parentPool));
		}

		Iterator<ScoredChromosome> iter = survivors.iterator();
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			outKey.set(sc.chromosome);
			outValue.set(sc.score);
			context.write(outKey, outValue);
		}*/
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		survivorProportion = context.getConfiguration().getFloat("survivorProportion", 0.3f);
		topTierProportion = context.getConfiguration().getFloat("topTierToSave", 0.0f);
		//desiredPopulationSize = context.getConfiguration().getInt("selectionBinSize", 1000);
		desiredPopulationSize = 1000;
		mutationChance = context.getConfiguration().getFloat("mutationChance", 0.01f);
		if (config.get("cities") == null) {
			throw new InterruptedException("Failure! No city map.");
		}
		scorer = new ChromosomeScorer(config.get("cities"));
		if (scorer.cities.size() < 3) {
			throw new InterruptedException("Failure! Invalid city map.");
		}
	}
}
