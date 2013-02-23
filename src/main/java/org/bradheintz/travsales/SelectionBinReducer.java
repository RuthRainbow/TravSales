package org.bradheintz.travsales;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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

public abstract class SelectionBinReducer extends Reducer<VIntWritable, Text, Text, DoubleWritable> {

	private final static Logger log = Logger.getLogger(TopLevelReducer.class);
	private double survivorProportion;
	private int desiredPopulationSize;
	protected double sideEffectSum = 0.0;
	protected double mutationChance = 0.01;
	protected Random random = new Random();
	protected ChromosomeScorer scorer;
	private Text outKey = new Text();
	private DoubleWritable outValue = new DoubleWritable();

	@Override
	protected void reduce(VIntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		//System.out.println("in inner reduce, the key is " + key);
		TreeSet<ScoredChromosome> sortedChromosomes = getSortedChromosomeSet(values);
		normalizeScores(sortedChromosomes);

		//System.out.println("sorted chrom size is " + sortedChromosomes.size());
		int survivorsWanted = (int) Math.ceil(sortedChromosomes.size() * survivorProportion);
		Set<ScoredChromosome> survivors = new HashSet<ScoredChromosome>(survivorsWanted);

		//System.out.println("survivors wanted " + survivorsWanted);
		while (survivors.size() < survivorsWanted) {
			survivors.add(selectSurvivor(sortedChromosomes));
		}

		// TODO just use survivors for newPopulation - why not? avoid dupes, save making another collection
		ArrayList<ScoredChromosome> parentPool = new ArrayList<ScoredChromosome>(survivors);

		//System.out.println("desiredPopulationSize " + desiredPopulationSize);
		while (survivors.size() < desiredPopulationSize) {
			survivors.add(makeOffspring(parentPool));
		}

		for (ScoredChromosome sc : survivors) {
			outKey.set(sc.chromosome);
			outValue.set(sc.score);
			context.write(outKey, outValue);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		survivorProportion = context.getConfiguration().getFloat("survivorProportion", 0.3f);
		desiredPopulationSize = context.getConfiguration().getInt("selectionBinSize", 1);
		mutationChance = context.getConfiguration().getFloat("mutationChance", 0.01f);
		if (config.get("cities") == null) {
			throw new InterruptedException("Failure! No city map.");
		}
		scorer = new ChromosomeScorer(config.get("cities"));
		if (scorer.cities.size() < 3) {
			throw new InterruptedException("Failure! Invalid city map.");
		}
	}

	protected TreeSet<ScoredChromosome> getSortedChromosomeSet(Iterable<Text> scoredChromosomeStrings) {
		TreeSet<ScoredChromosome> sortedChromosomes = new TreeSet<ScoredChromosome>(new Comparator<ScoredChromosome>() {
			@Override
			public int compare(ScoredChromosome c1, ScoredChromosome c2) {
				return c1.score.compareTo(c2.score);
			}
		});

		sideEffectSum = 0.0; // computing sum as a side effect saves us a pass over the set, even if it makes us feel dirty-in-a-bad-way
		Iterator<Text> iter = scoredChromosomeStrings.iterator();

		while (iter.hasNext()) {
			Text chromosomeToParse = iter.next();
			ScoredChromosome sc = new ScoredChromosome(chromosomeToParse);
			if (sortedChromosomes.add(sc)) sideEffectSum += sc.score;
			log.debug(String.format("SORTING: chromosome: %s, score: %g, accnormscore: %g, SUM: %g", sc.chromosome, sc.score, sc.accumulatedNormalizedScore, sideEffectSum));
		}

		return sortedChromosomes;
	}

	protected void normalizeScores(Iterable<ScoredChromosome> scoredChromosomes) {
		Iterator<ScoredChromosome> iter = scoredChromosomes.iterator();
		double accumulatedScore = 0.0;
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			accumulatedScore += sc.score / sideEffectSum;
			sc.accumulatedNormalizedScore = accumulatedScore;
			log.debug(String.format("NORMALIZING: chromosome: %s, score: %g, accnormscore: %g", sc.chromosome, sc.score, sc.accumulatedNormalizedScore));
		}
	}

	protected ScoredChromosome selectSurvivor(Iterable<ScoredChromosome> scoredAndNormalizedChromosomes) {
		double thresholdScore = random.nextDouble();
		Iterator<ScoredChromosome> iter = scoredAndNormalizedChromosomes.iterator();
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			if (sc.accumulatedNormalizedScore > thresholdScore) {
				log.debug(String.format("SELECTING: chromosome: %s, score: %g, accnormscore: %g, threshold: %g", sc.chromosome, sc.score, sc.accumulatedNormalizedScore, thresholdScore));
				return sc;
			}
		}

		return null; // LATER this is a horrible error condition, and I should do something about it
	}

	protected abstract ScoredChromosome makeOffspring(ArrayList<ScoredChromosome> parentPool) throws InterruptedException;

	protected ScoredChromosome crossover(ScoredChromosome parent1, ScoredChromosome parent2) {
		int crossoverPoint = random.nextInt(parent1.getChromosomeArray().length - 1) + 1;
		ScoredChromosome offspring = new ScoredChromosome();

		StringBuilder newChromosome = new StringBuilder();
		for (int i = 0; i < crossoverPoint; ++i) {
			newChromosome.append(parent1.getChromosomeArray()[i]);
			newChromosome.append(" ");
		}
		for (int j = crossoverPoint; j < parent2.getChromosomeArray().length; ++j) {
			newChromosome.append(parent2.getChromosomeArray()[j]);
			newChromosome.append(" ");
		}
		offspring.chromosome = newChromosome.toString().trim();

		return offspring;
	}

	protected void mutate(ScoredChromosome offspring) {
		int geneToMutate = random.nextInt(offspring.getChromosomeArray().length);
		offspring.setGene(geneToMutate, random.nextInt(offspring.getChromosomeArray().length + 1 - geneToMutate));
	}
}