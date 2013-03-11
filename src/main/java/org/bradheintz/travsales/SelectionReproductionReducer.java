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

/**
 * Processes entire sub-populations in 5 steps:
 * 1. Sort by fitness
 * 2. Normalise fitness scores
 * 3. Select survivors with a fitness above a threshold value
 * 4. Create offspring from survivors by choosing parents at random then performing crossover with a
 * chance of mutation (implementation handled by extending class)
 * 5. Write survivors and offspring to file
 */
public class SelectionReproductionReducer extends Reducer<VIntWritable, Text, Text, DoubleWritable> {

	private final static Logger log = Logger.getLogger(SelectionReproductionReducer.class);
	private double survivorProportion;
	private int desiredPopulationSize;
	protected double sideEffectSum = 0.0;
	protected double mutationChance = 0.01;
	protected Random random = new Random();
	protected ChromosomeScorer scorer;
	private Text outKey = new Text();
	private DoubleWritable outValue = new DoubleWritable();
	private Configuration config;

	@Override
	protected void reduce(VIntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		TreeSet<ScoredChromosome> sortedChromosomes = getSortedChromosomeSet(values);
		normalizeScores(sortedChromosomes);

		int survivorsWanted = (int) Math.ceil(sortedChromosomes.size() * survivorProportion);
		Set<ScoredChromosome> survivors = new HashSet<ScoredChromosome>(survivorsWanted);
		while (survivors.size() < survivorsWanted) {
			survivors.add(selectSurvivor(sortedChromosomes));
		}

		// TODO just use survivors for newPopulation - why not? avoid dupes, save making another collection
		ArrayList<ScoredChromosome> parentPool = new ArrayList<ScoredChromosome>(survivors);
		while (survivors.size() < desiredPopulationSize) {
			survivors.add(makeOffspring(parentPool));
		}

		for (ScoredChromosome sc : survivors) {
			outKey.set(sc.getChromosome());
			outValue.set(sc.getScore());
			context.write(outKey, outValue);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		config = context.getConfiguration();
		survivorProportion = context.getConfiguration().getFloat("survivorProportion", 0.3f);
		desiredPopulationSize = context.getConfiguration().getInt("selectionBinSize", 1);

		mutationChance = context.getConfiguration().getFloat("mutationChance", 0.01f);
		//int noImprovementCount = context.getConfiguration().getInt("noImprovementCount", 0);
		//for (; noImprovementCount > 20; noImprovementCount--) {
		//	mutationChance += 0.01;
		//}

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
				return c1.getScore().compareTo(c2.getScore());
			}
		});

		sideEffectSum = 0.0; // computing sum as a side effect saves us a pass over the set, even if it makes us feel dirty-in-a-bad-way
		Iterator<Text> iter = scoredChromosomeStrings.iterator();

		while (iter.hasNext()) {
			Text chromosomeToParse = iter.next();
			ScoredChromosome sc = new ScoredChromosome(chromosomeToParse);
			if (sortedChromosomes.add(sc)) sideEffectSum += sc.getScore();
			log.debug(String.format("SORTING: chromosome: %s, score: %g, accnormscore: %g, SUM: %g", sc.getChromosome(), sc.getScore(), sc.getAccumulatedNormalizedScore(), sideEffectSum));
		}

		return sortedChromosomes;
	}

	protected void normalizeScores(Iterable<ScoredChromosome> scoredChromosomes) {
		Iterator<ScoredChromosome> iter = scoredChromosomes.iterator();
		double accumulatedScore = 0.0;
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			accumulatedScore += sc.getScore() / sideEffectSum;
			sc.setAccumulatedNormalizedScore(accumulatedScore);
			log.debug(String.format("NORMALIZING: chromosome: %s, score: %g, accnormscore: %g", sc.getChromosome(), sc.getScore(), sc.getAccumulatedNormalizedScore()));
		}
	}

	protected ScoredChromosome selectSurvivor(Iterable<ScoredChromosome> scoredAndNormalizedChromosomes) {
		double thresholdScore = random.nextDouble();
		Iterator<ScoredChromosome> iter = scoredAndNormalizedChromosomes.iterator();
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			if (sc.getAccumulatedNormalizedScore() > thresholdScore) {
				log.debug(String.format("SELECTING: chromosome: %s, score: %g, accnormscore: %g, threshold: %g", sc.getChromosome(), sc.getScore(), sc.getAccumulatedNormalizedScore(), thresholdScore));
				return sc;
			}
		}

		return null; // TODO LATER this is a horrible error condition, and I should do something about it
	}

	protected ScoredChromosome makeOffspring(ArrayList<ScoredChromosome> parentPool) throws InterruptedException {
		int parent1Index = random.nextInt(parentPool.size());
		int parent2Index = parent1Index;
		while (parent2Index == parent1Index) {
			parent2Index = random.nextInt(parentPool.size());
		}

		try {
			ScoredChromosome parent1 = parentPool.get(parent1Index);
			ScoredChromosome parent2 = parentPool.get(parent2Index);
			log.debug(String.format("PARENT 1: chromosome: %s, score: %g, accnormscore: %g",
					parent1.getChromosome(), parent1.getScore(), parent1.getAccumulatedNormalizedScore()));
			log.debug(String.format("PARENT 2: chromosome: %s, score: %g, accnormscore: %g",
					parent2.getChromosome(), parent2.getScore(), parent2.getAccumulatedNormalizedScore()));

			ScoredChromosome offspring = crossover(parent1, parent2);
			if (random.nextDouble() < mutationChance) {
				mutate(offspring);
			}

			int hierarchyLevel = config.getInt("hierarchyLevel", 0);
			if (hierarchyLevel != 0 || config.getBoolean("finalHierarchyLevel", true)) {
				offspring.setScore(scorer.score(offspring.getChromosome()));
			}

			return offspring;
		} catch (NullPointerException npe) {
			log.error("*** NullPointerException in makeOffspring()");
			log.error(String.format("parent 1 index: %d parent 2 index: %d pool size: %d",
					parent1Index, parent2Index, parentPool.size()));
			if (parentPool.get(parent1Index) == null) log.error("parent 1 null!");
			if (parentPool.get(parent2Index) == null) log.error("parent 2 null!");
			throw new InterruptedException("null pointer exception in makeOffspring()");
		}
	}

	protected ScoredChromosome crossover(ScoredChromosome parent1, ScoredChromosome parent2) {
		ScoredChromosome offspring = new ScoredChromosome();

		// Random offspring generation to avoid premature convergence if parents equivalent
		if (parent2.getChromosome() == parent1.getChromosome()) {
			offspring.setChromosome(randomlyGenerateChromosome());
		} else {

			int crossoverPoint = random.nextInt(parent1.getChromosomeArray().length - 1) + 1;
			StringBuilder newChromosome = new StringBuilder();
			for (int i = 0; i < crossoverPoint; ++i) {
				newChromosome.append(parent1.getChromosomeArray()[i]);
				newChromosome.append(" ");
			}
			for (int j = crossoverPoint; j < parent2.getChromosomeArray().length; ++j) {
				newChromosome.append(parent2.getChromosomeArray()[j]);
				newChromosome.append(" ");
			}
			offspring.setChromosome(newChromosome.toString().trim());
			//offspring.setChromosome(uniformCrossover(parent1, parent2));
		}

		return offspring;
	}

	private String randomlyGenerateChromosome() {
		int numCities = config.getInt("numCities", 20);
		StringBuilder newChromosome = new StringBuilder();
		for (int j = 0; j < (numCities - 1); ++j) {
            if (j > 0) {
                newChromosome.append(" ");
            }
            newChromosome.append(String.format("%d", random.nextInt(numCities - j)));
        }

		return newChromosome.toString();
	}

	protected String uniformCrossover(ScoredChromosome parent1, ScoredChromosome parent2) {
		StringBuilder newChromosome = new StringBuilder();
		double random;
		for (int i = 0; i < parent1.getChromosomeArray().length; i++) {
			random = Math.random();
			if (random < 0.5) {
				newChromosome.append(parent1.getChromosomeArray()[i]);
				newChromosome.append(" ");
			} else {
				newChromosome.append(parent2.getChromosomeArray()[i]);
				newChromosome.append(" ");
			}
		}

		return newChromosome.toString();
	}

	protected void mutate(ScoredChromosome offspring) {
		int geneToMutate = random.nextInt(offspring.getChromosomeArray().length);
		offspring.setGene(geneToMutate, random.nextInt(offspring.getChromosomeArray().length + 1 - geneToMutate));
	}
}
