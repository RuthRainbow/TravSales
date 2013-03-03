package org.bradheintz.travsales;

import java.util.ArrayList;

import org.apache.log4j.Logger;

/**
 * The reducer used by the innermost hierarchy level - performs crossover and mutation with no scoring.
 *
 * @author bradheintz
 */
public class TopLevelReducer extends SelectionReproductionReducer {

	private final static Logger log = Logger.getLogger(TopLevelReducer.class);

	protected ScoredChromosome makeOffspring(ArrayList<ScoredChromosome> parentPool) throws InterruptedException {
		int parent1Index = random.nextInt(parentPool.size());
		int parent2Index = parent1Index;
		if (parentPool.size() > 1) {
			while (parent2Index == parent1Index) {
				parent2Index = random.nextInt(parentPool.size());
			}
		}

		try {
			ScoredChromosome parent1 = parentPool.get(parent1Index);
			ScoredChromosome parent2 = parentPool.get(parent2Index);
			log.debug(String.format("PARENT 1: chromosome: %s, score: %g, accnormscore: %g", parent1.chromosome, parent1.score, parent1.accumulatedNormalizedScore));
			log.debug(String.format("PARENT 2: chromosome: %s, score: %g, accnormscore: %g", parent2.chromosome, parent2.score, parent2.accumulatedNormalizedScore));

			ScoredChromosome offspring = crossover(parent1, parent2);
			if (random.nextDouble() < mutationChance) {
				mutate(offspring);
			}
			return offspring;
		} catch (NullPointerException npe) {
			log.error("*** NullPointerException in makeOffspring()");
			log.error(String.format("parent 1 index: %d   parent 2 index: %d   pool size: %d", parent1Index, parent2Index, parentPool.size()));
			if (parentPool.get(parent1Index) == null) log.error("parent 1 null!");
			if (parentPool.get(parent2Index) == null) log.error("parent 2 null!");
			throw new InterruptedException("null pointer exception in makeOffspring()");
		}
	}

}
