package org.bradheintz.travsales;

import org.bradheintz.generalalgorithm.SelectionReproductionReducer;


/**
 * Implementation of SelectionReproductionReducer for the TravSales algorithm
 *
 * @author bradheintz, ruthking
 */
public class TravSalesReducer extends SelectionReproductionReducer {

	// This method is overridden as the problem is the city map for TravSales
	@Override
	protected void checkProblemExists() throws InterruptedException {
		if (config.get("cities") == null) {
			throw new InterruptedException("Failure! No problem.");
		}
	}

	// Generate the TSP specific scorer using the city map
	@Override
	protected void createScorer() throws InterruptedException {
		scorer = new TravSalesScorer(config.get("cities"));
	}

	// Randomly generate a chromosome, specific to TSP
	protected String randomlyGenerateChromosome() {
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
}
