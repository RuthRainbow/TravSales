package org.bradheintz.generalalgorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class holds statistics for a single subpopulation, and calculates the lower bound for
 * migration and the mean fitness value.
 *
 * @author ruthking
 */
public class SubpopulationStatistics {

	protected ScoredChromosome bestChrom;
	protected ScoredChromosome worstChrom;
	protected float lowerBound;
	protected double meanFitness;
	protected double standardDev;
	protected List<ScoredChromosome> threshold;
	private int count;

	// On construction initialise the data structures and objects
	public SubpopulationStatistics() {
		threshold = new ArrayList<ScoredChromosome>();
		bestChrom = new ScoredChromosome();
		worstChrom = new ScoredChromosome();
	}

	// Reset all values to 0 or empty
	public void reset() {
		count = 0;
		bestChrom = new ScoredChromosome();
		worstChrom = new ScoredChromosome();
		threshold.clear();
		meanFitness = 0;
		standardDev = 0;
		lowerBound = 0;
	}

	/* When adding a new chromosome alter threshold values if required, add to the mean total and
	 * check if it is the worst or best chromosome of the subpopulation so far */
	public void add(ScoredChromosome currChromosome, int migrationNumber) {
		addThreshold(currChromosome, migrationNumber);
		count++;
		meanFitness += currChromosome.getScore();
		if (currChromosome.getScore() < worstChrom.getScore()) {
			worstChrom = currChromosome;
		}
		if (currChromosome.getScore() > bestChrom.getScore()) {
			worstChrom = currChromosome;
		}
	}

	// Alter the migration threshold if required for the given chromosome
	private void addThreshold(ScoredChromosome currChromosome, int migrationNumber) {
		// If not enough chromosomes flagged for migration yet add this chromosome
		if (threshold.size() != migrationNumber) {
			threshold.add(currChromosome);
		} else if (migrationNumber > 0) {
			// Else check if the chromosome should be added to the migration set
			Collections.sort(threshold);
			ScoredChromosome removed = threshold.remove(migrationNumber - 1);
			if (removed.getScore() != currChromosome.getScore()) {
				threshold.add(currChromosome);
			}
			lowerBound = threshold.get(migrationNumber-1).getScore().floatValue();
		}
	}

	/* Calculate the mean - until this method is called the mean fitness holds the total of all
	 * fitness scores so far */
	public void calculate() {
		meanFitness /= count;
	}
}
