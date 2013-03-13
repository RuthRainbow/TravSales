package org.bradheintz.travsales;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SubpopulationStats {

	protected ScoredChromosome bestChrom;
	protected ScoredChromosome worstChrom;
	protected float lowerBound;
	protected double meanFitness;
	protected double standardDev;
	protected List<ScoredChromosome> threshold;
	private int count;

	public SubpopulationStats() {
		threshold = new ArrayList<ScoredChromosome>();
		bestChrom = new ScoredChromosome();
		worstChrom = new ScoredChromosome();
	}

	public void reset() {
		count = 0;
		bestChrom = new ScoredChromosome();
		worstChrom = new ScoredChromosome();
		threshold.clear();
		meanFitness = 0;
		standardDev = 0;
		lowerBound = 0;
	}

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

	private void addThreshold(ScoredChromosome currChromosome, int migrationNumber) {
		if (threshold.size() != migrationNumber) {
			threshold.add(currChromosome);
		} else if (migrationNumber > 0) {
			Collections.sort(threshold);
			ScoredChromosome removed = threshold.remove(migrationNumber - 1);
			if (removed.getScore() != currChromosome.getScore()) {
				threshold.add(currChromosome);
			}
			lowerBound = threshold.get(migrationNumber-1).getScore().floatValue();
		}
	}

	public void calculate() {
		meanFitness /= count;
	}
}
