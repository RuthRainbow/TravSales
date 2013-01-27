package org.bradheintz.travsales;

import org.apache.hadoop.io.Text;

public class ScoredChromosome {
	String chromosome;
	String[] chromosomeArray = null;
	Double score;
	double accumulatedNormalizedScore = -1.0;

	ScoredChromosome() {
		chromosome = "";
		score = -1.0;
	}

	ScoredChromosome(Text testText) {
		String[] fields = testText.toString().split("\t");
		chromosome = fields[0];
		score = Double.parseDouble(fields[1]);
	}

	String[] getChromosomeArray() {
		if (chromosomeArray == null) {
			chromosomeArray = chromosome.split(" ");
		}
		return chromosomeArray;
	}

	void setGene(int geneToSet, int newValue) {
		// TODO boundary checks
		getChromosomeArray()[geneToSet] = Integer.toString(newValue);
		StringBuilder newChromosome = new StringBuilder();
		for (int j = 0; j < getChromosomeArray().length; ++j) {
			newChromosome.append(getChromosomeArray()[j]);
			newChromosome.append(" ");
		}
		chromosome = newChromosome.toString().trim();
	}
}
