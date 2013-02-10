package org.bradheintz.travsales;

import java.util.Arrays;

import org.apache.hadoop.io.Text;

public class ScoredChromosome implements Comparable<ScoredChromosome> {
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
		createNewChromosome(fields);
	}

	ScoredChromosome(String inputString) {
		String[] fields = inputString.split("\t");
		createNewChromosome(fields);
	}

	private void createNewChromosome(String[] fields) {
		chromosome = fields[0];
		try {
			score = Double.parseDouble(fields[1]);
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("fields became " + Arrays.toString(fields));
			e.printStackTrace();
			System.exit(1);
		}
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

	@Override
	public String toString() {
		return this.chromosome + " Score: " + this.score;
	}

	@Override
	public int compareTo(ScoredChromosome sc) {
		if (this.score == sc.score) {
			return 0;
		} else if (this.score > sc.score) {
			return -1;
		} else {
			return 1;
		}
	}
}
