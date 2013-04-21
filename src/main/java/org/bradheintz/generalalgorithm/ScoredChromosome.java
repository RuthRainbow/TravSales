package org.bradheintz.generalalgorithm;

import java.util.Arrays;

import org.apache.hadoop.io.Text;

/**
 * Class to hold chromosome-score pairs and handle mutation.
 *
 * @author bradheintz, ruthking
 */
public class ScoredChromosome implements Comparable<ScoredChromosome> {
	private String chromosome;
	String[] chromosomeArray = null;
	private Double score;
	private double accumulatedNormalizedScore = -1.0;

	// If no parameters given initialise the chromosome to empty and the score to -1
	public ScoredChromosome() {
		setChromosome("");
		setScore(-1.0);
	}

	// If text is given split by tab characters and create a new chromosome
	public ScoredChromosome(Text testText) {
		String[] fields = testText.toString().split("\t");
		createNewChromosome(fields);
	}

	// If a string is given treat as text - split by tab and create a new chromosome
	ScoredChromosome(String inputString) {
		String[] fields = inputString.split("\t");
		createNewChromosome(fields);
	}

	// Set the chromosome as the first item and the score as the second, if there is a score set
	private void createNewChromosome(String[] fields) {
		setChromosome(fields[0]);
		try {
			setScore(Double.parseDouble(fields[1]));
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("fields became " + Arrays.toString(fields));
			e.printStackTrace();
			System.exit(1);
		}
	}

	// Method to return the chromosome string as an array of genes
	public String[] getChromosomeArray() {
		if (chromosomeArray == null) {
			chromosomeArray = getChromosome().split(" ");
		}
		return chromosomeArray;
	}

	// Set a specific gene to the new value given
	public void setGene(int geneToSet, int newValue) {
		getChromosomeArray()[geneToSet] = Integer.toString(newValue);
		StringBuilder newChromosome = new StringBuilder();
		for (int j = 0; j < getChromosomeArray().length; ++j) {
			newChromosome.append(getChromosomeArray()[j]);
			newChromosome.append(" ");
		}
		setChromosome(newChromosome.toString().trim());
	}

	@Override
	public String toString() {
		return this.getChromosome() + " Score: " + this.getScore();
	}

	@Override
	public int compareTo(ScoredChromosome sc) {
		if (this.getScore() == sc.getScore()) {
			return 0;
		} else if (this.getScore() > sc.getScore()) {
			return -1;
		} else {
			return 1;
		}
	}

	public String getChromosome() {
		return chromosome;
	}

	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public double getAccumulatedNormalizedScore() {
		return accumulatedNormalizedScore;
	}

	public void setAccumulatedNormalizedScore(double accumulatedNormalizedScore) {
		this.accumulatedNormalizedScore = accumulatedNormalizedScore;
	}
}
