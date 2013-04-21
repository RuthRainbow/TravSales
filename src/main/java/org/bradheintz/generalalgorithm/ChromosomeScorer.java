package org.bradheintz.generalalgorithm;

/**
 * Abstract class to score individual chromosomes given a problem
 *
 * @author ruthking
 */
public abstract class ChromosomeScorer {

		public ChromosomeScorer(String problem) {}

	    protected abstract double score(String chromosomeString) throws InterruptedException;
}
