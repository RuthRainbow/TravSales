package org.bradheintz.travsales;

public abstract class ChromosomeScorer {

		public ChromosomeScorer(String problem) {}

	    protected abstract double score(String chromosomeString) throws InterruptedException;
}
