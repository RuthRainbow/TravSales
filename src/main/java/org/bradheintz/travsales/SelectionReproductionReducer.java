/*
  CURRENT ERROR : 
java.lang.OutOfMemoryError: GC overhead limit exceeded
	at java.util.Formatter$FormatSpecifier.mantissa(Formatter.java:3360)
	at java.util.Formatter$FormatSpecifier.print(Formatter.java:3295)
	at java.util.Formatter$FormatSpecifier.print(Formatter.java:3190)
	at java.util.Formatter$FormatSpecifier.printFloat(Formatter.java:2757)
	at java.util.Formatter$FormatSpecifier.print(Formatter.java:2708)
	at java.util.Formatter.format(Formatter.java:2488)
	at java.util.Formatter.format(Formatter.java:2423)
	at java.lang.String.format(String.java:2797)
	at org.bradheintz.travsales.SelectionReproductionReducer.makeOffspring(SelectionReproductionReducer.java:265)
	at org.bradheintz.travsales.SelectionReproductionReducer.generateSurvivors(SelectionReproductionReducer.java:140)
	at org.bradheintz.travsales.SelectionReproductionReducer.reduce(SelectionReproductionReducer.java:82)
	at org.bradheintz.travsales.SelectionReproductionReducer.reduce(SelectionReproductionReducer.java:28)

* 
* 
* To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
 *
 * @author bradheintz
 */
public class SelectionReproductionReducer extends Reducer<VIntWritable, Text, Text, DoubleWritable> {

	private final static Logger log = Logger.getLogger(SelectionReproductionReducer.class);
	private double survivorProportion;
	private double topTierProportion;
	private int numSubsets = 5;
	private int desiredPopulationSize;
	protected double sideEffectSum = 0.0;
	protected double mutationChance = 0.01;
	protected Random random = new Random();
	protected ChromosomeScorer scorer;
	private Text outKey = new Text();
	private DoubleWritable outValue = new DoubleWritable();

	@Override
	protected void reduce(VIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Want to embed this to split again then reduce again
		//System.out.println("IN THE REDUCE");
		generateSurvivors(values, context);
	}

	private void generateSurvivors(Iterable<Text> values, Context context) throws InterruptedException, IOException {
		// Want to do inner reduce before the values are sorted. Need to split iterable into parts
		//- have to iterate over. Don't know the number of values :(
		List<ScoredChromosome> reducedVals = new ArrayList<ScoredChromosome>();
		List<ScoredChromosome> cache = new ArrayList<ScoredChromosome>();
		
		int numValues = 0;
		for (Text text : values) {
			numValues++;
			ScoredChromosome sc = new ScoredChromosome(text);
			cache.add(sc);
		}

		int valuesPerSubset = (int) Math.floor(numValues/numSubsets);
		int nextSubset = valuesPerSubset;
		List<ScoredChromosome> currSubset = new ArrayList<ScoredChromosome>();
		//System.out.println(valuesPerSubset + " per subset");

		for (ScoredChromosome sc : cache) {
			currSubset.add(sc);
			if (currSubset.size() > nextSubset) {
				//System.out.println(currSubset.size() + " > " + nextSubset);
				int tmp = nextSubset + valuesPerSubset;
				if (numValues - tmp >= valuesPerSubset) {
					//System.out.println("size of reduced vals was " + reducedVals.size());
					//reducedVals.addAll(innerReduce(currSubset));
					//System.out.println("size of reduced vals is now " + reducedVals.size());
					currSubset.clear();
					nextSubset += valuesPerSubset;
				} else {
					nextSubset += numValues % numSubsets;
				}
			}
		}
		
		//System.out.println("REDUCED VALS SIZE IS " + reducedVals.size());

		TreeSet<ScoredChromosome> sortedChromosomes = getSortedChromosomeSet(reducedVals);
		normalizeScores(sortedChromosomes);

		//System.out.println("sorted chroms size is " + sortedChromosomes.size() + " suvivor prop is " + survivorProportion);
		int survivorsWanted = (int) ((double) sortedChromosomes.size() * survivorProportion);
		Set<ScoredChromosome> survivors = new HashSet<ScoredChromosome>(survivorsWanted);

		//        int topTier = (int)((double)sortedChromosomes.size() * topTierProportion);
		//        while (survivors.size() < topTier) {
		//            need a reverse iterator or something
		//        }

		//System.out.println(survivorsWanted + " SURVIVORS WANTED");
		while (survivors.size() < survivorsWanted) {
			survivors.add(selectSurvivor(sortedChromosomes));
		}

		ArrayList<ScoredChromosome> parentPool = new ArrayList<ScoredChromosome>(survivors);

		int survivorsSize = survivors.size();
		//System.out.println("Survivors size is " + survivorsSize + " desired pop size is " + desiredPopulationSize);
		while (survivorsSize < desiredPopulationSize) {
			survivors.add(makeOffspring(parentPool)); // OUT OF MEM ERROR HERE ON SOME POPS! :(
		}

		//System.out.println("Finished the inner, about to iterate over outer");
		Iterator<ScoredChromosome> iter = survivors.iterator();
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			outKey.set(sc.chromosome);
			outValue.set(sc.score);
			context.write(outKey, outValue);
		}
	}

	private List<ScoredChromosome> innerReduce(List<ScoredChromosome> currSubset) throws InterruptedException {
		//System.out.println("VALUES SIZE "+ currSubset.size());
		TreeSet<ScoredChromosome> sortedChromosomes = getSortedChromosomeSet(currSubset);
		//System.out.println("GOT SORTED CHROM SET SIZE " + sortedChromosomes.size());
		normalizeScores(sortedChromosomes);

		//System.out.println("SURVIVOR PROP " + survivorProportion);
		int survivorsWanted = (int) ((double) sortedChromosomes.size() * survivorProportion);
		//System.out.println("SURVIVORS WANTED " + survivorsWanted);
		Set<ScoredChromosome> survivors = new HashSet<ScoredChromosome>(survivorsWanted);

		//System.out.println(survivors.size() + " SURVIVORS AND " + survivorsWanted + " WANTED");
		while (survivors.size() < survivorsWanted) {
			survivors.add(selectSurvivor(sortedChromosomes));
		}

		ArrayList<ScoredChromosome> parentPool = new ArrayList<ScoredChromosome>(survivors);

		//System.out.println("SURVIVORS SIZE " + survivors.size() + " DESIREC SIZE " + desiredPopulationSize);
		int survivorsSize = survivors.size();
		while (survivorsSize < desiredPopulationSize*2) {
			survivors.add(makeOffspring(parentPool));
			survivorsSize++;
		}
		
		//System.out.println("IN THE INNER REDUCE> SURVIVORS + " + survivors.size());

		List<ScoredChromosome> toReturn = new ArrayList<ScoredChromosome>();
		Iterator<ScoredChromosome> iter = survivors.iterator();
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			toReturn.add(sc);
		}
		//System.out.println("SIZE OF RETURN IS " + toReturn.size());
		return toReturn;
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		survivorProportion = context.getConfiguration().getFloat("survivorProportion", 0.3f);
		topTierProportion = context.getConfiguration().getFloat("topTierToSave", 0.0f);
		//desiredPopulationSize = context.getConfiguration().getInt("selectionBinSize", 1000);
		desiredPopulationSize = 1000;
		mutationChance = context.getConfiguration().getFloat("mutationChance", 0.01f);
		if (config.get("cities") == null) {
			throw new InterruptedException("Failure! No city map.");
		}
		scorer = new ChromosomeScorer(config.get("cities"));
		if (scorer.cities.size() < 3) {
			throw new InterruptedException("Failure! Invalid city map.");
		}
	}
	
	  protected TreeSet<ScoredChromosome> getSortedChromosomeSet(Iterable<?> scoredChromosomeStrings) {
	        TreeSet<ScoredChromosome> sortedChromosomes = new TreeSet<ScoredChromosome>(new Comparator<ScoredChromosome>() {
	            @Override
	            public int compare(ScoredChromosome c1, ScoredChromosome c2) {
	                return c1.score.compareTo(c2.score);
	            }
	        });

	        sideEffectSum = 0.0; // computing sum as a side effect saves us a pass over the set, even if it makes us feel dirty-in-a-bad-way

	        ScoredChromosome sc = null;;
	        for (Object currString : scoredChromosomeStrings) {
	        	if (currString.getClass().equals(Text.class)) {
	        		sc = new ScoredChromosome((Text) currString);
	        	} else if (currString.getClass().equals(ScoredChromosome.class)) {
	        		sc = (ScoredChromosome) currString;
	        	}
	        	
	            if (sortedChromosomes.add(sc)) {
	            	sideEffectSum += sc.score;
	            } else {
	            	//System.out.println("ERRORRRRRRRRRRRRRR");
	            }
	            log.debug(String.format("SORTING: chromosome: %s, score: %g, accnormscore: %g, SUM: %g", sc.chromosome, sc.score, sc.accumulatedNormalizedScore, sideEffectSum));
	        }

	        return sortedChromosomes;
	    }
	  

	protected void normalizeScores(Iterable<ScoredChromosome> scoredChromosomes) {
		Iterator<ScoredChromosome> iter = scoredChromosomes.iterator();
		double accumulatedScore = 0.0;
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			accumulatedScore += sc.score / sideEffectSum;
			sc.accumulatedNormalizedScore = accumulatedScore;
			log.debug(String.format("NORMALIZING: chromosome: %s, score: %g, accnormscore: %g", sc.chromosome, sc.score, sc.accumulatedNormalizedScore));
		}
	}

	protected ScoredChromosome selectSurvivor(Iterable<ScoredChromosome> scoredAndNormalizedChromosomes) {
		double thresholdScore = random.nextDouble();
		Iterator<ScoredChromosome> iter = scoredAndNormalizedChromosomes.iterator();
		while (iter.hasNext()) {
			ScoredChromosome sc = iter.next();
			if (sc.accumulatedNormalizedScore > thresholdScore) {
				log.debug(String.format("SELECTING: chromosome: %s, score: %g, accnormscore: %g, threshold: %g", sc.chromosome, sc.score, sc.accumulatedNormalizedScore, thresholdScore));
				return sc;
			}
		}

		return null; // LATER this is a horrible error condition, and I should do something about it
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
			log.debug(String.format("PARENT 1: chromosome: %s, score: %g, accnormscore: %g", parent1.chromosome, parent1.score, parent1.accumulatedNormalizedScore));
			log.debug(String.format("PARENT 2: chromosome: %s, score: %g, accnormscore: %g", parent2.chromosome, parent2.score, parent2.accumulatedNormalizedScore));

			ScoredChromosome offspring = crossover(parent1, parent2);
			if (random.nextDouble() < mutationChance) {
				mutate(offspring);
			}

			offspring.score = scorer.score(offspring.chromosome);
			return offspring;
		} catch (NullPointerException npe) {
			log.error("*** NullPointerException in makeOffspring()");
			log.error(String.format("parent 1 index: %d   parent 2 index: %d   pool size: %d", parent1Index, parent2Index, parentPool.size()));
			if (parentPool.get(parent1Index) == null) log.error("parent 1 null!");
			if (parentPool.get(parent2Index) == null) log.error("parent 2 null!");
			throw new InterruptedException("null pointer exception in makeOffspring()");
		}
	}

	protected ScoredChromosome crossover(ScoredChromosome parent1, ScoredChromosome parent2) {
		int crossoverPoint = random.nextInt(parent1.getChromosomeArray().length - 1) + 1;
		ScoredChromosome offspring = new ScoredChromosome();

		StringBuilder newChromosome = new StringBuilder();
		for (int i = 0; i < crossoverPoint; ++i) {
			newChromosome.append(parent1.getChromosomeArray()[i]);
			newChromosome.append(" ");
		}
		for (int j = crossoverPoint; j < parent2.getChromosomeArray().length; ++j) {
			newChromosome.append(parent2.getChromosomeArray()[j]);
			newChromosome.append(" ");
		}
		offspring.chromosome = newChromosome.toString().trim();

		return offspring;
	}

	protected void mutate(ScoredChromosome offspring) {
		int geneToMutate = random.nextInt(offspring.getChromosomeArray().length);
		offspring.setGene(geneToMutate, random.nextInt(offspring.getChromosomeArray().length + 1 - geneToMutate));
	}
}
