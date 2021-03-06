/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.util.ArrayList;

import org.bradheintz.generalalgorithm.ChromosomeScorer;

/**
 * The class that assigns chromosomes a fitness value.
 *
 * @author bradheintz
 */
public class TravSalesScorer extends ChromosomeScorer {

    public ArrayList<double[]> cities;
    protected ArrayList<Boolean> citiesUsed;
    final static double SQRT2 = Math.sqrt(2.0);
    protected double maxDistance;

    public TravSalesScorer(String cityString) throws InterruptedException {
    	super(cityString);
        cities = getCitiesFromString(cityString);
        citiesUsed = new ArrayList<Boolean>(cities.size());
        maxDistance = SQRT2 * (cities.size() - 1);
        for (int i = 0; i < cities.size(); ++i) {
            citiesUsed.add(Boolean.FALSE);
        }
		if (cities.size() < 3) {
			throw new InterruptedException("Failure! Invalid city map.");
		}
    }

    protected double score(String chromosomeString) throws InterruptedException {
        ArrayList<Integer> route = getRouteFromChromosome(chromosomeString);
        double distance = 0.0;
        for (int i = 1; i < cities.size(); ++i) {
            int city1Index = route.get(i);
            int city2Index = route.get(i - 1);
            double[] city1 = cities.get(city1Index);
            double[] city2 = cities.get(city2Index);
            double city1x = city1[0]; double city1y = city1[1];
            double city2x = city2[0]; double city2y = city2[1];

            distance += Math.sqrt(((city1x - city2x) * (city1x - city2x)) + ((city1y - city2y) * (city1y - city2y)));
        }

        return maxDistance - distance;
    }

    protected static ArrayList<double[]> getCitiesFromString(String cityString) {
        String[] coordPairs = cityString.split(";");
        ArrayList<double[]> outList = new ArrayList<double[]>(coordPairs.length);

        for (String coordPair : coordPairs) {
            String coordStrings[] = coordPair.split(",");
            double[] coords = {Double.parseDouble(coordStrings[0]), Double.parseDouble(coordStrings[1])};
            // LATER die gracefully if !2coords
            outList.add(coords);
        }

        return outList;
    }

    protected ArrayList<Integer> getRouteFromChromosome(String chromosomeString) throws InterruptedException {
        String[] geneStrings = chromosomeString.split(" ");
        if (geneStrings.length != cities.size() - 1) {
        	throw new InterruptedException("Failure! Incorrect gene size " + chromosomeString);
        }

        ArrayList<Integer> genes = new ArrayList<Integer>(geneStrings.length + 1);
        for (String geneString : geneStrings) {
            genes.add(Integer.decode(geneString)); // LATER lots more logging and checking here
        }
        genes.add(0);

        for (int i = 0; i < citiesUsed.size(); ++i) {
            citiesUsed.set(i, Boolean.FALSE);
        }

        ArrayList<Integer> route = new ArrayList<Integer>(genes.size());
        for (Integer gene : genes) {
            int cityCounter = 0, i = 0;
            while ((cityCounter < gene) || (citiesUsed.get(i))) {
                if (!citiesUsed.get(i)) {
                    ++cityCounter;
                }
                ++i;
            }

            citiesUsed.set(i, Boolean.TRUE);
            route.add(i);
        }

        // LATER check no dupes

        return route;
    }
}
