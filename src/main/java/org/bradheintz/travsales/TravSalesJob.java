/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bradheintz.generalalgorithm.InitialJob;
import org.bradheintz.generalalgorithm.ScoringMapper;
import org.bradheintz.generalalgorithm.SelectionReproductionReducer;

/**
 * The main job - forms the innermost hierarchy level and calls all other levels.
 *
 * @author bradheintz, ruthking
 */
public class TravSalesJob extends InitialJob implements Tool {
	// Random used in problem generation and to create initial random population
    private static Random random = new Random();

    // Extra variable, specific to TSP, holding the number of cities
    private static final int numCities = 20;

    // Customise the path to where the files are stored
    private static String popPath = "travsales_populations";

    // Method to run this job, cleaning up the directory before and the intial temporary file after
    public static void main(String[] args) throws Exception {
    	FileUtils.deleteDirectory(new File(popPath));
        ToolRunner.run(new TravSalesJob(), args);
        FileUtils.deleteDirectory(new File(popPath + "/tmp_0_0"));
    }

    // For TravSales the problem is the city map
    @Override
    protected Configuration setInitialConfigValues(Configuration conf) {
    	conf.set("cities", problem);
    	return super.setInitialConfigValues(conf);
    }

    // Override this to return the customised file path
    @Override
    protected String setPopPath() {
    	return popPath;
    }

    /* For TravSales the problem is the city map and the number of cities is also needed by the
     * reducer to randomly generate chromosomes */
    @Override
    protected Configuration setIterativeConfigValues(Configuration conf, int generation) {
    	conf.set("cities", problem);
    	conf.setInt("numCities", numCities);
    	return super.setIterativeConfigValues(conf, generation);
    }

    // Create the coordinate map of cities
    protected static ArrayList<double[]> createMap(int numCities) {
        ArrayList<double[]> roadmap = new ArrayList<double[]>(numCities);
        for (int i = 0; i < numCities; ++i) {
            double[] coords = {random.nextDouble(), random.nextDouble()};
            roadmap.add(coords);
        }
        return roadmap;
    }

    // Add cities to the map of cities and write this to the HDFS
    @Override
    protected String setUpInitialProblem(FSDataOutputStream hdfsOut, Configuration hadoopConfig)
    		throws IOException {
        ArrayList<double[]> roadmap = new ArrayList<double[]>(20);
        for (int i = 0; i < 5; ++i) {
            double dummy = 0.2 * (double)i;
            roadmap.add(new double[] {0.0, dummy});
            roadmap.add(new double[] {dummy, 1.0});
            roadmap.add(new double[] {dummy + 0.2, 0.0});
            roadmap.add(new double[] {1.0, dummy + 0.2});
        }

        StringBuilder configStringBuilder = new StringBuilder("");
        for (int i = 0; i < roadmap.size(); ++i) {
            double[] coords = roadmap.get(i);
            hdfsOut.writeBytes(String.format("%d %g %g\n", i, coords[0], coords[1]));
            if (configStringBuilder.length() > 0) {
                configStringBuilder.append(";");
            }
            configStringBuilder.append(String.format("%g,%g", coords[0], coords[1]));
        }
        hdfsOut.close();
        hdfsOut = null;

        return configStringBuilder.toString();
    }

    // Create the roadmap and write this to HDFS
    protected static String createRoadmap(FSDataOutputStream hdfsOut, Configuration hadoopConfig,
    		final int numCities) throws IOException {
        ArrayList<double[]> roadmap = createMap(numCities);
        StringBuilder configStringBuilder = new StringBuilder("");
        for (int i = 0; i < roadmap.size(); ++i) {
            double[] coords = roadmap.get(i);
            hdfsOut.writeBytes(String.format("%d %g %g\n", i, coords[0], coords[1]));

            if (configStringBuilder.length() > 0) {
                configStringBuilder.append(";");
            }
            configStringBuilder.append(String.format("%g,%g", coords[0], coords[1]));
        }
        hdfsOut.close();
        hdfsOut = null;

        return configStringBuilder.toString();
    }

    // Generate the initial population and write this to file
    @Override
    protected void createInitialPopulation(FSDataOutputStream populationOutfile,
    		final int populationSize) throws IOException {
    	for (int i = 0; i < populationSize; ++i) {
            for (int j = 0; j < (numCities - 1); ++j) {
                if (j > 0) {
                    populationOutfile.writeBytes(" ");
                }
                populationOutfile.writeBytes(String.format("%d", random.nextInt(numCities - j)));
            }
            populationOutfile.writeBytes("\n");
        }

        populationOutfile.close();
    }

    // Add the number of cities to the default configuration
    @Override
    protected Configuration createHierarchicalConfig(int generation, int level) {
    	Configuration conf = super.createHierarchicalConfig(generation, level);
    	conf.setInt("numCities", numCities);
		return conf;
    }

    // Set the specific job, reducer and scoring mapper classes for TravSales
	@Override
	protected Class<? extends InitialJob> setJarByClass() {
		return TravSalesJob.class;
	}

	@Override
	protected Class<? extends SelectionReproductionReducer> setReducerByClass() {
		return TravSalesReducer.class;
	}

	@Override
	protected Class<? extends ScoringMapper> setInitialMapperClass() {
		return TravSalesScoringMapper.class;
	}
}
