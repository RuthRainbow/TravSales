/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author bradheintz
 */
public class TravSalesJob extends Configured implements Tool {

    private static Random random = new Random();

    // LATER these should all be configurable
    private static final String popPath = "travsales_populations";
    private static final int numCities = 20;
    private static final int populationSize = 10000;
    private static final int selectionBinSize = 1000;
    private static final float survivorProportion = 0.3f;
    private static final float mutationChance = 0.01f;
    // LATER have pluggable strategies, but for now, just pick a number of generations
    private static final int generations = 500;
    private static final int numSubPopulations = 100;

    private ArrayList<ScoredChromosome> bestChromosomes = new ArrayList<ScoredChromosome>();
    private static float lowerBound;
    private double bestScoreYet;
    private int noImprovementCount;

    public static void main(String[] args) throws Exception {
    	FileUtils.deleteDirectory(new File(popPath));
        ToolRunner.run(new TravSalesJob(), args);
        FileUtils.deleteDirectory(new File(popPath + "/tmp_" + generations));
    }

    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        String roadmap = createTrivialRoadmap(fs.create(new Path("_CITY_MAP")), conf, numCities);
        conf.set("cities", roadmap);
        System.out.println("city map created...");

        createInitialPopulation(fs.create(new Path(popPath + "/population_0/population_0_init")), populationSize, numCities);
        System.out.println("initial population created...");

        Job job = new Job(conf, "travsales");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setJarByClass(TravSalesJob.class);
        job.setMapperClass(ScoringMapper.class);

        FileInputFormat.setInputPaths(job, new Path(popPath + "/population_0"));
        FileOutputFormat.setOutputPath(job, new Path(popPath + "/tmp_0"));

        if (!job.waitForCompletion(true)) {
            System.out.println("Failure scoring first generation");
            System.exit(1);
        }

        // Copy the tmp file across as the initial population should just be scored
        FileUtils.copyDirectory(new File(popPath + "/tmp_0"), new File(popPath + "/population_0_scored"));

        int generation = 0;
        while (noImprovementCount < 20 && generation < generations) {
            selectAndReproduce(generation, roadmap);
            findTopOnePercent(generation);
            ScoredChromosome bestChromosome = bestChromosomes.get(0);
            printBestIndividual(generation, bestChromosome);
            if (bestChromosome.score > bestScoreYet) {
            	bestScoreYet = bestChromosome.score;
            	noImprovementCount = 0;
            } else {
            	noImprovementCount++;
            }
            generation++;
        }
        //findTopOnePercent(generation);
        //printBestIndividual(generation, bestChromosomes.get(0));
		return 0;
    }

    protected static void selectAndReproduce(int generation, String roadmap) throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat("survivorProportion", survivorProportion);
        conf.setInt("selectionBinSize", selectionBinSize);
        conf.setInt("numSubPopulations", numSubPopulations);
        conf.setFloat("mutationChance", mutationChance);
        conf.set("cities", roadmap);
        conf.setFloat("lowerBound", lowerBound);

        Job job = new Job(conf, String.format("travsales_select_and_reproduce_%d", generation));

        job.setInputFormatClass(KeyValueFormat.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(TravSalesJob.class);
        job.setMapperClass(SelectionBinMapper.class);
        job.setReducerClass(SelectionReproductionReducer.class);

        FileInputFormat.setInputPaths(job, new Path(popPath + String.format("/population_%d_scored", generation)));
        FileOutputFormat.setOutputPath(job, new Path(popPath + String.format("/tmp_%d", generation + 1)));

        System.out.println(String.format("Selecting from population %d, breeding and scoring population %d", generation, generation + 1));
        if (!job.waitForCompletion(true)) {
            System.out.println(String.format("FAILURE selecting & reproducing generation %d", generation));
            System.exit(1);
        }

        String[] args = new String[1];
        args[0] = String.valueOf(generation);
        ToolRunner.run(new InnerJob(), args);
        FileUtils.deleteDirectory(new File(popPath + String.format("/tmp_%d", generation)));
    }

    private static int numSelectionBins() {
        return populationSize / selectionBinSize;
    }

    private void printBestIndividual(int generation, ScoredChromosome bestChromosome) throws IOException {
        System.out.println("BEST INDIVIDUAL OF GENERATION " + generation + " IS " + bestChromosome);
    }

    private ArrayList<ScoredChromosome> findTopOnePercent(int generation) throws IOException {
    	String inputPath = popPath + String.format("/population_%d_scored/part-r-00000", generation);
    	BufferedReader br = new BufferedReader(new FileReader(inputPath));
    	lowerBound = 0;
    	bestChromosomes.clear();
    	final int topPercent = (int) Math.floor(populationSize/100);

        try {
            String line = br.readLine();

            while (line != null) {
            	String[] fields = line.split("\t");
            	double fitness = Double.valueOf(fields[1]);
            	if (bestChromosomes.size() < topPercent) {
            		bestChromosomes.add(new ScoredChromosome(line));
            		if (fitness > lowerBound) {
            			lowerBound = (float) fitness;
            		}
            	} else if (fitness > lowerBound) {
            		bestChromosomes.add(new ScoredChromosome(line));
            		Collections.sort(bestChromosomes);
            		bestChromosomes.remove(topPercent);
            		lowerBound = bestChromosomes.get(topPercent-1).score.floatValue();
            	}
                line = br.readLine();
            }
        } finally {
            br.close();
        }

        return bestChromosomes;
    }

    protected static ArrayList<double[]> createMap(int numCities) {
        ArrayList<double[]> roadmap = new ArrayList<double[]>(numCities);
        for (int i = 0; i < numCities; ++i) {
            double[] coords = {random.nextDouble(), random.nextDouble()};
            roadmap.add(coords);
        }
        return roadmap;
    }

    protected static String createTrivialRoadmap(FSDataOutputStream hdfsOut, Configuration hadoopConfig,
    		final int numCitiesIgnored) throws IOException {
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

    protected static void createInitialPopulation(FSDataOutputStream populationOutfile,
    		final int populationSize, final int numCities) throws IOException {
        // Could create good initial solution using what the paper was chatting about graphs least tree or something
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
}
