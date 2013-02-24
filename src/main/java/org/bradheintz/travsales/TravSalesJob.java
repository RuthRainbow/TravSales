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

    private static final float survivorProportion = 0.3f;
    private static final float mutationChance = 0.01f;
    private static final int migrationFrequency = 3;
    private static final int migrationNumber = 3;
    private static final Topology topology = Topology.RING;

    private static int numCities = 20;
    private static int populationSize = 10000;
    private static int selectionBinSize;
    private static int numSubPopulations;
    private static int maxGenerations = 500;
    private static int numHierarchyLevels = 2;
    private static String popPath = "travsales_populations";

    private static float lowerBound;
    private double bestScoreCurrGen;
    private int noImprovementCount;
    private ScoredChromosome overallBestChromosome;

    protected enum Topology {
    	HYPERCUBE, RING;
    }

    // Not sure if char needs to be saved
    private enum Option {
    	NUMCITIES('c'),
    	POPULATIONSIZE('s'),
    	MAXNUMGENERATIONS('g'),
    	NUMHIERARCHYLEVELS('h'),
    	MUTATIONCHANCE('m'),
    	FILEPATH('f');

    	protected final char shortcut;

    	Option(char shortcut) {
    		this.shortcut = shortcut;
    	}
    }

    public static void main(String[] args) throws Exception {
    	FileUtils.deleteDirectory(new File(popPath));
        ToolRunner.run(new TravSalesJob(), args);
        FileUtils.deleteDirectory(new File(popPath + "/tmp_" + maxGenerations));
    }

    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = new Configuration();

    	if (args != null) {
    		parseArgs(args);
    	}
    	/*if (args != null && args.length == 3) {
    		numCities = Integer.valueOf(args[0]);
    		populationSize = Integer.valueOf(args[1]);
    		generations = Integer.valueOf(args[2]);
    	} else {
    		System.out.println("Incorrect args, using default values. Usage: <num. cities> <population size> <max. num. generations>");
    	}*/

    	numSubPopulations = (int) Math.pow(10, numHierarchyLevels);
    	selectionBinSize = (int) populationSize/numSubPopulations;

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
        FileOutputFormat.setOutputPath(job, new Path(popPath + "/tmp_0_1"));

        if (!job.waitForCompletion(true)) {
            System.out.println("Failure scoring first generation");
            System.exit(1);
        }

        // Copy the tmp file across as the initial population should just be scored
        FileUtils.copyDirectory(new File(popPath + "/tmp_0_1"), new File(popPath + "/population_0_scored"));

        int generation = 0;
        while (noImprovementCount < 50 && generation < maxGenerations) {
            selectAndReproduce(generation, roadmap);
            ScoredChromosome bestChromosome = findMigrationBounds(generation);
            printBestIndividual(generation, bestChromosome);
            if (bestChromosome.score > bestScoreCurrGen) {
            	bestScoreCurrGen = bestChromosome.score;
            	noImprovementCount = 0;
            	overallBestChromosome = bestChromosome;
            } else {
            	noImprovementCount++;
            }
            generation++;
        }
        System.out.println("BEST INDIVIDUAL WAS " + overallBestChromosome);
		return 0;
    }

    private void parseArgs(String[] args) {
    	Option chosenOption = null;
    	int num = -1;
    	String str = null;
    	for (int i = 0; i < args.length; i+=2) {
    		if (!args[0].startsWith("-")) {
    			System.out.println("Incorrect formatting - " +
    					"all parameters must begin with a '-' character");
    			System.exit(1);
    		}

    		String optionsString = args[0].substring(1);
    		try {
    			chosenOption = Option.valueOf(optionsString);
    		} catch (IllegalArgumentException e) {
    			chosenOption = tryShortcut(optionsString);
    			if (chosenOption == null) {
    				System.out.println("Incorrect formatting - not a valid parameter");
    				System.exit(1);
    			}
    		}

    		if (args.length <= i+1) {
    			System.out.println("Incorrect formatting - all parameters must have a value");
    			System.exit(1);
    		} else if (args[i+1].matches("[0-9]+")) {
    			num = Integer.valueOf(args[i+1]);
			} else {
				str = args[i+1];
			}

    		switch (chosenOption) {
    			case NUMCITIES : numCities = checkValid(num, chosenOption); break;
    			case POPULATIONSIZE : populationSize = checkValid(num, chosenOption); break;
    			case MAXNUMGENERATIONS : maxGenerations = checkValid(num, chosenOption); break;
    			case NUMHIERARCHYLEVELS : numHierarchyLevels = checkValid(num, chosenOption); break;
    			case FILEPATH : popPath = checkValid(str); break;
    			default: break;
    		}
    	}
	}

	private int checkValid(int num, Option parameter) {
		//TODO popsize must also be a multiple of 10, cannot have too many hierarchies etc'
		//TODO make this error statement better
		if (num < 0) {
			System.out.println("Incorrect formatting - parameter " + parameter + " must be a positive integer");
			System.exit(1);
		}
		return num;
	}

	private String checkValid(String str) {
		if (str == null) {
			System.out.println("Incorrect formatting - the file path must be a string");
			System.exit(1);
		}
		return str;
	}

	// Return the option the corresponds to the shortcut, or return null
	private Option tryShortcut(String parameter) {
		switch (parameter) {
			case "c" : return Option.NUMCITIES;
			case "s" : return Option.POPULATIONSIZE;
			case "g" : return Option.MAXNUMGENERATIONS;
			case "h" : return Option.NUMHIERARCHYLEVELS;
			case "f" : return Option.FILEPATH;
			default : return null;
		}
	}

	protected static void selectAndReproduce(int generation, String roadmap) throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat("survivorProportion", survivorProportion);
        conf.setInt("selectionBinSize", selectionBinSize);
        conf.setInt("numSubPopulations", numSubPopulations);
        conf.setFloat("mutationChance", mutationChance);
        conf.set("cities", roadmap);
        conf.setFloat("lowerBound", lowerBound);
        conf.setInt("migrationFrequency", migrationFrequency);
        conf.setInt("generation", generation);
        conf.setEnum("topology", topology);
        conf.setInt("populationSize", populationSize);

        Job job = new Job(conf, String.format("travsales_select_and_reproduce_%d", generation));

        job.setInputFormatClass(KeyValueFormat.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(TravSalesJob.class);
        job.setMapperClass(SelectionBinMapper.class);
        job.setReducerClass(TopLevelReducer.class);

        FileInputFormat.setInputPaths(job, new Path(popPath + String.format("/population_%d_scored", generation)));
        if (numHierarchyLevels == 1) {
        	FileOutputFormat.setOutputPath(job, new Path(popPath + String.format("/population_%d_scored", generation + 1)));
        } else {
        	FileOutputFormat.setOutputPath(job, new Path(popPath + String.format("/tmp_%d_1", generation)));
        }

        // Work with the innermost
        System.out.println(String.format("Hierarchy level 1: Selecting from population %d, breeding and scoring population %d", generation, generation + 1));
        if (!job.waitForCompletion(true)) {
            System.out.println(String.format("FAILURE selecting & reproducing generation %d", generation));
            System.exit(1);
        }

        // Args for new job: <generation #> <# cities> <population size> <# subpopulations> <hierarchy level> <final hierarchy level?>
        String[] args = new String[6];
        for (int i = 1; i < numHierarchyLevels; i++) {
        	args[0] = String.valueOf(generation);
        	args[1] = String.valueOf(numCities);
        	args[2] = String.valueOf(populationSize);
        	args[3] = String.valueOf((int) Math.pow(10, numHierarchyLevels-i));
        	// hierarchy indexes start from 0
        	args[4] = String.valueOf(i+1);
        	args[5] = (i + 1 == numHierarchyLevels) ? String.valueOf(true) : String.valueOf(false);
        	ToolRunner.run(new HierarchicalJob(), args);
        }

        FileUtils.deleteDirectory(new File(popPath + String.format("/tmp_%d", generation)));
    }

    private void printBestIndividual(int generation, ScoredChromosome bestChromosome) throws IOException {
        System.out.println("BEST INDIVIDUAL OF GENERATION " + generation + " IS " + bestChromosome);
    }

    private ScoredChromosome findMigrationBounds(int generation) throws IOException {
    	String inputPath = popPath + String.format("/population_%d_scored/part-r-00000", generation);
    	BufferedReader br = new BufferedReader(new FileReader(inputPath));
    	lowerBound = 0;
    	ArrayList<ScoredChromosome> bestChromosomes = new ArrayList<ScoredChromosome>();

        try {
            String line = br.readLine();

            while (line != null) {
            	String[] fields = line.split("\t");
            	double fitness = Double.valueOf(fields[1]);
            	if (bestChromosomes.size() < migrationNumber) {
            		bestChromosomes.add(new ScoredChromosome(line));
            		if (fitness > lowerBound) {
            			lowerBound = (float) fitness;
            		}
            	} else if (fitness > lowerBound) {
            		bestChromosomes.add(new ScoredChromosome(line));
            		Collections.sort(bestChromosomes);
            		bestChromosomes.remove(migrationNumber);
            		lowerBound = bestChromosomes.get(migrationNumber-1).score.floatValue();
            	}
                line = br.readLine();
            }
        } finally {
            br.close();
        }

        return bestChromosomes.get(0);
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
