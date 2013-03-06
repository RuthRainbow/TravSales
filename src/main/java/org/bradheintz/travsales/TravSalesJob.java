/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * The main job - forms the innermost hierarchy level and calls all other levels.
 *
 * @author bradheintz
 */
public class TravSalesJob extends Configured implements Tool {

    private static Random random = new Random();

    private static final float survivorProportion = 0.3f;
    private static final Topology topology = Topology.RING;
    private static final int numCities = 20;

    private static int populationSize = 10000;
    private static int selectionBinSize;
    private static int numSubPopulations;
    private static int maxGenerations = 500;
    private static int numHierarchyLevels = 2;
    private static float mutationChance = 0.01f;
    private static int migrationFrequency = 3;
    private static float migrationPercentage = 0.0003f;
    private static int migrationNumber;
    private static String popPath = "travsales_populations";

    private static float[] lowerBounds;
    private double bestScoreCurrGen;
    private int noImprovementCount;
    private ScoredChromosome overallBestChromosome;

    protected enum Topology {
    	HYPERCUBE, RING;
    }

    private enum Option {
    	POPULATIONSIZE('s', "population size"),
    	MAXNUMGENERATIONS('g', "maximum number of generations"),
    	NUMHIERARCHYLEVELS('h', "number of hierarchy levels"),
    	MUTATIONCHANCE('m', "mutation chance"),
    	MIGRATIONRATE('r', "migration rate"),
    	MIGRATIONPERCENTAGE('p', "migration percentage"),
    	FILEPATH('f', "filepath");

    	protected final char shortcut;
    	private final String humanReadable;

    	Option(char shortcut, String str) {
    		this.shortcut = shortcut;
    		this.humanReadable = str;
    	}

    	@Override
    	public String toString() {
    		return humanReadable;
    	}
    }

    public static void main(String[] args) throws Exception {
    	FileUtils.deleteDirectory(new File(popPath));
        ToolRunner.run(new TravSalesJob(), args);
        FileUtils.deleteDirectory(new File(popPath + "/tmp_0_0"));
    }

    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = new Configuration();

    	if (args != null) {
    		parseArgs(args);
    		validateArgs();
    	}

    	numSubPopulations = (int) Math.pow(10, numHierarchyLevels);
    	selectionBinSize = (int) populationSize/numSubPopulations;
    	lowerBounds = new float[numSubPopulations];
    	migrationNumber = (int) Math.floor(populationSize * migrationPercentage);

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
        FileOutputFormat.setOutputPath(job, new Path(popPath + "/tmp_0_0"));

        if (!job.waitForCompletion(true)) {
            System.out.println("Failure scoring first generation");
            System.exit(1);
        }

        // Copy the tmp file across as the initial population should just be scored
        FileUtils.copyDirectory(new File(popPath + "/tmp_0_0"), new File(popPath + "/population_0_scored"));

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
        // MAY HAVE TO HAVE THIS ON HDFS?
        writeResultToFile();
		return 0;
    }

	protected static void selectAndReproduce(int generation, String roadmap) throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat("survivorProportion", survivorProportion);
        conf.setInt("selectionBinSize", selectionBinSize);
        conf.setInt("numSubPopulations", numSubPopulations);
        conf.setFloat("mutationChance", mutationChance);
        conf.set("cities", roadmap);
        conf.setInt("migrationFrequency", migrationFrequency);
        conf.setInt("generation", generation);
        conf.setEnum("topology", topology);
        conf.setInt("populationSize", populationSize);
        for (int i = 0; i < lowerBounds.length; i++) {
        	conf.setFloat("lowerBound" + i, lowerBounds[i]);
        }

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

        System.out.println(String.format("Hierarchy level 1: Selecting from population %d, " +
        		"breeding and scoring population %d", generation, generation + 1));
        if (!job.waitForCompletion(true)) {
            System.out.println(String.format("FAILURE selecting & reproducing generation %d", generation));
            System.exit(1);
        }

        runNextLevel(generation);

        for (int i = 1; i < numHierarchyLevels; i++) {
        	FileUtils.deleteDirectory(new File(popPath + String.format("/tmp_%d_%d", generation, i)));
        }
    }

    private static void runNextLevel(int generation) throws Exception {
        /* Args for new job: <generation #> <# cities> <population size> <# subpopulations>
    	   <hierarchy level> <final hierarchy level?> <migration frequency> <migration percentage>
    	   <mutation chance> */
        String[] args = new String[9];
        for (int i = 1; i < numHierarchyLevels; i++) {
        	args[0] = String.valueOf(generation);
        	args[1] = String.valueOf(numCities);
        	args[2] = String.valueOf(populationSize);
        	args[3] = String.valueOf((int) Math.pow(10, numHierarchyLevels-i));
        	// hierarchy indexes start from 0
        	args[4] = String.valueOf(i+1);
        	args[5] = (i + 1 == numHierarchyLevels) ? String.valueOf(true) : String.valueOf(false);
        	// TODO maybe this doesn't work well for many hierarchies - in parallel?
        	args[6] = String.valueOf(migrationFrequency * i);
        	args[7] = String.valueOf(migrationPercentage * i);
        	args[8] = String.valueOf(mutationChance);
        	ToolRunner.run(new HierarchicalJob(), args);
        }
	}

    private void parseArgs(String[] args) {
    	int num = -1;
    	float dec = -1;
    	String str = null;
    	for (int i = 0; i < args.length; i+=2) {
    		dec = -1;
    		num = -1;
    		str = null;
    		Option chosenOption = null;
    		if (!args[0].startsWith("-")) {
    			System.out.println("Incorrect formatting - " +
    					"all parameters must begin with a '-' character");
    			System.exit(1);
    		}

    		String optionsString = args[i].substring(1);
    		try {
    			chosenOption = Option.valueOf(optionsString.toUpperCase());
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
			} else if (args[i+1].matches("[0-9]*.[0-9]+")) {
				dec = Float.valueOf(args[i+1]);
			} else {
				str = args[i+1];
			}

    		switch (chosenOption) {
    			case POPULATIONSIZE : populationSize = checkValid(num, chosenOption); break;
    			case MAXNUMGENERATIONS : maxGenerations = checkValid(num, chosenOption); break;
    			case NUMHIERARCHYLEVELS : numHierarchyLevels = checkValid(num, chosenOption); break;
    			case MUTATIONCHANCE : mutationChance = checkValid(dec, chosenOption); break;
    			case MIGRATIONRATE: migrationFrequency = checkValid(num, chosenOption); break;
    			case MIGRATIONPERCENTAGE: migrationPercentage = checkValid(dec, chosenOption); break;
    			case FILEPATH : popPath = checkValid(str, chosenOption); break;
    			default: break;
    		}
    	}
	}

	private int checkValid(int num, Option parameter) {
		//TODO cannot have too many hierarchies etc'
		if (num < 0) {
			System.out.println("Incorrect formatting - parameter " + parameter + " must be a positive integer");
			System.exit(1);
		}
		return num;
	}

	private String checkValid(String str, Option parameter) {
		if (str == null) {
			System.out.println("Incorrect formatting - parameter " + parameter + " must be a string");
			System.exit(1);
		}
		return str;
	}

	private float checkValid(float num, Option parameter) {
		if (num > 1) {
			System.out.println("Incorrect formatting - parameter " + parameter + " must be a float between 0 and 1");
			System.exit(1);
		}
		return num;
	}

	private void validateArgs() {
		if (Math.pow(10, numHierarchyLevels+1) > populationSize) {
			System.out.println("Incorrect format - population size must be greater than 10 to the " +
					"power of the number of hierarchy levels, as the minimum size of sub-populations is 10");
			System.exit(1);
		}
	}

	// Return the option the corresponds to the shortcut, or return null
	private Option tryShortcut(String parameter) {
		switch (parameter) {
			case "s" : return Option.POPULATIONSIZE;
			case "g" : return Option.MAXNUMGENERATIONS;
			case "h" : return Option.NUMHIERARCHYLEVELS;
			case "m" : return Option.MUTATIONCHANCE;
			case "r" : return Option.MIGRATIONRATE;
			case "p" : return Option.MIGRATIONPERCENTAGE;
			case "f" : return Option.FILEPATH;
			default : return null;
		}
	}

	private void printBestIndividual(int generation, ScoredChromosome bestChromosome) throws IOException {
        System.out.println("BEST INDIVIDUAL OF GENERATION " + generation + " IS " + bestChromosome);
    }

    private void writeResultToFile() {
    	try {
			String content = overallBestChromosome.toString();
			File file = new File(popPath + "/result");

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(content);
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    // Finds the lower bound on migration for every sub-population
    private ScoredChromosome findMigrationBounds(int generation) throws IOException {
    	String inputPath = popPath + String.format("/population_%d_scored/part-r-00000", generation);
    	BufferedReader br = new BufferedReader(new FileReader(inputPath));
    	ScoredChromosome bestChromosome = null;
    	Map<Integer, List<ScoredChromosome>> bestChromosomes =
    			new HashMap<Integer, List<ScoredChromosome>>();
    	int pos = 0;
    	int currSubPop = pos % numSubPopulations;

        try {
            String line = br.readLine();

            while (line != null) {
            	currSubPop = pos % numSubPopulations;

            	String[] fields = line.split("\t");
            	double fitness = Double.valueOf(fields[1]);
            	if (!bestChromosomes.containsKey(currSubPop)) {
            		bestChromosomes.put(currSubPop, new ArrayList<ScoredChromosome>());
            	}
            	if (bestChromosome == null) {
            		bestChromosome = new ScoredChromosome(line);
            	}

            	if (bestChromosomes.get(currSubPop).size() < migrationNumber) {
            		List<ScoredChromosome> currList = bestChromosomes.get(currSubPop);
            		ScoredChromosome currChromosome = new ScoredChromosome(line);
            		currList.add(currChromosome);
            		bestChromosomes.put(currSubPop, currList);
            		lowerBounds[currSubPop] = (float) fitness;

            		if (currChromosome.score > bestChromosome.score) {
            			bestChromosome = currChromosome;
            		}
            	} else if (fitness > lowerBounds[currSubPop]) {
            		List<ScoredChromosome> currList = bestChromosomes.get(currSubPop);
            		ScoredChromosome currChromosome = new ScoredChromosome(line);
            		currList.add(currChromosome);
            		Collections.sort(currList);
            		currList.remove(migrationNumber);
            		bestChromosomes.put(currSubPop, currList);
            		lowerBounds[currSubPop] = currList.get(migrationNumber-1).score.floatValue();

            		if (currChromosome.score > bestChromosome.score) {
            			bestChromosome = currChromosome;
            		}
            	}
                line = br.readLine();
                pos++;
            }
        } finally {
            br.close();
        }

        return bestChromosome;
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
