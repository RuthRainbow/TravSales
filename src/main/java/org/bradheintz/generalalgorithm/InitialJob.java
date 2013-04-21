package org.bradheintz.generalalgorithm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

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
import org.bradheintz.travsales.TravSalesHierarchicalJob;

/**
 * The main job initialises the problem and processes the innermost level of hierarchy - to
 * be implemented with the algorithm specifics.
 *
 * @author bradheintz, ruthking
 */
public abstract class InitialJob extends Configured implements Tool{
	// Configuration and problem parameters
	protected static Configuration initialConfig;
	protected static Configuration iterativeConfig;
	protected static String problem;
	protected static int numNodes;

	// Static parameters
    private static final Topology topology = Topology.HYPERCUBE;
    private static final int hierarchyLevel = 0;

    // Genetic algorithm parameters
    protected static String popPath = "genetic_algorithm_populations";
    protected static int populationSize = 100000;
    protected static int selectionBinSize;
    protected static int numSubPopulations;
    protected static int maxGenerations = 500;
    protected static int numHierarchyLevels = 2;
    protected static int migrationFrequency = 3;
    protected static int migrationNumber;
    protected static float mutationChance = 0.01f;
    protected static float migrationPercentage = 0.0003f;
    protected static float survivorProportion = 0.3f;

    // Statistics parameters
    private static SubpopulationStatistics[] stats;
    protected double bestScoreCurrGen;
    protected int noImprovementCount;
    protected ScoredChromosome overallBestChromosome;
    protected ScoredChromosome currWorstChromosome;
    protected double currMeanFitness;

    public enum Topology {
    	HYPERCUBE, RING;
    }

    // The command line options and their shortcuts
    protected enum Option {
    	POPULATIONSIZE("ps", "population size"),
    	MAXNUMGENERATIONS("g", "maximum number of generations"),
    	NUMHIERARCHYLEVELS("h", "number of hierarchy levels"),
    	MUTATIONCHANCE("mc", "mutation chance"),
    	MIGRATIONRATE("mr", "migration rate"),
    	MIGRATIONPERCENTAGE("mp", "migration percentage"),
    	SURVIVORPROPORTION("sp", "survivor proportion"),
    	FILEPATH("f", "filepath");

    	protected final String shortcut;
    	private final String humanReadable;

    	Option(String shortcut, String str) {
    		this.shortcut = shortcut;
    		this.humanReadable = str;
    	}

    	@Override
    	public String toString() {
    		return humanReadable;
    	}
    }

    @Override
    public int run(String[] args) throws Exception {
    	// Clear the directory for the new run
    	FileUtils.deleteDirectory(new File(popPath));
    	initialConfig = new Configuration();

    	if (args != null) {
    		parseArgs(args);
    		validateArgs();
    	}

    	// Initialise algorithm parameters now command line args have been parsed
    	numSubPopulations = (int) Math.pow(10, numHierarchyLevels);
    	selectionBinSize = (int) populationSize/numSubPopulations;
    	migrationNumber = (int) Math.floor(populationSize * migrationPercentage);
    	stats = new SubpopulationStatistics[numSubPopulations];
    	for (int i = 0; i < numSubPopulations; i++) {
    		stats[i] = new SubpopulationStatistics();
    	}

    	// Initialise the problem and the initial population
        FileSystem fs = FileSystem.get(initialConfig);
        problem = setUpInitialProblem(fs.create(new Path("_CITY_MAP")), initialConfig);
        initialConfig = setInitialConfigValues(initialConfig);
        popPath = setPopPath();
        System.out.println("city map created...");
    	createInitialPopulation(fs.create(
    			new Path(popPath + "/population_0/population_0_init")), populationSize);
        System.out.println("initial population created...");
    	Job job = setUpInitialJob();

        FileInputFormat.setInputPaths(job, new Path(popPath + "/population_0"));
        FileOutputFormat.setOutputPath(job, new Path(popPath + "/tmp_0_0"));

        // Run the initial job
        if (!job.waitForCompletion(true)) {
            System.out.println("Failure scoring first generation");
            System.exit(1);
        }

        // Copy the tmp file across as the initial population should just be scored
        FileUtils.copyDirectory(new File(popPath + "/tmp_0_0"), new File(
        		popPath + "/population_0_scored"));

        // Iterate over the generations of the algorithm
        iterate();

        // Write the results and delete the initial temporary file
        System.out.println("BEST INDIVIDUAL WAS " + overallBestChromosome);
        writeResultToFile();
        FileUtils.deleteDirectory(new File(popPath + "/tmp_0_0"));
    	return 0;
    }

    // Method to set the path to where files created by the algorithm are stored
    protected String setPopPath() {
		return popPath;
	}

    /* Set any configuration values for the initial iteration, dedicated to scoring the
     * initial population */
	protected Configuration setInitialConfigValues(Configuration conf) {
    	conf.set("problem", problem);
    	return conf;
    }

	// Append the results from the given generation to the output file
    private void appendCurrentResults(ScoredChromosome bestChromosome, int generation) {
    	try {
    	    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
    	    		popPath + "/result", true)));
    	    out.println(generation + ") BEST: " + bestChromosome + " WORST: " + currWorstChromosome
    	    		+ " AVERAGE FITNESS: " + currMeanFitness);
    	    out.close();
    	} catch (IOException e) {
    		System.out.println("Failure writing this generation's overview to file");
    	    e.printStackTrace();
    	    System.exit(1);
    	}
    }

    /* Method for setting up the problem to be solved - must be implemented.
     * Returns the problem expressed as a string */
	protected abstract String setUpInitialProblem(
			FSDataOutputStream fsDataOutputStream, Configuration conf)
			throws IOException;

	/* Set up the initial job used by this class - the first job dedicated to scoring the initial
	 * population */
    protected Job setUpInitialJob() throws IOException {
    	 Job job = new Job(initialConfig, "hierarchicalGA");

         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(DoubleWritable.class);

         job.setJarByClass(setJarByClass());
         job.setMapperClass(setInitialMapperClass());

         return job;
    }

    // Set the classes used by the first initial job
	protected abstract Class<? extends InitialJob> setJarByClass();
	protected abstract Class<? extends ScoringMapper> setInitialMapperClass();

	// Iterate over another generation, whilst the convergence criteria has not yet been met
    private void iterate() throws Exception {
    	int generation = 0;
        while (!convergenceCriteriaMet(generation)) {
            selectAndReproduce(generation, problem);
            // Calculate statistics for the processed generation
            ScoredChromosome bestChromosome = findMigrationBounds(generation);
            printBestIndividual(generation, bestChromosome);
            if (bestChromosome.getScore() > bestScoreCurrGen) {
            	bestScoreCurrGen = bestChromosome.getScore();
            	noImprovementCount = 0;
            	overallBestChromosome = bestChromosome;
            } else {
            	noImprovementCount++;
            }
            generation++;
        }
    }

    // Method that checks convergence criteria, returns true if algorithm deemed to have converged
    protected boolean convergenceCriteriaMet(int generation) {
    	if (noImprovementCount < 300 && generation < maxGenerations) {
    		return false;
    	} else {
    		return true;
    	}
    }

    // Perform a genetic algorithm for the given generation
    private void selectAndReproduce(int generation, String problem) throws Exception {
    	// Set up the configuration for this iteration
    	iterativeConfig = new Configuration();
    	iterativeConfig = setIterativeConfigValues(iterativeConfig, generation);
        Job iterativeJob = setUpIterativeJob(generation);

        FileInputFormat.setInputPaths(iterativeJob, new Path(popPath + String.format(
        		"/population_%d_scored", generation)));
        // If at the last level, write output to a permanent file, else write to a temporary file
        if (numHierarchyLevels == 1) {
        	FileOutputFormat.setOutputPath(iterativeJob, new Path(popPath + String.format(
        			"/population_%d_scored", generation + 1)));
        } else {
        	FileOutputFormat.setOutputPath(iterativeJob, new Path(popPath + String.format(
        			"/tmp_%d_1", generation)));
        }

        // Run the job for the current generation
        System.out.println(String.format("Hierarchy level %d: Selecting from population %d, " +
        		"breeding and scoring population %d", hierarchyLevel, generation, generation + 1));
        if (!iterativeJob.waitForCompletion(true)) {
            System.out.println(String.format(
            		"FAILURE selecting & reproducing generation %d", generation));
            System.exit(1);
        }

        // Run the other levels of hierarchy for this generation
        runLevels(generation);

        // Remove any temporary files created by this generation
        for (int i = 1; i < numHierarchyLevels; i++) {
        	FileUtils.deleteDirectory(new File(popPath + String.format("/tmp_%d_%d", generation, i)));
        }
	}

    // Set up the configuration values for the main, iterative job for the given generation.
    protected Configuration setIterativeConfigValues(Configuration conf, int generation) {
    	conf.setFloat("survivorProportion", survivorProportion);
    	conf.setInt("selectionBinSize", selectionBinSize);
    	conf.setInt("numSubPopulations", numSubPopulations);
    	conf.setFloat("mutationChance", mutationChance);
    	conf.set("problem", problem);
    	conf.setInt("migrationFrequency", migrationFrequency);
    	conf.setInt("generation", generation);
    	conf.setEnum("topology", topology);
    	conf.setInt("populationSize", populationSize);
    	// Add the current statistics for every subpopulation
        for (int i = 0; i < stats.length; i++) {
        	conf.setFloat("lowerBound" + i, stats[i].lowerBound);
        }
        conf.setInt("noImprovementCount", noImprovementCount);
        conf.setInt("hierarchyLevel", hierarchyLevel);
        conf.setBoolean("finalHierarchyLevel", hierarchyLevel + 1 == numHierarchyLevels);
        /* Work out whether the generation is a migration generation here in order to safe
         * calculating this in every mapper */
        boolean isMigrate = migrationFrequency == 0 ? false :
        	(generation%migrationFrequency == 0 ? true : false);
        conf.setBoolean("isMigrate", isMigrate);
        return conf;
    }

    /* Set up the main iterative job for the given generation by assigning input/output types and
     * classes for the job, mapper and reducer */
    protected Job setUpIterativeJob(int generation) throws IOException {
    	Job job = new Job(iterativeConfig, String.format(
    			"hierarchical_select_and_reproduce_%d", generation));

        job.setInputFormatClass(KeyValueFormat.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(setJarByClass());
        job.setMapperClass(setMapperByClass());
        job.setReducerClass(setReducerByClass());

        return job;
    }

    // Set the mapper - by default this is the SelectionBinMapper although this can be extended
    protected Class<? extends SelectionBinMapper> setMapperByClass() {
    	return SelectionBinMapper.class;
    }

    // Set the reducer - must be implemented
    protected abstract Class<? extends SelectionReproductionReducer> setReducerByClass();

    // Run each of the hierarchy levels needed
    private void runLevels(int generation) throws Exception {
         for (int i = 1; i < numHierarchyLevels; i++) {
        	 Configuration conf = createHierarchicalConfig(generation, i);
        	runHierarchicalJob(conf);
         }
    }

    // Run a hierarchical job
    protected void runHierarchicalJob(Configuration conf) throws Exception {
    	ToolRunner.run(conf, new TravSalesHierarchicalJob(), new String[0]);
    }

    protected Configuration createHierarchicalConfig(int generation, int level) {
    	Configuration conf = new Configuration();
    	// hierarchy indexes start from 0
    	conf.setBoolean("finalHierarchyLevel", (level + 1 == numHierarchyLevels) ? true : false);
    	conf.setInt("generation", generation);
    	conf.setInt("populationSize", populationSize);
    	/* The number of subpopulations is 10 to the power of the inverse level index - less
     	 * subpopulations at the upper levels */
    	conf.setInt("numSubPopulations", (int) Math.pow(10, numHierarchyLevels - level));
    	conf.setInt("hierarchyLevel", level+1);
    	/* Increase the rate of migration as a multiple of the level number - less migration at
     	 * upper levels */
    	conf.setInt("migrationFrequency", migrationFrequency * (level + 1));
    	conf.setInt("noImprovementCount", noImprovementCount);
    	conf.setFloat("migrationPercentage", migrationPercentage);
    	conf.setFloat("mutationChance", mutationChance);
    	conf.setFloat("survivorPropotion", survivorProportion);
    	conf.set("popPath", popPath);
    	conf.set("problem", problem);
    	return conf;
    }

    // Parse any command line parameters - check they are valid then set the corresponding parameter
    protected void parseArgs(String[] args) {
    	int num = -1;
    	float dec = -1;
    	String str = null;

    	// Process each command value pairing
    	for (int i = 0; i < args.length; i+=2) {
    		dec = -1;
    		num = -1;
    		str = null;
    		Option chosenOption = null;

    		// Check the current parameter starts with a -
    		if (!args[0].startsWith("-")) {
    			System.out.println("Incorrect formatting - " +
    					"all parameters must begin with a '-' character");
    			System.exit(1);
    		}

    		// Parse the command option, either as the full name or the shortcut
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

    		// Check this command has a value and the type of this value
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

    		// Set the corresponding variable, checking the value given is valid
    		switch (chosenOption) {
    			case POPULATIONSIZE : populationSize = checkValid(num, chosenOption); break;
    			case MAXNUMGENERATIONS : maxGenerations = checkValid(num, chosenOption); break;
    			case NUMHIERARCHYLEVELS : numHierarchyLevels = checkValid(num, chosenOption); break;
    			case MIGRATIONRATE: migrationFrequency = checkValid(num, chosenOption); break;
    			case MUTATIONCHANCE : mutationChance = checkValid(dec, chosenOption); break;
    			case MIGRATIONPERCENTAGE:
    				migrationPercentage = checkValid(dec, chosenOption); break;
    			case SURVIVORPROPORTION : survivorProportion = checkValid(dec, chosenOption); break;
    			case FILEPATH : popPath = checkValid(str, chosenOption); break;
    			default: break;
    		}
    	}
	}

    // Check a number is non negative
	private int checkValid(int num, Option parameter) {
		if (num < 0) {
			System.out.println("Incorrect formatting - parameter "
					+ parameter + " must be a positive integer");
			System.exit(1);
		}
		return num;
	}

	// Check a string is not null
	private String checkValid(String str, Option parameter) {
		if (str == null) {
			System.out.println("Incorrect formatting - parameter "
					+ parameter + " must be a string");
			System.exit(1);
		}
		return str;
	}

	// Check a float is in the range [0,1]
	private float checkValid(float num, Option parameter) {
		if (num > 1 || num < 0) {
			System.out.println("Incorrect formatting - parameter "
					+ parameter + " must be a float between 0 and 1");
			System.exit(1);
		}
		return num;
	}

	/* Other validation, such as checking the number of hierarchy levels is acceptable for the
	 * population size */
	protected void validateArgs() {
		if (Math.pow(10, numHierarchyLevels+1) > populationSize) {
			System.out.println("Incorrect format - population size must be greater than 10 to the "
					+ "power of the number of hierarchy levels, as the minimum size of " +
					"sub-populations is 10");
			System.exit(1);
		}
	}

	// Return the option that corresponds to the shortcut, or return null if no shortcut matches
	private Option tryShortcut(String parameter) {
		switch (parameter) {
			case "ps" : return Option.POPULATIONSIZE;
			case "g" : return Option.MAXNUMGENERATIONS;
			case "h" : return Option.NUMHIERARCHYLEVELS;
			case "mc" : return Option.MUTATIONCHANCE;
			case "mr" : return Option.MIGRATIONRATE;
			case "mp" : return Option.MIGRATIONPERCENTAGE;
			case "sp" : return Option.SURVIVORPROPORTION;
			case "f" : return Option.FILEPATH;
			default : return null;
		}
	}

	// Print the best individual from the current generation to standard out
	protected void printBestIndividual(int generation, ScoredChromosome bestChromosome)
			throws IOException {
        System.out.println("BEST INDIVIDUAL OF GENERATION " + generation + " IS " + bestChromosome);
        appendCurrentResults(bestChromosome, generation);
	}

	// Write the final result, the best chromosome found, to file
    protected void writeResultToFile() {
    	try {
			String content = overallBestChromosome.toString();
			File file = new File(popPath + "/finalresult");
			file.createNewFile();

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(content);
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    // Find the lower bound on migration for every subpopulation
    protected ScoredChromosome findMigrationBounds(int generation) throws IOException {
    	String inputPath = popPath + String.format(
    			"/population_%d_scored/part-r-00000", generation);
    	BufferedReader br = new BufferedReader(new FileReader(inputPath));
    	ScoredChromosome bestChromosome = null;
    	int pos = 0;
    	int currSubPop = pos % numSubPopulations;
    	currMeanFitness = 0;
    	double currWorstScore = Double.MAX_VALUE;

    	// Reset the statistics array for the new information
    	for (int i = 0; i < numSubPopulations; i++) {
    		stats[i].reset();
    	}

    	// Parse the generation results line by line
        try {
            String line = br.readLine();

            while (line != null) {
            	/* Check if the current individual changes the statistics either globally or for its
            	 *  own subpopulation */
            	currSubPop = pos % numSubPopulations;
            	ScoredChromosome currChromosome = new ScoredChromosome(line);
            	double fitness = currChromosome.getScore();

            	if (bestChromosome == null) {
            		bestChromosome = new ScoredChromosome(line);
            	}

            	stats[currSubPop].add(currChromosome, migrationNumber);

            	if (currChromosome.getScore() > bestChromosome.getScore()) {
            		bestChromosome = currChromosome;
            	}

            	if (fitness < currWorstScore) {
            		currWorstScore = fitness;
            		currWorstChromosome = currChromosome;
            	}
            	currMeanFitness += fitness;
            	line = br.readLine();
            	pos++;
            }

            // Once all lines have been parsed, calculate the statistics for each subpopulation
        	for (int i = 0; i < numSubPopulations; i++) {
        		stats[i].calculate();
        	}
        } finally {
        	br.close();
        }

        // Calculate the global mean fitness for the entire population
        currMeanFitness /= pos;

        return bestChromosome;
    }

    // Create the initial population - to be implemented
    protected abstract void createInitialPopulation(FSDataOutputStream populationOutfile,
    		final int populationSize) throws IOException;

}