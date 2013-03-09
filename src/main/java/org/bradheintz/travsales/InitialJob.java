package org.bradheintz.travsales;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * The main job - to be implemented with the algorithm specifics
 *
 * @author ruthking
 */
public abstract class InitialJob extends Configured implements Tool{

	protected static Configuration initialConfig;
	protected static Configuration iterativeConfig;
	protected static String problem;
	protected static int numNodes;

    private static final float survivorProportion = 0.3f;
    private static final Topology topology = Topology.RING;
    private static final int hierarchyLevel = 0;

    protected static int populationSize = 10000;
    protected static int selectionBinSize;
    protected static int numSubPopulations;
    protected static int maxGenerations = 500;
    protected static int numHierarchyLevels = 1;
    protected static float mutationChance = 0.01f;
    protected static int migrationFrequency = 3;
    protected static float migrationPercentage = 0.0003f;
    protected static int migrationNumber;
    protected static String popPath = "genetic_algorithm_populations";

    protected static float[] lowerBounds;
    protected double bestScoreCurrGen;
    protected int noImprovementCount;
    protected ScoredChromosome overallBestChromosome;
    protected ScoredChromosome currWorstChromosome;
    protected double currMeanFitness;

    public enum Topology {
    	HYPERCUBE, RING;
    }

    protected enum Option {
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

    @Override
    public int run(String[] args) throws Exception {
    	FileUtils.deleteDirectory(new File(popPath));
    	initialConfig = new Configuration();

    	if (args != null) {
    		parseArgs(args);
    		validateArgs();
    	}

    	numSubPopulations = (int) Math.pow(10, numHierarchyLevels);
    	selectionBinSize = (int) populationSize/numSubPopulations;
    	lowerBounds = new float[numSubPopulations];
    	migrationNumber = (int) Math.floor(populationSize * migrationPercentage);

        FileSystem fs = FileSystem.get(initialConfig);
        problem = setUpInitialProblem(fs.create(new Path("_CITY_MAP")), initialConfig);
        initialConfig = setInitialConfigValues(initialConfig);

        popPath = setPopPath();

        System.out.println("city map created...");
    	createInitialPopulation(fs.create(new Path(popPath + "/population_0/population_0_init")), populationSize);
        System.out.println("initial population created...");
    	Job job = setUpInitialJob();

        FileInputFormat.setInputPaths(job, new Path(popPath + "/population_0"));
        FileOutputFormat.setOutputPath(job, new Path(popPath + "/tmp_0_0"));

        if (!job.waitForCompletion(true)) {
            System.out.println("Failure scoring first generation");
            System.exit(1);
        }

        // Copy the tmp file across as the initial population should just be scored
        FileUtils.copyDirectory(new File(popPath + "/tmp_0_0"), new File(popPath + "/population_0_scored"));

        iterate();
        System.out.println("BEST INDIVIDUAL WAS " + overallBestChromosome);
        // MAY HAVE TO HAVE THIS ON HDFS?
        writeResultToFile();
        FileUtils.deleteDirectory(new File(popPath + "/tmp_0_0"));
    	return 0;
    }

    protected String setPopPath() {
		return popPath;
	}

	protected Configuration setInitialConfigValues(Configuration conf) {
    	conf.set("problem", problem);
    	return conf;
    }

    private void iterate() throws Exception {
    	int generation = 0;
        while (!convergenceCriteriaMet(generation)) {
            selectAndReproduce(generation, problem);
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

    protected boolean convergenceCriteriaMet(int generation) {
    	if (noImprovementCount < 50 && generation < maxGenerations) {
    		return false;
    	} else {
    		return true;
    	}
    }

    private void appendCurrentResults(ScoredChromosome bestChromosome, int generation) {
    	try {
    	    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(popPath + "/result", true)));
    	    out.println(generation + ") BEST: " + bestChromosome + " WORST: " + currWorstChromosome
    	    		+ " AVERAGE FITNESS: " + currMeanFitness);
    	    out.close();
    	} catch (IOException e) {
    		System.out.println("Failure writing this generation's overview to file");
    	    e.printStackTrace();
    	    System.exit(1);
    	}
    }

	protected abstract String setUpInitialProblem(FSDataOutputStream fsDataOutputStream, Configuration conf)
			throws IOException;

    protected Job setUpInitialJob() throws IOException {
    	 Job job = new Job(initialConfig, "hierarchicalGA");

         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(DoubleWritable.class);

         job.setJarByClass(TravSalesJob.class);
         job.setMapperClass(ScoringMapper.class);

         return job;
    }

    private void selectAndReproduce(int generation, String problem2) throws Exception {
    	iterativeConfig = new Configuration();
    	iterativeConfig = setIterativeConfigValues(iterativeConfig, generation);

        Job iterativeJob = setUpIterativeJob(generation);

        FileInputFormat.setInputPaths(iterativeJob, new Path(popPath + String.format("/population_%d_scored", generation)));
        if (numHierarchyLevels == 1) {
        	FileOutputFormat.setOutputPath(iterativeJob, new Path(popPath + String.format("/population_%d_scored", generation + 1)));
        } else {
        	FileOutputFormat.setOutputPath(iterativeJob, new Path(popPath + String.format("/tmp_%d_1", generation)));
        }

        System.out.println(String.format("Hierarchy level %d: Selecting from population %d, " +
        		"breeding and scoring population %d", hierarchyLevel, generation, generation + 1));
        if (!iterativeJob.waitForCompletion(true)) {
            System.out.println(String.format("FAILURE selecting & reproducing generation %d", generation));
            System.exit(1);
        }

        runLevels(generation);

        for (int i = 1; i < numHierarchyLevels; i++) {
        	FileUtils.deleteDirectory(new File(popPath + String.format("/tmp_%d_%d", generation, i)));
        }
	}

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
        for (int i = 0; i < lowerBounds.length; i++) {
        	conf.setFloat("lowerBound" + i, lowerBounds[i]);
        }
        conf.setInt("noImprovementCount", noImprovementCount);
        conf.setInt("hierarchyLevel", hierarchyLevel);
        conf.setBoolean("finalHierarchyLevel", hierarchyLevel + 1 == numHierarchyLevels);
        return conf;
    }

    protected Job setUpIterativeJob(int generation) throws IOException {
    	Job job = new Job(iterativeConfig, String.format("hierarchical_select_and_reproduce_%d", generation));

        job.setInputFormatClass(KeyValueFormat.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(TravSalesJob.class);
        job.setMapperClass(SelectionBinMapper.class);
        job.setReducerClass(SelectionReproductionReducer.class);

        return job;
    }

    private void runLevels(int generation) throws Exception {
         for (int i = 1; i < numHierarchyLevels; i++) {
        	String[] args = fillArgs(generation, i);
        	runHierarchicalJob(args);
         }
    }

    protected void runHierarchicalJob(String[] args) throws Exception {
    	ToolRunner.run(new TravSalesHierarchicalJob(), args);
    }

    /* Args: <generation #> <population size> <# subpopulations> <hierarchy level>
       <final hierarchy level?> <migration frequency> <migration percentage> <mutation chance>
       <population filepath> <problem string> <no improvement count> */
    protected String[] fillArgs(int generation, int level) {
    	String[] args = new String[11];
    	args[0] = String.valueOf(generation);
     	args[1] = String.valueOf(populationSize);
     	args[2] = String.valueOf((int) Math.pow(10, numHierarchyLevels - level));
     	args[3] = String.valueOf(level);
     	// hierarchy indexes start from 0
     	args[4] = (level + 1 == numHierarchyLevels) ? String.valueOf(true) : String.valueOf(false);
     	// TODO maybe this doesn't work well for many hierarchies - in parallel?
     	args[5] = String.valueOf(migrationFrequency * level * 3);
     	args[6] = String.valueOf(migrationPercentage);
     	args[7] = String.valueOf(mutationChance);
     	args[8] = popPath;
     	args[9] = problem;
     	args[10] = String.valueOf(noImprovementCount);
     	return args;
    }

    protected void parseArgs(String[] args) {
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

	protected void validateArgs() {
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

	protected void printBestIndividual(int generation, ScoredChromosome bestChromosome) throws IOException {
        System.out.println("BEST INDIVIDUAL OF GENERATION " + generation + " IS " + bestChromosome);
        appendCurrentResults(bestChromosome, generation);
	}

    protected void writeResultToFile() {
    	try {
			String content = overallBestChromosome.toString();
			File file = new File(popPath + "/finalresult");

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
    protected ScoredChromosome findMigrationBounds(int generation) throws IOException {
    	String inputPath = popPath + String.format("/population_%d_scored/part-r-00000", generation);
    	BufferedReader br = new BufferedReader(new FileReader(inputPath));
    	ScoredChromosome bestChromosome = null;
    	Map<Integer, List<ScoredChromosome>> bestChromosomes =
    			new HashMap<Integer, List<ScoredChromosome>>();
    	int pos = 0;
    	int currSubPop = pos % numSubPopulations;
    	currMeanFitness = 0;
    	double currWorstScore = Double.MAX_VALUE;

        try {
            String line = br.readLine();

            while (line != null) {
            	currSubPop = pos % numSubPopulations;
            	ScoredChromosome currChromosome = new ScoredChromosome(line);
            	double fitness = currChromosome.getScore();

            	if (!bestChromosomes.containsKey(currSubPop)) {
            		bestChromosomes.put(currSubPop, new ArrayList<ScoredChromosome>());
            	}
            	if (bestChromosome == null) {
            		bestChromosome = new ScoredChromosome(line);
            	}

            	if (bestChromosomes.get(currSubPop).size() < migrationNumber) {
            		List<ScoredChromosome> currList = bestChromosomes.get(currSubPop);
            		currList.add(currChromosome);
            		bestChromosomes.put(currSubPop, currList);
            		lowerBounds[currSubPop] = (float) fitness;

            		if (currChromosome.getScore() > bestChromosome.getScore()) {
            			bestChromosome = currChromosome;
            		}
            	} else if (fitness > lowerBounds[currSubPop]) {
            		List<ScoredChromosome> currList = bestChromosomes.get(currSubPop);
            		currList.add(currChromosome);
            		Collections.sort(currList);
            		currList.remove(migrationNumber);
            		bestChromosomes.put(currSubPop, currList);
            		lowerBounds[currSubPop] = currList.get(migrationNumber-1).getScore().floatValue();

            		if (currChromosome.getScore() > bestChromosome.getScore()) {
            			bestChromosome = currChromosome;
            		}
            	}

            	if (fitness < currWorstScore) {
            		currWorstScore = fitness;
            		currWorstChromosome = currChromosome;
            	}
            	currMeanFitness += fitness;
                line = br.readLine();
                pos++;
            }
        } finally {
            br.close();
        }

        currMeanFitness /= populationSize;

        return bestChromosome;
    }

    protected abstract void createInitialPopulation(FSDataOutputStream populationOutfile,
    		final int populationSize) throws IOException;

}
