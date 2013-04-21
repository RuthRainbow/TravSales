package org.bradheintz.generalalgorithm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.bradheintz.generalalgorithm.InitialJob.Topology;

/**
 * The base hierarchical job, processes all levels except the innermost - must be implemented with
 * the algorithm specifics
 *
 * @author ruthking
 */
public abstract class HierarchicalJob extends Configured implements Tool {
	// Configuration parameter
	protected Configuration config;

	// Static parameters - topology is a ring as less mobility at outer levels
    private static final Topology topology = Topology.RING;

    // General algorithm parameters
    protected static String problem;
    protected static String popPath;
    protected static boolean finalHierarchyLevel;
    protected static int generation;
    protected static int selectionBinSize;
    protected static int numSubPopulations;
    protected static int hierarchyLevel;
    protected static int migrationFrequency;
    protected static int migrationNumber;

    // Statistics parameters
    private static SubpopulationStatistics[] stats;

    // To run this level set up the config then run the algorithm
    @Override
	public int run(String[] args) throws Exception {
		config = this.getConf();
		parseConfig();
		initialiseStatistics();
        selectAndReproduce(generation, problem);
		return 0;
	}

    // Get parameter values from the config parsed to the job
    protected void parseConfig() {
    	finalHierarchyLevel = config.getBoolean("finalHierarchyLevel", true);
    	generation = config.getInt("generation", 0);
    	int populationSize = config.getInt("populationSize", 100000);
    	numSubPopulations = config.getInt("numSubPopulations", 100);
    	selectionBinSize = (int) populationSize/numSubPopulations;
    	hierarchyLevel = config.getInt("hierarchyLevel", 2);
    	migrationFrequency = config.getInt("migrationFrequency", 2);
    	migrationNumber = (int) Math.floor(
    			populationSize * config.getFloat("migrationPercentage", 0));
    	popPath = config.get("popPath");
    	problem = config.get("problem");
    }

    // Initialise the statistics array
    protected void initialiseStatistics() {
    	stats = new SubpopulationStatistics[numSubPopulations];
    	for (int i = 0; i < numSubPopulations; i++) {
    		stats[i] = new SubpopulationStatistics();
    	}
    }

    // Process a single level of hierarchy for this generation
    protected void selectAndReproduce(int generation, String problem) throws Exception {
    	// Firstly, find the migration bounds for the current generation
    	findMigrationBounds(generation);

    	// Set up the configuration and the job
    	config = setConfigValues(config);
    	Job job = setUpHierarchicalJob(generation);

        FileInputFormat.setInputPaths(job, new Path(popPath + String.format(
        		"/tmp_%d_%d", generation, hierarchyLevel-1)));
        if (finalHierarchyLevel == true) {
        	FileOutputFormat.setOutputPath(job, new Path(popPath + String.format(
        			"/population_%d_scored", generation + 1)));
        } else {
        	FileOutputFormat.setOutputPath(job, new Path(popPath + String.format(
        			"/tmp_%d_%d", generation, hierarchyLevel)));
        }

        System.out.println(String.format("Hierarchy level %d: Selecting from population %d, " +
        		"breeding and scoring population %d", hierarchyLevel, generation, generation + 1));

        // Run the job
        if (!job.waitForCompletion(true)) {
            System.out.println(String.format(
            		"FAILURE selecting & reproducing generation %d INNER", generation));
            System.exit(1);
        }
    }

    /* Set the configuration values not yet set (most were set in the initial job and
     * passed through the ToolRunner */
    protected Configuration setConfigValues(Configuration conf) {
        conf.setInt("selectionBinSize", selectionBinSize);
        conf.setEnum("topology", topology);
        for (int i = 0; i < stats.length; i++) {
        	conf.setFloat("lowerBound" + i, stats[i].lowerBound);
        }
        /* Calculate whether the current generation is a migration generation here to save
         * calculations in the mapper */
        boolean isMigrate = migrationFrequency == 0 ?
        		false : (generation%migrationFrequency == 0 ? true : false);
        conf.setBoolean("isMigrate", isMigrate);
        return conf;
    }

    // Set up the job, including the input/output classes and the jar, mapper and reducer classes
    protected Job setUpHierarchicalJob(int generation) throws IOException {
    	Job job = new Job(config, String.format("inner_hierarchical_select_and_reproduce_%d_%d",
    			generation, hierarchyLevel));

        job.setInputFormatClass(KeyValueFormat.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(setJarByClass());
        job.setMapperClass(setMapperByClass());
        job.setReducerClass(setReducerByClass());

        return job;
    }

    // Set the job class - must be extended
    protected abstract Class<? extends HierarchicalJob> setJarByClass();

    // Set the mapper class - by default this is the SelectionBinMapper
    protected Class<? extends SelectionBinMapper> setMapperByClass() {
    	return SelectionBinMapper.class;
    }

    // Set the reducer class - must be extended
    protected abstract Class<? extends SelectionReproductionReducer> setReducerByClass();

	// Find the lower bound on migration for every subpopulation
    protected ScoredChromosome findMigrationBounds(int generation) throws IOException {
    	String inputPath = popPath + String.format(
    			"/population_%d_scored/part-r-00000", generation);
    	BufferedReader br = new BufferedReader(new FileReader(inputPath));
    	ScoredChromosome bestChromosome = null;
    	int pos = 0;
    	int currSubPop = pos % numSubPopulations;
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
            	}
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

        return bestChromosome;
    }
}