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
 * The base hierarchical job - can be implemented with the algorithm specifics or run as is
 *
 * @author ruthking
 */
public abstract class HierarchicalJob extends Configured implements Tool {
	private Configuration config;

    private static final float survivorProportion = 0.3f;
    private static final Topology topology = Topology.RING;
    protected static String problem;

    protected static String popPath;
    protected static int generation;
    protected static int populationSize;
    protected static int selectionBinSize;
    protected static int numSubPopulations;
    protected static int hierarchyLevel;
    protected static boolean finalHierarchyLevel;
    private static SubpopulationStatistics[] stats;
    protected static int migrationFrequency;
    protected static int migrationNumber;
    protected static float mutationChance;
    protected static int noImprovementCount;

    @Override
	public int run(String[] args) throws Exception {
		config = new Configuration();
		readArgs(args);

        selectAndReproduce(generation, problem);
		return 0;
	}

    protected void readArgs(String[] args) {
    	/* Args: <generation #> <population size> <# subpopulations> <hierarchy level>
        <final hierarchy level?> <migration frequency> <migration percentage> <mutation chance>
        <population filepath> <problem string> <no improvement count> */
    	generation = Integer.valueOf(args[0]);
    	populationSize = Integer.valueOf(args[1]);
    	numSubPopulations = (int) Integer.valueOf(args[2]);
    	selectionBinSize = (int) populationSize/numSubPopulations;
    	stats = new SubpopulationStatistics[numSubPopulations];
    	for (int i = 0; i < numSubPopulations; i++) {
    		stats[i] = new SubpopulationStatistics();
    	}
    	hierarchyLevel = Integer.valueOf(args[3]);
    	finalHierarchyLevel = Boolean.valueOf(args[4]);
    	migrationFrequency = Integer.valueOf(args[5]);
    	migrationNumber = (int) Math.floor(populationSize * Float.parseFloat(args[6]));
    	mutationChance = Float.parseFloat(args[7]);
    	popPath = args[8];
    	problem = args[9];
    	noImprovementCount = Integer.valueOf(args[10]);
    }

    protected void selectAndReproduce(int generation, String roadmap) throws Exception {
    	findMigrationBounds(generation);

    	config = setConfigValues(config);
    	Job job = setUpHierarchicalJob(generation);

        FileInputFormat.setInputPaths(job, new Path(popPath + String.format("/tmp_%d_%d", generation, hierarchyLevel-1)));
        if (finalHierarchyLevel == true) {
        	FileOutputFormat.setOutputPath(job, new Path(popPath + String.format("/population_%d_scored", generation + 1)));
        } else {
        	FileOutputFormat.setOutputPath(job, new Path(popPath + String.format("/tmp_%d_%d", generation, hierarchyLevel)));
        }

        System.out.println(String.format("Hierarchy level %d: Selecting from population %d, " +
        		"breeding and scoring population %d", hierarchyLevel, generation, generation + 1));

        if (!job.waitForCompletion(true)) {
            System.out.println(String.format("FAILURE selecting & reproducing generation %d INNER", generation));
            System.exit(1);
        }
    }

    protected Configuration setConfigValues(Configuration conf) {
        conf.setFloat("survivorProportion", survivorProportion);
        conf.setInt("selectionBinSize", selectionBinSize);
        conf.setInt("numSubPopulations", numSubPopulations);
        conf.setFloat("mutationChance", mutationChance);
        conf.set("problem", problem);
        conf.setInt("migrationFrequency", migrationFrequency);
        conf.setInt("generation", generation);
        conf.setEnum("topology", topology);
        conf.setInt("populationSize", populationSize);
        for (int i = 0; i < stats.length; i++) {
        	conf.setFloat("lowerBound" + i, stats[i].lowerBound);
        }
        conf.setInt("noImprovementCount", noImprovementCount);
        conf.setInt("hierarchyLevel", hierarchyLevel);
        conf.setBoolean("finalHierarchyLevel", finalHierarchyLevel);
        boolean isMigrate = migrationFrequency == 0 ? false : (generation%migrationFrequency == 0 ? true : false);
        conf.setBoolean("isMigrate", isMigrate);
        return conf;
    }

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

    protected abstract Class<? extends HierarchicalJob> setJarByClass();

    protected Class<? extends SelectionBinMapper> setMapperByClass() {
    	return SelectionBinMapper.class;
    }

    protected abstract Class<? extends SelectionReproductionReducer> setReducerByClass();

	// Finds the lower bound on migration for every sub-population
    protected ScoredChromosome findMigrationBounds(int generation) throws IOException {
    	String inputPath = popPath + String.format("/population_%d_scored/part-r-00000", generation);
    	BufferedReader br = new BufferedReader(new FileReader(inputPath));
    	ScoredChromosome bestChromosome = null;
    	int pos = 0;
    	int currSubPop = pos % numSubPopulations;
    	double currWorstScore = Double.MAX_VALUE;

    	for (int i = 0; i < numSubPopulations; i++) {
    		stats[i].reset();
    	}

        try {
            String line = br.readLine();

            while (line != null) {
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

        	for (int i = 0; i < numSubPopulations; i++) {
        		stats[i].calculate();
        	}
        } finally {
        	br.close();
        }

        return bestChromosome;
    }
}