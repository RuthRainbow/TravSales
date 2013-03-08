package org.bradheintz.travsales;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.bradheintz.travsales.InitialJob.Topology;

/**
 * The base hierarchical job - can be implemented with the algorithm specifics or run as is
 *
 * @author ruthking
 */
public class HierarchicalJob extends Configured implements Tool {
	private Configuration config;

    private static final float survivorProportion = 0.3f;
    private static final Topology topology = Topology.HYPERCUBE;
    protected static String problem;

    protected static String popPath;
    protected static int generation;
    protected static int populationSize;
    protected static int selectionBinSize;
    protected static int numSubPopulations;
    protected static int hierarchyLevel;
    protected static boolean finalHierarchyLevel;
    protected static float[] lowerBounds;
    protected static int migrationFrequency;
    protected static int migrationNumber;
    protected static float mutationChance;

    @Override
	public int run(String[] args) throws Exception {
		config = new Configuration();

        /* Args: <generation #> <population size> <# subpopulations> <hierarchy level>
           <final hierarchy level?> <migration frequency> <migration percentage> <mutation chance>
           <population filepath> <problem string> */
        generation = Integer.valueOf(args[0]);
        populationSize = Integer.valueOf(args[1]);
        numSubPopulations = (int) Integer.valueOf(args[2]);
        selectionBinSize = (int) populationSize/numSubPopulations;
        lowerBounds = new float[numSubPopulations];
        hierarchyLevel = Integer.valueOf(args[3]);
        finalHierarchyLevel = Boolean.valueOf(args[4]);
        migrationFrequency = Integer.valueOf(args[5]);
        migrationNumber = (int) Math.floor(populationSize * Float.valueOf(args[6]));
        mutationChance = Float.valueOf(args[7]);
        popPath = args[8];
        problem = args[9];

        selectAndReproduce(generation, problem);
		return 0;
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
        for (int i = 0; i < lowerBounds.length; i++) {
        	conf.setFloat("lowerBound" + i, lowerBounds[i]);
        }
        return conf;
    }

    protected Job setUpHierarchicalJob(int generation) throws IOException {
    	Job job = new Job(config, String.format("inner_hierarchical_select_and_reproduce_%d_%d",
    			generation, hierarchyLevel));

        job.setInputFormatClass(KeyValueFormat.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(TravSalesHierarchicalJob.class);
        job.setMapperClass(SelectionBinMapper.class);
        job.setReducerClass(HierarchicalReducer.class);

        return job;
    }

    // Finds the lower bound on migration for every sub-population
    private static ScoredChromosome findMigrationBounds(int generation) throws IOException {
    	String inputPath = popPath + String.format("/tmp_%d_%d/part-r-00000", generation, hierarchyLevel-1);
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
}