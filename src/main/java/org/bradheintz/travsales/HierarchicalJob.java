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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bradheintz.travsales.TravSalesJob.Topology;

/**
 * The job that works for every hierarchy level except the innermost; called from TravSalesJob
 *
 */
public class HierarchicalJob extends Configured implements Tool {

    private static final String popPath = "travsales_populations";
    private static final float survivorProportion = 0.3f;
    private static final Topology topology = Topology.HYPERCUBE;

    private static int generation;
    private static int numCities;
    private static int populationSize;
    private static int selectionBinSize;
    private static int numSubPopulations;
    private static int hierarchyLevel;
    private static boolean finalHierarchyLevel;
    private static float[] lowerBounds;
    private static int migrationFrequency;
    private static int migrationNumber;
    private static float mutationChance;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HierarchicalJob(), args);
    }

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        String roadmap = createTrivialRoadmap(fs.create(new Path("_CITY_MAP")), conf, numCities);

        /* Args: <generation #> <# cities> <population size> <# subpopulations> <hierarchy level>
           <final hierarchy level?> <migration frequency> <migration percentage> <mutation chance> */
        generation = Integer.valueOf(args[0]);
        numCities = Integer.valueOf(args[1]);
        populationSize = Integer.valueOf(args[2]);
        numSubPopulations = (int) Integer.valueOf(args[3]);
        selectionBinSize = (int) populationSize/numSubPopulations;
        lowerBounds = new float[numSubPopulations];
        hierarchyLevel = Integer.valueOf(args[4]);
        finalHierarchyLevel = Boolean.valueOf(args[5]);
        migrationFrequency = Integer.valueOf(args[6]);
        migrationNumber = (int) Math.floor(populationSize * Float.valueOf(args[7]));
        mutationChance = Float.valueOf(args[8]);

        selectAndReproduce(generation, roadmap);
		return 0;
	}

	protected static void selectAndReproduce(int generation, String roadmap) throws Exception {
		findMigrationBounds(generation);

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

        Job job = new Job(conf, String.format("inner_travsales_select_and_reproduce_%d_%d", generation, hierarchyLevel));

        job.setInputFormatClass(KeyValueFormat.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(HierarchicalJob.class);
        job.setMapperClass(SelectionBinMapper.class);
        job.setReducerClass(HierarchicalReducer.class);

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
