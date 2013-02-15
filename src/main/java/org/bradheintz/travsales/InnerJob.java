package org.bradheintz.travsales;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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

public class InnerJob extends Configured implements Tool {

	// LATER these should all be configurable
    private static final String popPath = "travsales_populations";
    private static final float topTierToSave = 0.1f; // TODO
    private static final float survivorProportion = 0.3f;
    private static final float mutationChance = 0.01f;
    private static final int migrationFrequency = 10;
    private static final int migrationNumber = 3;
    private static final Topology topology = Topology.HYPERCUBE;

    private static int generation;
    private static int numCities;
    private static int populationSize;
    private static int selectionBinSize;
    private static int numSubPopulations;
    private static float lowerBound;

    // Args: generation number, number of cities, population size
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new InnerJob(), args);
    }

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        String roadmap = createTrivialRoadmap(fs.create(new Path("_CITY_MAP")), conf, numCities);

        generation = Integer.valueOf(args[0]);
        numCities = Integer.valueOf(args[1]);
        populationSize = Integer.valueOf(args[2]);
        selectionBinSize = (int) Math.ceil(populationSize/10);
        numSubPopulations = (int) Math.ceil(populationSize/selectionBinSize);

        selectAndReproduce(generation, roadmap);
		return 0;
	}

	protected static void selectAndReproduce(int generation, String roadmap) throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat("survivorProportion", survivorProportion);
        conf.setFloat("topTierToSave", topTierToSave);
        conf.setInt("selectionBinSize", selectionBinSize);
        conf.setInt("numSubPopulations", numSubPopulations);
        conf.setFloat("mutationChance", mutationChance);
        conf.setFloat("lowerBound", lowerBound);
        conf.set("cities", roadmap);
        conf.setInt("migrationFrequency", migrationFrequency);
        conf.setInt("generation", generation);
        conf.setEnum("topology", topology);
        conf.setInt("populationSize", populationSize);

        Job job = new Job(conf, String.format("inner_travsales_select_and_reproduce_%d", generation));

        job.setInputFormatClass(KeyValueFormat.class);
        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(InnerJob.class);
        job.setMapperClass(SelectionBinMapper.class);
        job.setReducerClass(InnerReducer.class);

        FileInputFormat.setInputPaths(job, new Path(popPath + String.format("/tmp_%d", generation)));
        FileOutputFormat.setOutputPath(job, new Path(popPath + String.format("/population_%d_scored", generation + 1)));

        System.out.println(String.format("INNER Selecting from population %d, breeding and scoring population %d", generation, generation + 1));
        if (!job.waitForCompletion(true)) {
            System.out.println(String.format("FAILURE selecting & reproducing generation %d INNER", generation));
            System.exit(1);
        }

        findMigrationBounds(generation);
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

	private static void findMigrationBounds(int generation) throws IOException {
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
    }

}
