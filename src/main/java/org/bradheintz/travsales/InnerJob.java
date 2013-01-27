package org.bradheintz.travsales;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InnerJob extends Configured implements Tool {
	
	// LATER these should all be configurable
    private static String inputPath = "travsales_populations_outer";
    private static String outputPath = "travsales_populations";
    private static int numCities = 20;
    private static int populationSize = 10000;
    private static int selectionBinSize = 10000;
    private static float topTierToSave = 0.1f; // TODO
    private static float survivorProportion = 0.3f;
    private static float mutationChance = 0.01f;
    
    public static void main(String[] args) throws Exception {
    	FileUtils.deleteDirectory(new File(outputPath));
        ToolRunner.run(new InnerJob(), args);
    }
    
	@Override
	public int run(String[] args) throws Exception {
		System.out.println("RUNNING THE INNER JOB ARGS ARE " + Arrays.toString(args));
		
		FileUtils.deleteDirectory(new File(outputPath));
		
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        FileSystem fs = FileSystem.get(conf);
        String roadmap = createTrivialRoadmap(fs.create(new Path("_CITY_MAP")), conf, numCities);
        conf.set("cities", roadmap);

        Job job = new Job(conf, "innerJob");

        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(InnerJob.class);
        job.setMapperClass(InnerMapper.class);

        //ASSUMING POP NUMBER WILL COME IN AT ARGS[0];
        //FileInputFormat.setInputPaths(job, new Path(inputPath + "/population_" + args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(outputPath + "/population_" + args[0] + "_scored"));
        
        FileInputFormat.setInputPaths(job, new Path(inputPath + "/tmp_" + 0));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/population_" + 0 + "_scored"));

        if (!job.waitForCompletion(true)) {
            System.out.println("Failure scoring first generation");
            System.exit(1);
        }

        selectAndReproduce(0, roadmap);
		return 0;
	}
	
	protected static void selectAndReproduce(int generation, String roadmap)
    		throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat("survivorProportion", survivorProportion);
        conf.setFloat("topTierToSave", topTierToSave);
        conf.setInt("selectionBinSize", selectionBinSize);
        conf.setFloat("mutationChance", mutationChance);
        conf.set("cities", roadmap);

        Job job = new Job(conf, String.format("travsales_select_and_reproduce_%d", generation));

        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(InnerJob.class);
        job.setMapperClass(InnerMapper.class);
        job.setReducerClass(InnerReducer.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath + String.format("/tmp_%d", generation)));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + String.format("/population_%d_scored", generation + 1)));

        System.out.println(String.format("Selecting from population %d, breeding and scoring population %d", generation, generation + 1));
        if (!job.waitForCompletion(true)) {
            System.out.println(String.format("FAILURE selecting & reproducing generation %d", generation));
            System.exit(1);
        }
        
        ToolRunner.run(new InnerJob(), null);
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

}
