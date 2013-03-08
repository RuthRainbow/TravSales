/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main job - forms the innermost hierarchy level and calls all other levels.
 *
 * @author bradheintz, ruthking
 */
public class TravSalesJob extends InitialJob implements Tool {

    private static Random random = new Random();

    private static final int numCities = 20;

    private static String popPath = "travsales_populations";

    public static void main(String[] args) throws Exception {
    	FileUtils.deleteDirectory(new File(popPath));
        ToolRunner.run(new TravSalesJob(), args);
        FileUtils.deleteDirectory(new File(popPath + "/tmp_0_0"));
    }

    // For TravSales the problem is the city map
    @Override
    protected Configuration setInitialConfigValues(Configuration conf) {
    	conf.set("cities", problem);
    	return super.setInitialConfigValues(conf);
    }

    @Override
    protected String setPopPath() {
    	return popPath;
    }


    @Override
    protected Job setUpInitialJob() throws IOException {
    	Job job = new Job(initialConfig, "travsales");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setJarByClass(TravSalesJob.class);
        job.setMapperClass(ScoringMapper.class);

        FileInputFormat.setInputPaths(job, new Path(popPath + "/population_0"));
        FileOutputFormat.setOutputPath(job, new Path(popPath + "/tmp_0_0"));

        return job;
    }

    // For TravSales the problem is the city map
    @Override
    protected Configuration setIterativeConfigValues(Configuration conf, int generation) {
    	conf.set("cities", problem);
    	return super.setIterativeConfigValues(conf, generation);
    }

    protected static ArrayList<double[]> createMap(int numCities) {
        ArrayList<double[]> roadmap = new ArrayList<double[]>(numCities);
        for (int i = 0; i < numCities; ++i) {
            double[] coords = {random.nextDouble(), random.nextDouble()};
            roadmap.add(coords);
        }
        return roadmap;
    }

    @Override
    protected String setUpInitialProblem(FSDataOutputStream hdfsOut, Configuration hadoopConfig)
    		throws IOException {
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

    @Override
    protected void createInitialPopulation(FSDataOutputStream populationOutfile,
    		final int populationSize) throws IOException {
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
