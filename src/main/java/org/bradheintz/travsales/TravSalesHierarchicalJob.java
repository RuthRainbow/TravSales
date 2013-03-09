package org.bradheintz.travsales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The job that works for every hierarchy level except the innermost; called from TravSalesJob
 *
 */
public class TravSalesHierarchicalJob extends HierarchicalJob implements Tool {
	private int numCities;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TravSalesHierarchicalJob(), args);
    }

    @Override
    protected Configuration setConfigValues(Configuration conf) {
    	conf.set("cities", problem);
    	conf.setInt("numCities", numCities);
    	return super.setConfigValues(conf);
    }

    @Override
    protected void readArgs(String[] args) {
    	super.readArgs(args);
    	numCities = Integer.valueOf(args[10]);
    }

}
