package org.bradheintz.travsales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bradheintz.generalalgorithm.HierarchicalJob;
import org.bradheintz.generalalgorithm.SelectionReproductionReducer;

/**
 * The job that works for every hierarchy level except the innermost; called from TravSalesJob
 *
 * @author ruthking
 */
public class TravSalesHierarchicalJob extends HierarchicalJob implements Tool {

	// Run this hierarchical job
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TravSalesHierarchicalJob(), args);
    }

    // For TravSales the problem is the city map
    @Override
    protected Configuration setConfigValues(Configuration conf) {
    	conf.set("cities", problem);
    	return super.setConfigValues(conf);
    }

    // Set the specific job and reducer for TravSales
	@Override
	protected Class<? extends HierarchicalJob> setJarByClass() {
		return TravSalesHierarchicalJob.class;
	}

	@Override
	protected Class<? extends SelectionReproductionReducer> setReducerByClass() {
		return TravSalesReducer.class;
	}

}
