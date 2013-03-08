package org.bradheintz.travsales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The job that works for every hierarchy level except the innermost; called from TravSalesJob
 *
 */
public class TravSalesHierarchicalJob extends HierarchicalJob implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TravSalesHierarchicalJob(), args);
    }

    @Override
    protected Configuration setConfigValues(Configuration conf) {
    	conf.set("cities", problem);
    	return super.setConfigValues(conf);
    }


}
