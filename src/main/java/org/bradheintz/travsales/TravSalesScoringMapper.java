package org.bradheintz.travsales;

import org.apache.hadoop.conf.Configuration;
import org.bradheintz.generalalgorithm.ScoringMapper;

/**
 * Implementation of ScoringMapper for TravSales.
 *
 * @author ruthrawr
 */
public class TravSalesScoringMapper extends ScoringMapper {

	// Create the TSP specific scorer, checking there is a city map to use
	@Override
	protected void createScorer(Configuration config) throws InterruptedException {
		if (config.get("cities") == null) {
			throw new InterruptedException("Failure! No city map.");
		}
		scorer = new TravSalesScorer(config.get("cities"));
	}

}
