package org.bradheintz.travsales;

import org.apache.hadoop.conf.Configuration;
import org.bradheintz.generalalgorithm.ScoringMapper;

public class TravSalesScoringMapper extends ScoringMapper {

	@Override
	protected void createScorer(Configuration config) throws InterruptedException {
		if (config.get("cities") == null) {
			throw new InterruptedException("Failure! No city map.");
		}
		scorer = new TravSalesScorer(config.get("cities"));
	}

}
