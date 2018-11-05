package oeg.lstbs.algorithms;

import oeg.lstbs.data.Document;
import oeg.lstbs.metrics.SimilarityMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class ThresholdBasedAlgorithm implements Explorer {

    private static final Logger LOG = LoggerFactory.getLogger(ThresholdBasedAlgorithm.class);

    @Override
    public boolean add(Document document) {
        return false;
    }

    @Override
    public boolean commit() {
        return false;
    }

    @Override
    public List<Document> findDuplicates(SimilarityMetric metric) {
        return null;
    }

    @Override
    public List<Document> findSimilarTo(Document query, SimilarityMetric metric, int maxResults) {
        return null;
    }
}
