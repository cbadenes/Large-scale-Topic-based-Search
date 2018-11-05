package oeg.lstbs.metrics;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public interface SimilarityMetric {

    String id();

    Double compare(List<Double> v1, List<Double> v2);
}
