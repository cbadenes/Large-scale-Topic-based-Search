package oeg.lstbs.metrics;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public interface ComparisonMetric {

    String id();

    Double distance(List<Double> v1, List<Double> v2);

    Double similarity(List<Double> v1, List<Double> v2);
}
