package oeg.lstbs.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class KL implements ComparisonMetric {

    private static final Logger LOG = LoggerFactory.getLogger(KL.class);

    @Override
    public String id() {
        return "Kullback-Leibler";
    }

    @Override
    public Double distance(List<Double> v1, List<Double> v2) {

        assert (v1.size() == v2.size());

        double klDiv = 0.0;
        for (int i = 0; i < v1.size(); ++i) {
            if (v1.get(i) == 0) {
                continue;
            }
            if (v2.get(i) == 0) {
                return Double.POSITIVE_INFINITY;
            }
            klDiv += v1.get(i) * Math.log(v1.get(i)/ v2.get(i));
        }
        return klDiv; // moved this division out of the loop -DM

    }

    @Override
    public Double similarity(List<Double> v1, List<Double> v2) {
        return 1-distance(v1,v2);
    }
}
