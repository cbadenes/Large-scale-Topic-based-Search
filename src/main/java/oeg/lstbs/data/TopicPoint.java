package oeg.lstbs.data;

import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class TopicPoint implements Clusterable {

    private final String id;
    private final Double score;

    public TopicPoint(String id, Double score) {
        this.id = id;
        this.score = score;
    }

    @Override
    public double[] getPoint() {
        return new double[]{score};
    }

    public String getId() {
        return id;
    }

    public Double getScore() {
        return score;
    }
}
