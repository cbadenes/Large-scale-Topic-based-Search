package oeg.lstbs.algorithms;

import oeg.lstbs.data.TopicPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class CentroidBasedAlgorithm extends GroupsBasedAlgorithm {

    private static final Logger LOG = LoggerFactory.getLogger(CentroidBasedAlgorithm.class);


    public CentroidBasedAlgorithm() {
        super("centroid-based", 5);
    }

    @Override
    public List<TopicPoint> getGroups(List<Double> vector) {
        return null;
    }
}
