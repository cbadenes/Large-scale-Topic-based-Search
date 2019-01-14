package experiments;

import oeg.lstbs.hash.CentroidHHM;
import oeg.lstbs.hash.DensityHHM;
import oeg.lstbs.hash.ThresholdHHM;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class HashMethodTest {

    private static final Logger LOG = LoggerFactory.getLogger(HashMethodTest.class);


    List<Double> topicDistribution  = Arrays.asList(0.01779975,0.14165316,0.01029262,0.172136,0.03061161,0.09046587,0.19987289,0.13398581,0.03119906,0.17198322);

    Integer relevanceLevel          = 3;

    @Test
    public void thresholdBased(){
        LOG.info("Threshold-based ");
        Map<Integer, List<String>> hashcode = new ThresholdHHM(relevanceLevel).hash(topicDistribution);
        hashcode.entrySet().forEach(entry -> LOG.info("Level"+entry.getKey() + ": " + entry.getValue()));
    }

    @Test
    public void centroidBased(){
        LOG.info("Centroid-based ");
        Map<Integer, List<String>> hashcode = new CentroidHHM(relevanceLevel,1000).hash(topicDistribution);
        hashcode.entrySet().forEach(entry -> LOG.info("Level"+entry.getKey() + ": " + entry.getValue()));

    }


    @Test
    public void densityBased(){
        LOG.info("Density-based ");
        Map<Integer, List<String>> hashcode = new DensityHHM(relevanceLevel).hash(topicDistribution);
        hashcode.entrySet().forEach(entry -> LOG.info("Level"+entry.getKey() + ": " + entry.getValue()));

    }

}
