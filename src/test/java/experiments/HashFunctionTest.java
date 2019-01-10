package experiments;

import oeg.lstbs.algorithms.CentroidBasedAlgorithm;
import oeg.lstbs.algorithms.DensityBasedAlgorithm;
import oeg.lstbs.data.TopicPoint;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class HashFunctionTest {

    private static final Logger LOG = LoggerFactory.getLogger(HashFunctionTest.class);


    List<Double> topicDistribution  = Arrays.asList(0.01779975,0.14165316,0.01029262,0.168136,0.03061161,0.09046587,0.19987289,0.13398581,0.03119906,0.17598322);

    Integer relevanceLevel          = 3;

    @Test
    public void thresholdBased(){
        double inc = 1.0 / (Double.valueOf(topicDistribution.size())*Double.valueOf(relevanceLevel));
        LOG.info("inc=" + inc);
        double acc = inc;
        for(int i=0;i<relevanceLevel;i++){
            double lowerLimit = (relevanceLevel-i)*inc;
            double upperLimit = i==0? Double.MAX_VALUE : lowerLimit+inc;

            List<Integer> ts = IntStream.range(0,topicDistribution.size()).filter(s -> topicDistribution.get(s) > lowerLimit && topicDistribution.get(s) < upperLimit).boxed().collect(Collectors.toList());
            LOG.info("set " + i + ": " + ts);
            acc += inc;
        }
    }

    @Test
    public void centroidBased(){

        CentroidBasedAlgorithm cb = new CentroidBasedAlgorithm();
        List<TopicPoint> g = cb.getGroups(topicDistribution);

        for(int i=0;i<relevanceLevel;i++){
            String ts = g.get(i).getId();
            LOG.info("set " + i + ": " + ts);
        }

    }


    @Test
    public void densityBased(){

        DensityBasedAlgorithm cb = new DensityBasedAlgorithm();
        List<TopicPoint> g = cb.getGroups(topicDistribution);

        for(int i=0;i<relevanceLevel;i++){
            String ts = g.get(i).getId();
            LOG.info("set " + i + ": " + ts);
        }

    }

}
