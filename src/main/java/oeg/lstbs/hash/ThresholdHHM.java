package oeg.lstbs.hash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class ThresholdHHM implements HierarchicalHashMethod {

    private static final Logger LOG = LoggerFactory.getLogger(ThresholdHHM.class);
    private final int depth;

    public ThresholdHHM(int depth) {
        this.depth = depth;
    }

    @Override
    public String id() {
        return "threshold";
    }

    @Override
    public Map<Integer, List<String>> hash(List<Double> topicDistribution) {
        double inc = 1.0 / (Double.valueOf(topicDistribution.size())*Double.valueOf(depth+1));
        Map<Integer,List<String>> hashCode = new HashMap<>();
        for(int i=0;i<depth;i++){
            double lowerLimit = (depth-i)*inc;
            double upperLimit = i==0? Double.MAX_VALUE : lowerLimit+inc;

            List<Integer> ts = IntStream.range(0,topicDistribution.size()).filter(s -> topicDistribution.get(s) > lowerLimit && topicDistribution.get(s) < upperLimit).boxed().collect(Collectors.toList());
            hashCode.put(i,ts.stream().map(t -> "t"+t).collect(Collectors.toList()));
        }
        return hashCode;
    }
}
