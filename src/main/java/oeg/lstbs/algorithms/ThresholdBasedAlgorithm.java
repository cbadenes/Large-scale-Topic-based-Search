package oeg.lstbs.algorithms;

import oeg.lstbs.data.TopicPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class ThresholdBasedAlgorithm extends GroupsBasedAlgorithm {

    private static final Logger LOG = LoggerFactory.getLogger(ThresholdBasedAlgorithm.class);


    public ThresholdBasedAlgorithm() {
        super("threshold-based",2,1);
    }

    @Override
    public List<TopicPoint> getGroups(List<Double> vector) {
        final double threshold = 1.0 / Double.valueOf(vector.size());

        List<TopicPoint> activeTopics = IntStream.range(0, vector.size()).mapToObj(i -> new TopicPoint("t" + i, vector.get(i))).filter(tp -> tp.getScore() > threshold).collect(Collectors.toList());
        String activePointId    = activeTopics.stream().map(tp -> tp.getId()).sorted((a,b) -> a.compareTo(b)).collect(Collectors.joining("_"));
        Double activeScore      = activeTopics.stream().map(tp -> tp.getScore()).reduce((a,b) -> a+b).get() / Double.valueOf(activeTopics.size());

        List<TopicPoint> inactiveTopics = IntStream.range(0, vector.size()).mapToObj(i -> new TopicPoint("t" + i, vector.get(i))).filter(tp -> tp.getScore() <= threshold).collect(Collectors.toList());
        String inactivePointId    = inactiveTopics.stream().map(tp -> tp.getId()).sorted((a,b) -> a.compareTo(b)).collect(Collectors.joining("_"));
        Double inactiveScore      = inactiveTopics.stream().map(tp -> tp.getScore()).reduce((a,b) -> a+b).get() / Double.valueOf(activeTopics.size());

        return Arrays.asList(new TopicPoint(activePointId, activeScore), new TopicPoint(inactivePointId, inactiveScore));

    }

    @Override
    public String id() {
        return "threshold";
    }
}
