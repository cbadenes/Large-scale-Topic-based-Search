package oeg.lstbs.algorithms;

import oeg.lstbs.data.Stats;
import oeg.lstbs.data.TopicPoint;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class DensityBasedAlgorithm extends GroupsBasedAlgorithm {

    private static final Logger LOG = LoggerFactory.getLogger(DensityBasedAlgorithm.class);

    public DensityBasedAlgorithm() {
        super("density-based",7,1);
    }

    public List<TopicPoint> getGroups(List<Double> topicDistribution) {
        return cluster(topicDistribution, new Stats(topicDistribution).getVariance());
    }

    public static List<TopicPoint> cluster(List<Double> vector, double eps){
        DistanceMeasure distanceMeasure = new MonoDimensionalDistanceMeasure();

        int minPts = 0;

        DBSCANClusterer<TopicPoint> clusterer = new DBSCANClusterer<>(eps, minPts, distanceMeasure);


        List<TopicPoint> points = IntStream.range(0, vector.size()).mapToObj(i -> new TopicPoint("" + i, vector.get(i))).collect(Collectors.toList());
        List<Cluster<TopicPoint>> clusterList = clusterer.cluster(points);

        List<TopicPoint> groups = new ArrayList<>();
        int totalPoints = 0;
        for (Cluster<TopicPoint> cluster : clusterList) {
            Double score = (cluster.getPoints().stream().map(p -> p.getScore()).reduce((x, y) -> x + y).get()) / (cluster.getPoints().size());
            String label = cluster.getPoints().stream().map(p -> "t" + p.getId()).sorted((x, y) -> -x.compareTo(y)).collect(Collectors.joining("_"));

            totalPoints += cluster.getPoints().size();

            groups.add(new TopicPoint(label, score));
        }
        if (totalPoints < vector.size()) {
            List<TopicPoint> clusterPoints = clusterList.stream().flatMap(l -> l.getPoints().stream()).collect(Collectors.toList());
            List<TopicPoint> isolatedTopics = points.stream().filter(p -> !clusterPoints.contains(p)).collect(Collectors.toList());
            Double score = (isolatedTopics.stream().map(p -> p.getScore()).reduce((x, y) -> x + y).get()) / (isolatedTopics.size());
            String label = isolatedTopics.stream().map(p -> "t" + p.getId()).sorted((x, y) -> -x.compareTo(y)).collect(Collectors.joining("_"));
            groups.add(new TopicPoint(label, score));
        }
        Collections.sort(groups, (a, b) -> -a.getScore().compareTo(b.getScore()));
        return groups;
    }

    @Override
    public String id() {
        return "density";
    }


    private static class MonoDimensionalDistanceMeasure implements DistanceMeasure {

        @Override
        public double compute(double[] p1, double[] p2) {
            return Math.abs(p1[0] - p2[0]);
        }
    }

}
