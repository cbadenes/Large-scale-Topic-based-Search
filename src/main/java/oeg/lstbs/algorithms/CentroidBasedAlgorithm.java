package oeg.lstbs.algorithms;

import oeg.lstbs.data.TopicPoint;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
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

public class CentroidBasedAlgorithm extends GroupsBasedAlgorithm {

    private static final Logger LOG = LoggerFactory.getLogger(CentroidBasedAlgorithm.class);


    public CentroidBasedAlgorithm() {
        super("centroid-based", 5,1);
    }

    @Override
    public List<TopicPoint> getGroups(List<Double> topicDistribution) {

        DistanceMeasure distanceMeasure = new CentroidBasedAlgorithm.MonoDimensionalDistanceMeasure();

        KMeansPlusPlusClusterer<TopicPoint> clusterer = new KMeansPlusPlusClusterer(5, 100, distanceMeasure);

        List<TopicPoint> points = IntStream.range(0, topicDistribution.size()).mapToObj(i -> new TopicPoint("" + i, topicDistribution.get(i))).collect(Collectors.toList());
        List<CentroidCluster<TopicPoint>> clusterList = clusterer.cluster(points);
        try{

            List<TopicPoint> groups = new ArrayList<>();
            int totalPoints = 0;
            for (Cluster<TopicPoint> cluster : clusterList) {
                if (cluster.getPoints().isEmpty()) continue;
                Double score = (cluster.getPoints().stream().map(p -> p.getScore()).reduce((x, y) -> x + y).get()) / (cluster.getPoints().size());
                String label = cluster.getPoints().stream().map(p -> "t" + p.getId()).sorted((x, y) -> -x.compareTo(y)).collect(Collectors.joining("_"));

                totalPoints += cluster.getPoints().size();

                groups.add(new TopicPoint(label, score));
            }
            if (totalPoints < topicDistribution.size()) {
                List<TopicPoint> clusterPoints = clusterList.stream().flatMap(l -> l.getPoints().stream()).collect(Collectors.toList());
                List<TopicPoint> isolatedTopics = points.stream().filter(p -> !clusterPoints.contains(p)).collect(Collectors.toList());
                Double score = (isolatedTopics.stream().map(p -> p.getScore()).reduce((x, y) -> x + y).get()) / (isolatedTopics.size());
                String label = isolatedTopics.stream().map(p -> "t" + p.getId()).sorted((x, y) -> -x.compareTo(y)).collect(Collectors.joining("_"));
                groups.add(new TopicPoint(label, score));
            }
            Collections.sort(groups, (a, b) -> -a.getScore().compareTo(b.getScore()));
            return groups;

        }catch (Exception e){
            LOG.error("Unexpected error",e);
            return Collections.emptyList();
        }
    }

    @Override
    public String id() {
        return "centroid";
    }


    private class MonoDimensionalDistanceMeasure implements DistanceMeasure {

        @Override
        public double compute(double[] p1, double[] p2) {
            return Math.abs(p1[0] - p2[0]);
        }
    }
}
