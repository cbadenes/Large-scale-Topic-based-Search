package oeg.lstbs.data;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class TopicSummary {

    private static final Logger LOG = LoggerFactory.getLogger(TopicSummary.class);

    private final DBSCANClusterer<TopicPoint> clusterer;
    private final List<TopicPoint> groups;

    private final HashFunction hf = Hashing.murmur3_32();


    public TopicSummary(List<Double> topicDistribution) {
        Stats stats = new Stats(topicDistribution);
        DistanceMeasure distanceMeasure = new MonoDimensionalDistanceMeasure();
        double eps      = stats.getVariance();
        int minPts      = 0;
        this.clusterer  = new DBSCANClusterer<>(eps,minPts,distanceMeasure);


        List<TopicPoint> points = IntStream.range(0, topicDistribution.size()).mapToObj(i -> new TopicPoint("" + i, topicDistribution.get(i))).collect(Collectors.toList());
        List<Cluster<TopicPoint>> clusterList = clusterer.cluster(points);

        this.groups = new ArrayList<>();
        int totalPoints = 0;
        for(Cluster<TopicPoint> cluster: clusterList){
            Double score    = (cluster.getPoints().stream().map(p -> p.score).reduce((x,y) -> x+y).get()) / (cluster.getPoints().size());
            String label    = cluster.getPoints().stream().map(p -> "t"+p.id).sorted((x,y) -> -x.compareTo(y)).collect(Collectors.joining("_"));

            totalPoints += cluster.getPoints().size();

            groups.add(new TopicPoint(label,score));
        }
        if (totalPoints < topicDistribution.size()){
            List<TopicPoint> clusterPoints = clusterList.stream().flatMap(l -> l.getPoints().stream()).collect(Collectors.toList());
            List<TopicPoint> isolatedTopics = points.stream().filter(p -> !clusterPoints.contains(p)).collect(Collectors.toList());
            Double score = (isolatedTopics.stream().map(p -> p.score).reduce((x,y) -> x+y).get()) / (isolatedTopics.size());
            String label = isolatedTopics.stream().map(p -> "t"+p.id).sorted((x,y) -> -x.compareTo(y)).collect(Collectors.joining("_"));
            groups.add(new TopicPoint(label,score));
        }
        Collections.sort(groups, (a, b) -> -a.score.compareTo(b.score));
    }

    public String getHashTopics(int top) {
        if (groups.size()<=top) return "";
        return groups.subList(0,groups.size()-top).stream().map(tp -> tp.id).collect(Collectors.joining("|"));
    }

    public Integer getHashCode(int top){
        if (groups.size()<=top) return 0;
        return hf.hashString(groups.subList(0,groups.size()-top).stream().map(tp -> tp.id).collect(Collectors.joining("|")), Charset.defaultCharset()).asInt();
    }

    public String getHashExpression(){
        return this.groups.stream().map(tp -> tp.id).collect(Collectors.joining("\n"));
    }


    private class TopicPoint implements Clusterable {

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
    }

    private class MonoDimensionalDistanceMeasure implements DistanceMeasure{

        @Override
        public double compute(double[] p1, double[] p2) {
            return Math.abs(p1[0]-p2[0]);
        }
    }

}