package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class TopicDistribution {

    private static final Logger LOG = LoggerFactory.getLogger(TopicDistribution.class);

    Stats stats;

    String docId;

    String label;

    List<Double> vector;

    public TopicDistribution() {
    }

    public TopicDistribution(Stats stats, String docId, List<Double> vector) {
        this.stats = stats;
        this.docId = docId;
        this.vector = vector;
    }

    public Stats getStats() {
        return stats;
    }

    public void setStats(Stats stats) {
        this.stats = stats;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public List<Double> getVector() {
        return vector;
    }

    public void setVector(List<Double> vector) {
        this.vector = vector;
    }


    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return "TopicDistribution{" +
                "docId='" + docId + '\'' +
                "label='" + label + '\'' +
                ", vector=" + vector +
                '}';
    }
}
