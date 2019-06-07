package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Relation {

    private static final Logger LOG = LoggerFactory.getLogger(Relation.class);

    private String queryDoc;

    private String relDoc;

    private Double score;

    private String metric;

    public Relation() {
    }

    public String getQueryDoc() {
        return queryDoc;
    }

    public void setQueryDoc(String queryDoc) {
        this.queryDoc = queryDoc;
    }

    public String getRelDoc() {
        return relDoc;
    }

    public void setRelDoc(String relDoc) {
        this.relDoc = relDoc;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }
}
