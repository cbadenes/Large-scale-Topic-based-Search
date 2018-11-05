package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Similarity {

    private static final Logger LOG = LoggerFactory.getLogger(Similarity.class);

    Double score;

    Document d1;

    Document d2;

    public Similarity(Double score, Document d1, Document d2) {
        this.score = score;
        this.d1 = d1;
        this.d2 = d2;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Document getD1() {
        return d1;
    }

    public void setD1(Document d1) {
        this.d1 = d1;
    }

    public Document getD2() {
        return d2;
    }

    public void setD2(Document d2) {
        this.d2 = d2;
    }
}
