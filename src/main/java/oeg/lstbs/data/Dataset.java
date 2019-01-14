package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Dataset {

    private static final Logger LOG = LoggerFactory.getLogger(Dataset.class);

    Corpus corpus;

    Integer indexSize;

    Integer testSize;

    Integer relevantSize;

    public Dataset(Corpus corpus, Integer indexSize, Integer testSize, Integer relevantSize) {
        this.corpus = corpus;
        this.indexSize = indexSize;
        this.testSize = testSize;
        this.relevantSize = relevantSize;
    }

    public Corpus getCorpus() {
        return corpus;
    }

    public Integer getIndexSize() {
        return indexSize;
    }

    public Integer getTestSize() {
        return testSize;
    }

    public Integer getRelevantSize() {
        return relevantSize;
    }

    @Override
    public String toString() {
        return "Dataset{" +
                "corpus=" + corpus +
                ", indexSize=" + indexSize +
                ", testSize=" + testSize +
                ", relevantSize=" + relevantSize +
                '}';
    }
}
