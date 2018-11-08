package oeg.lstbs.algorithms;

import oeg.lstbs.data.Document;
import oeg.lstbs.data.Similarity;
import oeg.lstbs.metrics.ComparisonMetric;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public interface Explorer {

    boolean add(Document document);

    boolean commit();

    /**
     * Results measured in terms of precision, recall, map and time
     * @param metric similarity metric
     * @param counter monitor number of comparisons required to perform it
     * @return documents with the highest similarity value in the corpus
     */
    List<Similarity> findDuplicates(ComparisonMetric metric, AtomicInteger counter);

    /**
     * Results measured in terms of precision, recall, map and time
     * @param query reference document for the search
     * @param metric
     * @param maxResults maximum number of documents to be returned
     * @param counter monitor number of comparisons required to perform it
     * @return
     */
    List<Similarity> findSimilarTo(Document query, ComparisonMetric metric, int maxResults, AtomicInteger counter);
}
