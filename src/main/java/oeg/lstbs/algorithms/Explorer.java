package oeg.lstbs.algorithms;

import oeg.lstbs.data.Document;
import oeg.lstbs.metrics.SimilarityMetric;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public interface Explorer {

    boolean add(Document document);

    boolean commit();

    /**
     * Results measured in terms of precision, recall, map and time
     * @param metric similarity metric
     * @return documents with the highest similarity value in the corpus
     */
    List<Document> findDuplicates(SimilarityMetric metric);

    /**
     * Results measured in terms of precision, recall, map and time
     * @param query reference document for the search
     * @param metric
     * @param maxResults maximum number of documents to be returned
     * @return
     */
    List<Document> findSimilarTo(Document query, SimilarityMetric metric, int maxResults);
}
