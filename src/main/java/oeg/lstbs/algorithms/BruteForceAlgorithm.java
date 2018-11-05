package oeg.lstbs.algorithms;

import oeg.lstbs.data.Document;
import oeg.lstbs.data.LuceneRepository;
import oeg.lstbs.data.Similarity;
import oeg.lstbs.io.SerializationUtils;
import oeg.lstbs.metrics.SimilarityMetric;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class BruteForceAlgorithm implements Explorer {

    private static final Logger LOG = LoggerFactory.getLogger(BruteForceAlgorithm.class);
    private final LuceneRepository repository;

    public BruteForceAlgorithm() {
        this.repository = new LuceneRepository("brute-force");
    }

    @Override
    public boolean add(Document document) {


        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();

        luceneDoc.add(new TextField("name", document.getId(), Field.Store.YES));

        BytesRef bytesRef = new BytesRef(SerializationUtils.serialize(document.getVector()));
        luceneDoc.add(new BinaryDocValuesField("vector", bytesRef));

        this.repository.add(luceneDoc);
        return true;
    }

    @Override
    public boolean commit() {
        this.repository.commit();
        return true;
    }

    @Override
    public List<Document> findDuplicates(SimilarityMetric metric) {

        TopDocs results = this.repository.getBy(new MatchAllDocsQuery(), -1);

        List<Document> documents = new ArrayList<>();

        Double topScore = 0.0;
        List<Document> similarDocs = new ArrayList<>();

        for(ScoreDoc scoreDoc: results.scoreDocs){
            org.apache.lucene.document.Document doc = repository.getDocument(scoreDoc.doc);

            String id = String.format(doc.get("name"));

            BytesRef byteRef = doc.getBinaryValue("vector");
            List<Double> vector = (List<Double>) SerializationUtils.deserialize(byteRef.bytes);

            Document d1 = new Document(id,vector);

            documents.stream().map(d2 -> new Similarity(metric.compare(d1.getVector(), d2.getVector()), d1, d2));

            documents.add(d1);

        }

        return null;
    }

    @Override
    public List<Document> findSimilarTo(Document query, SimilarityMetric metric, int maxResults) {
        return null;
    }

}
