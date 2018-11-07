package oeg.lstbs.algorithms;

import com.google.common.collect.MinMaxPriorityQueue;
import oeg.lstbs.data.Document;
import oeg.lstbs.data.LuceneRepository;
import oeg.lstbs.data.Similarity;
import oeg.lstbs.io.SerializationUtils;
import oeg.lstbs.metrics.ComparisonMetric;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
        luceneDoc.add(new StoredField("vector", bytesRef));

        this.repository.add(luceneDoc);
        return true;
    }

    @Override
    public boolean commit() {
        this.repository.commit();
        return true;
    }

    @Override
    public List<Similarity> findDuplicates(ComparisonMetric metric, int maxResults, int level, AtomicInteger counter) {


        MinMaxPriorityQueue<Similarity> pairs = MinMaxPriorityQueue.orderedBy(new Similarity.ScoreComparator()).maximumSize(maxResults).create();

        TopDocs results = this.repository.getBy(new MatchAllDocsQuery(), -1);

        for(ScoreDoc scoreDoc: results.scoreDocs){
            org.apache.lucene.document.Document doc = repository.getDocument(scoreDoc.doc);

            String id = String.format(doc.get("name"));

            BytesRef byteRef = doc.getBinaryValue("vector");
            List<Double> vector = (List<Double>) SerializationUtils.deserialize(byteRef.bytes);

            Document d1 = new Document(id,vector);


            IndexReader indexReader = this.repository.getReader();

            for(int i=0; i < scoreDoc.doc; i++){

                try {
                    org.apache.lucene.document.Document doc2 = indexReader.document(i);
                    String id2 = String.format(doc2.get("name"));

                    BytesRef byteRef2 = doc2.getBinaryValue("vector");
                    List<Double> vector2 = (List<Double>) SerializationUtils.deserialize(byteRef2.bytes);

                    Document d2 = new Document(id2,vector2);
                    pairs.add(new Similarity(metric.similarity(vector, vector2),d1,d2));
                    counter.incrementAndGet();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

//
//            for(Similarity sim: findSimilarTo(d1, scoreDoc.doc, metric, maxResults, counter)){
//                pairs.add(sim);
//            }
        }

        return pairs.stream().sorted((a,b) -> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());
    }

    @Override
    public List<Similarity> findSimilarTo(Document query, ComparisonMetric metric, int maxResults) {

        return findSimilarTo(query,0,metric,maxResults);
    }

    private List<Similarity> findSimilarTo(Document query, int offset, ComparisonMetric metric, int maxResults) {

        TopDocs results = this.repository.getBy(new MatchAllDocsQuery(), -1);

        MinMaxPriorityQueue<Similarity> pairs = MinMaxPriorityQueue.orderedBy(new Similarity.ScoreComparator()).maximumSize(maxResults).create();

        for(ScoreDoc scoreDoc: results.scoreDocs){

            if (scoreDoc.doc <= offset) continue;

            org.apache.lucene.document.Document doc = repository.getDocument(scoreDoc.doc);

            String id = String.format(doc.get("name"));

            if (id.equalsIgnoreCase(query.getId())) continue;

            BytesRef byteRef = doc.getBinaryValue("vector");
            List<Double> vector = (List<Double>) SerializationUtils.deserialize(byteRef.bytes);

            Document d1 = new Document(id,vector);

            Similarity similarity = new Similarity(metric.similarity(d1.getVector(), query.getVector()), query, d1);

            pairs.add(similarity);
        }

        return pairs.stream().sorted((a,b) -> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());
    }

}
