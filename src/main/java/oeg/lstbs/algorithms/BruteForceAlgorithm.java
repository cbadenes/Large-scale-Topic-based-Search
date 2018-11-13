package oeg.lstbs.algorithms;

import com.google.common.collect.MinMaxPriorityQueue;
import oeg.lstbs.data.Document;
import oeg.lstbs.data.LuceneRepository;
import oeg.lstbs.data.Similarity;
import oeg.lstbs.data.Time;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.SerializationUtils;
import oeg.lstbs.metrics.ComparisonMetric;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class BruteForceAlgorithm implements Explorer {

    private static final Logger LOG = LoggerFactory.getLogger(BruteForceAlgorithm.class);

    private final LuceneRepository repository;

    private final Double threshold;


    public BruteForceAlgorithm() {
        this.threshold = 0.9;
        this.repository = new LuceneRepository("brute-force");
    }

    public BruteForceAlgorithm(Double threshold) {
        this.threshold = threshold;
        this.repository = new LuceneRepository("brute-force");
    }

    @Override
    public String id() {
        return "brute-force";
    }

    @Override
    public boolean add(Document document) {

        try{
            org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
            luceneDoc.add(new TextField("name", document.getId(), Field.Store.YES));

            BytesRef bytesRef = new BytesRef(SerializationUtils.serialize(document.getVector()));
            luceneDoc.add(new StoredField("vector", bytesRef));

            this.repository.add(luceneDoc);
            return true;
        }catch (Exception e){
            LOG.error("Unexpected error",e);
            return false;
        }

    }

    @Override
    public boolean commit() {
        this.repository.commit();
        return true;
    }

    @Override
    public List<Similarity> findDuplicates(ComparisonMetric metric, AtomicInteger counter) {

        ConcurrentLinkedDeque<Similarity> pairs = new ConcurrentLinkedDeque<>();

        try{
            DirectoryReader reader = this.repository.getReader();

            IndexSearcher searcher = new IndexSearcher(reader);


            int total = reader.numDocs();

            TopDocs results = searcher.search(new MatchAllDocsQuery(), total);
            ParallelExecutor executor = new ParallelExecutor();
            for(ScoreDoc scoreDoc: results.scoreDocs){

                final int refId = scoreDoc.doc;

                double ratio = (refId * 100.0) / Double.valueOf(total);
                if (ratio % 10 == 0) LOG.info("" + ratio + "% progress");
                executor.submit(() -> {
                    try{
                        IndexReader indexReader = repository.getReader();

                        org.apache.lucene.document.Document doc = indexReader.document(refId);

                        String id = String.format(doc.get("name"));

                        BytesRef byteRef = doc.getBinaryValue("vector");
                        List<Double> vector = (List<Double>) SerializationUtils.deserialize(byteRef.bytes);

                        Document d1 = new Document(id,vector);


                        for(int i=0; i < scoreDoc.doc; i++){

                            try {
                                org.apache.lucene.document.Document doc2 = indexReader.document(i);
                                String id2 = String.format(doc2.get("name"));

                                BytesRef byteRef2 = doc2.getBinaryValue("vector");
                                List<Double> vector2 = (List<Double>) SerializationUtils.deserialize(byteRef2.bytes);

                                Document d2 = new Document(id2,vector2);

                                Double similarityScore = metric.similarity(vector, vector2);
                                counter.incrementAndGet();
                                if (similarityScore>=threshold){
                                    pairs.add(new Similarity(similarityScore,d1,d2));
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        indexReader.close();
                    }catch (Exception e){
                        LOG.error("Unexpected error",e);
                    }

                });
            }

            executor.awaitTermination(1, TimeUnit.HOURS);

            reader.close();

        }catch (Exception e){
            LOG.error("Unexpected error",e);
        }

        return pairs.stream().sorted((a,b) -> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());

    }

    @Override
    public List<Similarity> findSimilarTo(Document query, ComparisonMetric metric, int maxResults, AtomicInteger counter) {


        try{
            DirectoryReader reader = this.repository.getReader();

            IndexSearcher searcher = new IndexSearcher(reader);
            int maxDocs = reader.numDocs();
            TopDocs results = searcher.search(new MatchAllDocsQuery(), maxDocs);

            MinMaxPriorityQueue<Similarity> pairs = MinMaxPriorityQueue.orderedBy(new Similarity.ScoreComparator()).maximumSize(maxResults).create();

            for(ScoreDoc scoreDoc: results.scoreDocs){

                org.apache.lucene.document.Document doc = reader.document(scoreDoc.doc);

                String id = String.format(doc.get("name"));

//            if (id.equalsIgnoreCase(query.getId())) continue;

                BytesRef byteRef = doc.getBinaryValue("vector");
                List<Double> vector = (List<Double>) SerializationUtils.deserialize(byteRef.bytes);

                Document d1 = new Document(id,vector);

                Similarity similarity = new Similarity(metric.similarity(d1.getVector(), query.getVector()), query, d1);

                pairs.add(similarity);
                counter.incrementAndGet();
            }

            reader.close();
            return pairs.stream().sorted((a,b) -> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());

        }catch (Exception e){
            LOG.error("Unexpected error",e);
            return Collections.emptyList();
        }

    }

}
