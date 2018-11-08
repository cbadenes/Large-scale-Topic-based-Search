package oeg.lstbs.algorithms;

import com.google.common.collect.MinMaxPriorityQueue;
import oeg.lstbs.data.*;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.SerializationUtils;
import oeg.lstbs.metrics.ComparisonMetric;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public abstract class GroupsBasedAlgorithm implements Explorer {

    private static final Logger LOG = LoggerFactory.getLogger(GroupsBasedAlgorithm.class);

    protected final LuceneRepository repository;
    private final int maxGroups;
    private int level;

    public GroupsBasedAlgorithm(String id, int maxGroups, int level) {
        this.repository = new LuceneRepository(id);
        this.maxGroups = maxGroups;
        this.level = level;
    }

    public abstract List<TopicPoint> getGroups(List<Double> vector);

    @Override
    public boolean add(Document document) {

        List<TopicPoint> groups = getGroups(document.getVector());

        TopicSummary topicSummary = new TopicSummary(groups);

        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();



        luceneDoc.add(new TextField("name", document.getId(), Field.Store.YES));

        BytesRef bytesRef = new BytesRef(SerializationUtils.serialize(document.getVector()));
        luceneDoc.add(new StoredField("vector", bytesRef));

        for (int i = 1; i <= maxGroups; i++) {

            int tail = i;

            luceneDoc.add(new StringField("hashcodeR" + tail, ""+ topicSummary.getReducedHashCodeBy(tail), Field.Store.YES));

            luceneDoc.add(new TextField("hashexpR" + tail, "" + topicSummary.getReducedHashTopicsBy(tail).replace("#"," "), Field.Store.YES));

            int top = (maxGroups - i);

            luceneDoc.add(new StringField("hashcodeT" + top, "" +topicSummary.getTopHashCodeBy(top), Field.Store.YES));

            luceneDoc.add(new TextField("hashexpT" + top, "" + topicSummary.getTopHashTopicsBy(top).replace("#"," "), Field.Store.YES));
        }

        this.repository.add(luceneDoc);

        return false;
    }


    @Override
    public boolean commit() {
        this.repository.commit();
        return true;
    }

    @Override
    public List<Similarity> findDuplicates(ComparisonMetric metric, AtomicInteger counter) {

        final String fieldName = "hashcodeR"+level;
        ConcurrentLinkedDeque<Similarity> pairs = new ConcurrentLinkedDeque<>();
        TermStats[] commonTerms;
        try {
            IndexReader reader = repository.getReader();
            final int maxQuery = reader.numDocs();
            HighFreqTerms.DocFreqComparator cmp = new HighFreqTerms.DocFreqComparator();
            commonTerms = HighFreqTerms.getHighFreqTerms(reader, 100, fieldName, cmp);
            ParallelExecutor executor = new ParallelExecutor();

            for (TermStats commonTerm : commonTerms) {

                if (commonTerm.docFreq < 2) break;

                final String val = commonTerm.termtext.utf8ToString();

                executor.submit(() -> {
                    try{

                        IndexSearcher searcher = new IndexSearcher(reader);
                        List<Document> docs = new ArrayList<>();

                        TermQuery query = new TermQuery(new Term(fieldName, val));

                        TopDocs results = searcher.search(query, maxQuery);

                        for (ScoreDoc scoreDoc : results.scoreDocs) {
                            org.apache.lucene.document.Document doc = reader.document(scoreDoc.doc);

                            String id = String.format(doc.get("name"));

                            BytesRef byteRef = doc.getBinaryValue("vector");
                            List<Double> vector = (List<Double>) SerializationUtils.deserialize(byteRef.bytes);

                            Document d1 = new Document(id, vector);

                            for (Document d2 : docs) {
                                counter.incrementAndGet();
                                pairs.add(new Similarity(metric.similarity(d1.getVector(), d2.getVector()), d1, d2));
                            }

                            docs.add(d1);
                        }
                    }catch (Exception e){
                        LOG.error("Unexpected error",e);
                    }
                });

            }
            executor.awaitTermination(1, TimeUnit.HOURS);
            reader.close();
            return pairs.stream().sorted((a, b) -> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    public void setLevel(int level){
        this.level = level;
    }

    @Override
    public List<Similarity> findSimilarTo(Document query, ComparisonMetric metric, int maxResults, AtomicInteger counter) {
        return null;
    }

}