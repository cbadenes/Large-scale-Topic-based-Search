package oeg.lstbs.algorithms;

import com.google.common.collect.MinMaxPriorityQueue;
import oeg.lstbs.data.*;
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public abstract class GroupsBasedAlgorithm implements Explorer {

    private static final Logger LOG = LoggerFactory.getLogger(GroupsBasedAlgorithm.class);

    private final LuceneRepository repository;
    private final int maxGroups;

    public GroupsBasedAlgorithm(String id, int maxGroups) {
        this.repository = new LuceneRepository(id);
        this.maxGroups = maxGroups;
    }

    protected abstract List<TopicPoint> getGroups(List<Double> vector);

    @Override
    public boolean add(Document document) {

        List<TopicPoint> groups = getGroups(document.getVector());

        TopicSummary topicSummary = new TopicSummary(groups);

        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();

        luceneDoc.add(new TextField("name", document.getId(), Field.Store.YES));

        BytesRef bytesRef = new BytesRef(SerializationUtils.serialize(document.getVector()));
        luceneDoc.add(new StoredField("vector", bytesRef));

        for (int i = 0; i <= maxGroups; i++) {

            int tail = i;

            luceneDoc.add(new StringField("hashcodeR" + tail, ""+ topicSummary.getReducedHashCodeBy(tail), Field.Store.YES));

            luceneDoc.add(new StringField("hashexpR" + tail, "" + topicSummary.getReducedHashTopicsBy(tail), Field.Store.YES));

            int top = (maxGroups - i);

            luceneDoc.add(new StringField("hashcodeT" + top, "" +topicSummary.getTopHashCodeBy(top), Field.Store.YES));

            luceneDoc.add(new StringField("hashexpT" + top, "" + topicSummary.getTopHashTopicsBy(top), Field.Store.YES));
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
    public List<Similarity> findDuplicates(ComparisonMetric metric, int maxResults) {

        List<Similarity> duplicates = getDuplicatesByField("hashcodeR0", metric, maxResults);
        return duplicates;
    }

    private List<Similarity> getDuplicatesByField(String fieldName, ComparisonMetric metric, int maxResults) {
        MinMaxPriorityQueue<Similarity> pairs = MinMaxPriorityQueue.orderedBy(new Similarity.ScoreComparator()).maximumSize(maxResults).create();
        IndexReader reader = repository.getReader();
        TermStats[] commonTerms;
        try {
            HighFreqTerms.DocFreqComparator cmp = new HighFreqTerms.DocFreqComparator();
            commonTerms = HighFreqTerms.getHighFreqTerms(reader, 100, fieldName, cmp);
            for (TermStats commonTerm : commonTerms) {

                if (commonTerm.docFreq < 2) break;

                String val = commonTerm.termtext.utf8ToString();

                List<Document> docs = new ArrayList<>();

                TermQuery query = new TermQuery(new Term(fieldName, val));
                TopDocs results = repository.getBy(query, repository.getSize());


                for (ScoreDoc scoreDoc : results.scoreDocs) {
                    org.apache.lucene.document.Document doc = repository.getDocument(scoreDoc.doc);

                    String id = String.format(doc.get("name"));

                    BytesRef byteRef = doc.getBinaryValue("vector");
                    List<Double> vector = (List<Double>) SerializationUtils.deserialize(byteRef.bytes);

                    Document d1 = new Document(id, vector);

                    for (Document d2 : docs) {
                        pairs.add(new Similarity(metric.similarity(d1.getVector(), d2.getVector()), d1, d2));
                    }

                    docs.add(d1);
                }


            }
            return pairs.stream().sorted((a, b) -> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Similarity> findSimilarTo(Document query, ComparisonMetric metric, int maxResults) {
        return null;
    }

}