package oeg.lstbs.data;

import com.google.common.collect.MinMaxPriorityQueue;
import oeg.lstbs.algorithms.BruteForceAlgorithm;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.io.SerializationUtils;
import oeg.lstbs.metrics.JSD;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Rater {

    private static final Logger LOG = LoggerFactory.getLogger(Rater.class);

    private final Corpus corpus;
    private final LuceneRepository repository;
    private Map<String,Integer> categories = new HashMap<>();


    public Rater(Corpus corpus, Integer size) {
        this.corpus = corpus;
        this.repository = new LuceneRepository("gold-"+corpus.getId());
        try{
            BufferedReader reader = ReaderUtils.from(corpus.getPath());
            String row;
            AtomicInteger counter = new AtomicInteger();
            while((row = reader.readLine()) != null){
                String[] values = row.split(",");
                String dId = values[0];
                List<Double> vector = Arrays.stream(values).skip(1).mapToDouble(v -> Double.valueOf(v)).boxed().collect(Collectors.toList());
                final Document document = new Document(dId, vector);

                org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
                luceneDoc.add(new TextField("name", document.getId(), Field.Store.YES));

                BytesRef bytesRef = new BytesRef(SerializationUtils.serialize(document.getVector()));
                luceneDoc.add(new StoredField("vector", bytesRef));

                this.repository.add(luceneDoc);

                if (counter.incrementAndGet() % (size >10? size/10 : (size<0)? 1000 : size) == 0) LOG.info("Added " + counter.get() + " documents to Annotator: '" + corpus.getId());
                if ((size> 0) && (counter.get() >= size)) break;
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        this.repository.commit();
    }

    public Integer getSize(){
        return this.repository.getSize();
    }

    public String getId(){
        return this.corpus.getId();
    }

    public void add(String document, int limit) {

        JSD metric = new JSD();

        try{
            DirectoryReader reader = this.repository.getReader();

            IndexSearcher searcher = new IndexSearcher(reader);

            Query nameQuery = new QueryParser("name",new KeywordAnalyzer()).parse(document);
            TopDocs result = searcher.search(nameQuery, 1);

            org.apache.lucene.document.Document luceneDoc = reader.document(result.scoreDocs[0].doc);

            List<Double> v1 = (List<Double>) SerializationUtils.deserialize(luceneDoc.getBinaryValue("vector").bytes);

            TopDocs results = searcher.search(new MatchAllDocsQuery(), reader.numDocs());

            MinMaxPriorityQueue<Similarity> pairs = MinMaxPriorityQueue.orderedBy(new Similarity.ScoreComparator()).maximumSize(limit).create();

            for(ScoreDoc scoreDoc: results.scoreDocs){

                org.apache.lucene.document.Document doc = reader.document(scoreDoc.doc);

                String id2 = String.format(doc.get("name"));

                BytesRef byteRef = doc.getBinaryValue("vector");
                List<Double> v2 = (List<Double>) SerializationUtils.deserialize(byteRef.bytes);

                Similarity similarity = new Similarity(metric.similarity(v1, v2), new Document(document), new Document(id2));

                pairs.add(similarity);
            }

            reader.close();
            pairs.forEach(pair -> categories.put(pair.getD1().getId()+"-"+pair.getD2().getId(),1));

        }catch (Exception e){
            LOG.error("Unexpected error",e);
        }
    }

    public void add(List<String> pairs){
        for (String pair:pairs){
            Integer category = categories.containsKey(pair)? 1 : 0;
            categories.put(pair,category);
        }
    }

    public Set<String> getPairs(){
        return categories.keySet();
    }

    public Integer rate(String pair){
        if (!categories.containsKey(pair)) return 0;
        return categories.get(pair);
    }

    public List<String> getByCategory(Integer category){
        return categories.entrySet().stream().filter(e -> e.getValue() == category).map(e -> e.getKey()).collect(Collectors.toList());
    }
}
