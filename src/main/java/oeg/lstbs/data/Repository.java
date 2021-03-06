package oeg.lstbs.data;

import com.google.common.collect.MinMaxPriorityQueue;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.SerializationUtils;
import oeg.lstbs.metrics.ComparisonMetric;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.DelimitedTermFrequencyTokenFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Repository {

    private static final Logger LOG = LoggerFactory.getLogger(Repository.class);
    private IndexWriter writer;
    private FSDirectory directory;
    private final String id;
    private final File indexFile;
    private DirectoryReader reader;
    private AtomicInteger counter = new AtomicInteger();
    private IndexSearcher searcher;

    private static final Integer MAX_CLAUSES = 1024;

    public Repository(String id) {
        this.id = id;
        this.indexFile = Paths.get("repository",id).toFile();
    }

    public void delete(){
        if (indexFile.exists()) indexFile.delete();
    }

    public synchronized void add(String id, Map<Integer,List<String>> hashcode, List<Double> vector){
        try {
            open();
            org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
            doc.add(new StringField("id", id, Field.Store.YES));

            BytesRef bytesRef = new BytesRef(SerializationUtils.serialize(vector));
            doc.add(new StoredField("vector", bytesRef));

            for(Map.Entry<Integer,List<String>> entry: hashcode.entrySet()){
                doc.add(new TextField("hash" + entry.getKey(), entry.getValue().stream().collect(Collectors.joining(" ")), Field.Store.YES));
            }

            writer.addDocument(doc);
            if (counter.incrementAndGet() % 500 == 0 ) {
                commit();
                LOG.info("Added " + counter.get() + " documents");
            }
        } catch (Exception e) {
            LOG.warn("Error on document: '" + id + "'",e);
            throw new RuntimeException(e);
        }
    }

    public List<String> getIds(){
        List<String> idList = new ArrayList<>();
        TopDocs topDocs = getBy(new MatchAllDocsQuery(), getSize());
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for(int i=0;i<scoreDocs.length;i++){
            ScoreDoc sId = scoreDocs[i];
            Document d = getDocument(sId.doc);
            String id2 = d.get("id");
            idList.add(id2);
        }
        return idList;
    }

    public synchronized void add(String id, Map<Integer,List<String>> hashcode){
        try {
            open();
            org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
            doc.add(new StringField("id", id, Field.Store.YES));

            for(Map.Entry<Integer,List<String>> entry: hashcode.entrySet()){
                doc.add(new TextField("hash" + entry.getKey(), entry.getValue().stream().collect(Collectors.joining(" ")), Field.Store.YES));
            }

            writer.addDocument(doc);
            if (counter.incrementAndGet() % 500 == 0 ) {
                commit();
                LOG.info("Added " + counter.get() + " documents");
            }
        } catch (Exception e) {
            LOG.warn("Error on document: '" + id + "'",e);
            throw new RuntimeException(e);
        }
    }

    public synchronized void add(String id, List<Double> vector){
        try {
            open();
            org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
            doc.add(new StringField("id", id, Field.Store.YES));

            BytesRef bytesRef = new BytesRef(SerializationUtils.serialize(vector));
            doc.add(new StoredField("vector", bytesRef));

            writer.addDocument(doc);
            if (counter.incrementAndGet() % 500 == 0 ) {
                commit();
                LOG.info("Added " + counter.get() + " documents");
            }
        } catch (Exception e) {
            LOG.warn("Error on document: '" + id + "'",e);
            throw new RuntimeException(e);
        }
    }

    public synchronized void add(String id, String name, String txt){
        try {
            open();
            org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
            doc.add(new StringField("id", id, Field.Store.YES));

            doc.add(new TextField("name", name.replace("|", " "), Field.Store.YES));

            doc.add(new TextField("txt", txt.replace("|"," "), Field.Store.YES));

            writer.addDocument(doc);

            if (counter.incrementAndGet() % 500 == 0 ) {
                commit();
                LOG.info("Added " + counter.get() + " documents");
            }
        } catch (Exception e) {
            LOG.warn("Error on document: '" + id + "'",e);
            throw new RuntimeException(e);
        }
    }

    public boolean contains(String id){
        close();
        Query termQuery             = new TermQuery(new Term("id",id));
        BooleanClause booleanClause = new BooleanClause(termQuery, BooleanClause.Occur.MUST);
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.add(booleanClause);
        try {
            TopDocs topDocs = searcher.search(booleanQuery.build(),1);
            return topDocs.totalHits >0;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public int getSize(){
        return getReader().numDocs();
    }


    public Map<String,Double> getSimilarTo(List<Double> vector, Integer top, ComparisonMetric metric){

        MinMaxPriorityQueue<Similarity> pairs = MinMaxPriorityQueue.orderedBy(new Similarity.ScoreComparator()).maximumSize(top).create();
        close();
//        ParallelExecutor executor = new ParallelExecutor();
        for(int i=0;i<reader.numDocs();i++){
            Integer index  = i;
//            executor.submit(() -> {
                try {
                    Document doc = reader.document(index);
                    List<Double> v2 = (List<Double>) SerializationUtils.deserialize(doc.getBinaryValue("vector").bytes);
                    add(pairs,new Similarity(metric.similarity(vector,v2), new oeg.lstbs.data.Document(doc.get("id")),null));
                } catch (IOException e) {
                    e.printStackTrace();
                }
//            });
        }
//        executor.awaitTermination(1l, TimeUnit.HOURS);

        Map<String,Double> documents = new HashMap<>();
        while(!pairs.isEmpty()){
            Similarity s = pairs.poll();
            documents.put(s.getD1().getId(), s.getScore());
        }

        return documents;

    }

    public Map<String,Double> getSimilarToByThreshold(List<Double> vector, Double threshold, ComparisonMetric metric){

        ConcurrentHashMap<String, Double> documentMap = new ConcurrentHashMap<String, Double>();
        close();
        for(int i=0;i<reader.numDocs();i++){
            Integer index  = i;
            try {
                Document doc = reader.document(index);
                List<Double> v2 = (List<Double>) SerializationUtils.deserialize(doc.getBinaryValue("vector").bytes);
                Double score = metric.similarity(vector, v2);
                if (score > threshold){
                    oeg.lstbs.data.Document d = new oeg.lstbs.data.Document(doc.get("id"));
                    documentMap.put(d.getId(), score);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return documentMap;

    }

    public Map<String,Double> getSimilarTo(Map<Integer,List<String>> hashcode, Integer top){

        close();

        MinMaxPriorityQueue<Similarity> pairs = MinMaxPriorityQueue.orderedBy(new Similarity.ScoreComparator()).maximumSize(top).create();

        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = getSimilarToQuery(hashcode).build();

        try {
            TopDocs topDocs = searcher.search(query,reader.numDocs());
            for(int i=0;i<topDocs.totalHits;i++){
                ScoreDoc d = topDocs.scoreDocs[i];
                Document doc = getDocument(d.doc);
                Similarity similarity = new Similarity(Double.valueOf(d.score), new oeg.lstbs.data.Document(doc.get("id")),null);
                pairs.add(similarity);
            }
        } catch (IOException e) {
            LOG.error("Unexpected error",e);
        }

        Map<String,Double> documents = new HashMap<>();
        while(!pairs.isEmpty()){
            Similarity s = pairs.poll();
            documents.put(s.getD1().getId(), s.getScore());
        }

        return documents;
    }


    public Map<Integer,List<String>> getHashcodeOf(int docId, int depth){

        Map<Integer,List<String>> hashcode = new HashMap<>();

        Document doc = getDocument(docId);

        for(int i=0;i<depth;i++){

            String hash = doc.get("hash" + i);

            hashcode.put(i, Arrays.asList(hash.split(" ")));
        }

        return hashcode;
    }

    public BooleanQuery.Builder getSimilarToQuery(Map<Integer,List<String>> hashcode){
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        AtomicInteger counter = new AtomicInteger();
        for(Map.Entry<Integer,List<String>> entry : hashcode.entrySet()){
            Integer index = entry.getKey();
            for(String topic: entry.getValue()){
                for(int i=index;i<hashcode.size();i++){
                    if (counter.incrementAndGet() > MAX_CLAUSES){
                        LOG.warn("Max clauses limit reached");
                        break;
                    }
                    Integer boost = hashcode.size()-i;
                    Query termQuery             = new TermQuery(new Term("hash"+i,topic));
                    Query boostedQuery          = new BoostQuery(termQuery,boost*boost);
                    BooleanClause booleanClause = new BooleanClause(boostedQuery, BooleanClause.Occur.SHOULD);
                    booleanQuery.add(booleanClause);
                }
            }
        }
        return booleanQuery;
    }

    public Double getRatioHitsTo(Map<Integer,List<String>> hashcode){
        close();

        try {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(getSimilarToQuery(hashcode).build(), reader.numDocs());
            return (Double.valueOf(topDocs.totalHits) * 100.0) / Double.valueOf(reader.numDocs());
        } catch (IOException e) {
            LOG.error("Unexpected query error",e);
            return 100.0;
        }
    }

    public Map<String,Integer> getTermsFreqAt(String field){
        close();

        Map<String,Integer> topicsFreq = new HashMap<>();
        try {
            Terms terms = MultiFields.getTerms(reader, field);

            TermsEnum termsEnum = terms.iterator();
            BytesRef bytesRef;
            while(true){
                bytesRef = termsEnum.next();
                if (bytesRef == null) break;
                Term term = new Term(field, bytesRef);
                int freq = reader.docFreq(term);
                topicsFreq.put(term.text(),freq);
            }

        } catch (IOException e) {
            LOG.error("Unexpected query error",e);
        }
        return topicsFreq;
    }

    public Long getTotalHitsTo(Query query){
        close();
        try {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(query, reader.numDocs());
            return topDocs.totalHits;
        } catch (IOException e) {
            LOG.error("Unexpected query error",e);
            return 0l;
        }
    }


    private synchronized void add(MinMaxPriorityQueue<Similarity> queue, Similarity similarity){
        queue.add(similarity);
    }

    public void commit(){
        try {
            writer.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public org.apache.lucene.document.Document getDocument(int docId){
        try {
            close();
            return reader.document(docId);

        } catch (IOException e) {
            throw new RuntimeException("Unexpected error",e);
        }
    }


    public Map<String,Double> getMoreLikeThis(Integer docId, String[] fields, Integer max){
        Map<String,Double> related = new HashMap<>();
        try {
            close();


            MoreLikeThis mlt = new MoreLikeThis(reader);
            mlt.setFieldNames(fields);
            mlt.setMinWordLen(2);
//            mlt.setBoost(true);
            mlt.setAnalyzer(new StandardAnalyzer());
            Query q = mlt.like(docId);
            TopDocs topDocs = getBy(q, max+1);

            long limit = max+1 <= topDocs.totalHits? max+1 : topDocs.totalHits;
            for(int i=0;i<limit;i++){
                ScoreDoc d = topDocs.scoreDocs[i];
                if (d.doc == docId) continue;
                Document doc = getDocument(d.doc);
                Double score = Double.valueOf(d.score);
                related.put(doc.get("id"), score);
            }

        } catch (Exception e) {
            LOG.warn("error",e);
            throw new RuntimeException("Unexpected error",e);
        }
        return related;
    }

    public Optional<org.apache.lucene.document.Document> getDocumentBy(String id){
        try {
            Optional<Integer> dId = getDocumentIdBy(id);
            if (!dId.isPresent()) return Optional.empty();
            return Optional.of(reader.document(dId.get()));

        } catch (IOException e) {
            throw new RuntimeException("Unexpected error",e);
        }
    }

    public Optional<Integer> getDocumentIdBy(String id){
        try {
            close();
            BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
            Query termQuery             = new TermQuery(new Term("id",id));
            BooleanClause booleanClause = new BooleanClause(termQuery, BooleanClause.Occur.MUST);
            booleanQuery.add(booleanClause);

            IndexSearcher searcher = new IndexSearcher(reader);

            Query query = booleanQuery.build();


            TopDocs topDocs = searcher.search(query, reader.numDocs());

            if (topDocs.totalHits == 0){
                return Optional.empty();
            }

            return Optional.of(topDocs.scoreDocs[0].doc);

        } catch (IOException e) {
            throw new RuntimeException("Unexpected error",e);
        }
    }

    public TopDocs getBy(Query query, int max){
        try {
            close();
            return searcher.search(query, (max<0? reader.numDocs() : max));

        } catch (IOException e) {
            throw new RuntimeException("Unexpected error",e);
        }
    }

    public synchronized void close() {
            try {
                if (writer != null && writer.isOpen()) {
                    writer.commit();
                    writer.close();
                }
                if (directory == null){
                    directory = FSDirectory.open(indexFile.toPath());
                }
                if (reader == null){
                    reader = DirectoryReader.open(directory);
                    searcher  = new IndexSearcher(reader);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
    }

    public synchronized void open() {
        try {
            if (writer == null) {
                indexFile.getParentFile().mkdirs();
                if (indexFile.exists()) indexFile.delete();
                this.directory = FSDirectory.open(indexFile.toPath());
                IndexWriterConfig writerConfig = new IndexWriterConfig(new RepositoryAnalyzer());
                writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                writerConfig.setRAMBufferSizeMB(500.0);
                this.writer = new IndexWriter(directory, writerConfig);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public DirectoryReader getReader(){
        try {
            close();
            return DirectoryReader.open(directory);

        } catch (IOException e) {
            throw new RuntimeException("Unexpected error",e);
        }
    }

    public class RepositoryAnalyzer extends Analyzer {

        @Override
        protected TokenStreamComponents createComponents(String s) {
            Tokenizer tokenizer = new WhitespaceTokenizer();
            TokenFilter filters = new DelimitedTermFrequencyTokenFilter(tokenizer);
            return new TokenStreamComponents(tokenizer, filters);
        }
    }
}
