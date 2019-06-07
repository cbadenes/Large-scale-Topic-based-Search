package experiments.survey;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MinMaxPriorityQueue;
import oeg.lstbs.data.Repository;
import oeg.lstbs.data.Stats;
import oeg.lstbs.data.TopicDistribution;
import oeg.lstbs.io.SerializationUtils;
import oeg.lstbs.io.WriterUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

    public class QueryDocuments {

    private static final Logger LOG = LoggerFactory.getLogger(QueryDocuments.class);

    private static final String CORPUS = IndexDocuments.DATASET.getCorpus().getId();

    private final Random random = new Random();


    Repository index;
    private ObjectMapper jsonMapper;

    @Before
    public void setup(){
        index       = new Repository(CORPUS+"-density");
        jsonMapper  = new ObjectMapper();
    }


    @Test
    public void execute() throws IOException {

        BufferedWriter topicsWriter = WriterUtils.to(Paths.get( "survey", CORPUS, "topics.jsonl").toFile().getAbsolutePath());
        BufferedWriter docsWriter   = WriterUtils.to(Paths.get( "survey", CORPUS, "query-docs.jsonl").toFile().getAbsolutePath());
        try{
            // Topics

            List<Map.Entry<String, Integer>> topics = index.getTermsFreqAt("hash0").entrySet().stream().sorted((a, b) -> -a.getValue().compareTo(b.getValue())).collect(Collectors.toList());

            List<String> mostRelevantTopic  = topics.stream().limit(5).map(r -> r.getKey()).collect(Collectors.toList());

            List<String> lessRelevantTopic  = topics.stream().skip(topics.size() - 5).map(r -> r.getKey()).collect(Collectors.toList());

            // Corpus
            TopDocs corpus = index.getBy(new MatchAllDocsQuery(), index.getSize());


            // Thresholds
            Stats topicDevStats = getTopicDevStats(corpus);

            Double minStdDev    = topicDevStats.getMin();

            Double maxStdDev    = topicDevStats.getMax();

            Double meanStdDev   = topicDevStats.getMean();

            Double lowerLimitStdDev = (((meanStdDev - minStdDev) / 50.0 ) * 5.0) + minStdDev;

            Double upperLimitStdDev = (((maxStdDev - meanStdDev) / 50.0 ) * 45.0) + meanStdDev;

            // Predicates

            Predicate<? super TopicDistribution> aRange = new Predicate<TopicDistribution>() {
                @Override
                public boolean test(TopicDistribution topicDistribution) {
                    Double stdev = topicDistribution.getStats().getDev();
                    return  (stdev > upperLimitStdDev);
                }
            };

            Predicate<? super TopicDistribution> bRange = new Predicate<TopicDistribution>() {
                @Override
                public boolean test(TopicDistribution topicDistribution) {
                    Double stdev = topicDistribution.getStats().getDev();
                    return  (stdev > meanStdDev) && (stdev <= upperLimitStdDev);
                }
            };

            Predicate<? super TopicDistribution> cRange = new Predicate<TopicDistribution>() {
                @Override
                public boolean test(TopicDistribution topicDistribution) {
                    Double stdev = topicDistribution.getStats().getDev();
                    return  (stdev > lowerLimitStdDev) && (stdev <= meanStdDev);
                }
            };

            Predicate<? super TopicDistribution> dRange = new Predicate<TopicDistribution>() {
                @Override
                public boolean test(TopicDistribution topicDistribution) {
                    Double stdev = topicDistribution.getStats().getDev();
                    return  (stdev <= lowerLimitStdDev);
                }
            };





            /**
             * A: Document with a sparse topic distribution from Corpus
             */
            List<TopicDistribution> docsA   = getRandomBy(aRange, corpus, 1);

            /**
             * B: Document that contains one of the common topics, with high presence of certain topics with respect to the rest
             */

            TopDocs commonTopicsSet         = getDocsBy(mostRelevantTopic);
            List<TopicDistribution> docsB   = getRandomBy(bRange, commonTopicsSet, 1);


            /**
             * C: Document that contains one of the rare topics, with low presence of certain topics with respect to the rest
             */

            TopDocs lessCommonTopicSubset   = getDocsBy(lessRelevantTopic);
            List<TopicDistribution> docsC   = getRandomBy(cRange, lessCommonTopicSubset, 1);

            /**
             * D: Document with a uniform topic distribution
             */
            List<TopicDistribution> docsD   = getRandomBy(dRange, corpus, 1);


            LOG.info("Topics:");
            topics.subList(0,10).forEach(entry -> LOG.info("\tTopic: " + entry.getKey() + " in " + entry.getValue() + " docs"));
            LOG.info("\t...");
            topics.subList(topics.size()-10,topics.size()).forEach(entry -> LOG.info("\tTopic: " + entry.getKey() + " in " + entry.getValue() + " docs"));

            topics.forEach(topic -> {
                try {
                    topicsWriter.write(jsonMapper.writeValueAsString(topic) + "\n");
                } catch (IOException e) {
                    LOG.error("Unexpected error",e);
                }
            });

            writeDocs(docsA,"A",docsWriter);
            writeDocs(docsB,"B",docsWriter);
            writeDocs(docsC,"C",docsWriter);
            writeDocs(docsD,"D",docsWriter);

            LOG.info("Corpus: ");
            LOG.info("\tsize: " + corpus.totalHits);

            LOG.info("Topic Distribution:");
            LOG.info("\tmax: " + maxStdDev);
            LOG.info("\tmin: " + minStdDev);
            LOG.info("\tmean: " + meanStdDev);
            LOG.info("\tlower limit: " + lowerLimitStdDev);
            LOG.info("\tupper limit: " + upperLimitStdDev);


            LOG.info("DocA: " + docsA);
            LOG.info("DocB: " + docsB);
            LOG.info("DocC: " + docsC);
            LOG.info("DocD: " + docsD);
        }finally {
            topicsWriter.close();
            docsWriter.close();
        }

    }


    private void writeDocs(List<TopicDistribution> dist, String label, BufferedWriter writer){
        dist.forEach(td -> {
            td.setLabel(label);
            try {
                writer.write(jsonMapper.writeValueAsString(td)+"\n");
            } catch (IOException e) {
                LOG.error("Unexpected error",e);
            }
        });
    }

    private TopDocs getDocsBy(List<String> refTopics){

        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        for(String refTopic: refTopics){
            Query termQuery             = new TermQuery(new Term("hash0",refTopic));
            BooleanClause booleanClause = new BooleanClause(termQuery, BooleanClause.Occur.SHOULD);
            booleanQuery.add(booleanClause);
        }

        return index.getBy(booleanQuery.build(), index.getSize());
    }



    private Stats getTopicDevStats(TopDocs topDocs){
        AtomicInteger counter = new AtomicInteger();
        List<Double> valList = new ArrayList<>();
        for(int i=0;i<topDocs.totalHits;i++){
            if (counter.incrementAndGet() % 100 == 0 ) LOG.info(counter.get() + " topic dists analyzed");
            ScoreDoc d = topDocs.scoreDocs[i];
            Document d1 = index.getDocument(d.doc);
            List<Double> v = (List<Double>) SerializationUtils.deserialize(d1.getBinaryValue("vector").bytes);
            valList.add(new Stats(v).getDev());
        }
        return new Stats(valList);
    }

    private List<TopicDistribution> getRandomBy(Predicate<? super TopicDistribution> predicate, TopDocs topDocs, Integer num){

        List<TopicDistribution> sample = new ArrayList<>();

        AtomicInteger counter = new AtomicInteger();
        for(int i=0;i<topDocs.totalHits;i++){
            if (counter.incrementAndGet() % 100 == 0 ) LOG.info(counter.get() + " docs analyzed");

            if (random.nextInt(Integer.MAX_VALUE) % 100 == 0){
                ScoreDoc d = topDocs.scoreDocs[i];
                Document d1 = index.getDocument(d.doc);
                List<Double> v = (List<Double>) SerializationUtils.deserialize(d1.getBinaryValue("vector").bytes);
                Stats stats = new Stats(v);
                TopicDistribution td = new TopicDistribution(stats, d1.get("id"), v);
                if (predicate.test(td)) sample.add(td);
                if (sample.size() >= num) return sample;
            }
        }

        if (sample.size() < num) return getRandomBy(predicate, topDocs, num);
        return sample;
    }
}
