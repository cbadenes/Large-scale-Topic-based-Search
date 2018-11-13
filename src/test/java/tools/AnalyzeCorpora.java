package tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import oeg.lstbs.algorithms.*;
import oeg.lstbs.data.*;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.io.TimeUtils;
import oeg.lstbs.io.WriterUtils;
import oeg.lstbs.metrics.ComparisonMetric;
import oeg.lstbs.metrics.Hellinger;
import oeg.lstbs.metrics.JSD;
import oeg.lstbs.metrics.S2JSD;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class AnalyzeCorpora {

    private static final Logger LOG = LoggerFactory.getLogger(AnalyzeCorpora.class);


    Map<String,String> corpora = new HashMap<>();
    private List<ComparisonMetric> metrics;
    private int testingSize;
    private int trainingSize;
    private int goldStandardTop;
    private double goldStandardThreshold;

    @Before
    public void setup() throws IOException {

        this.goldStandardTop        = 100;
        this.goldStandardThreshold  = 0.9;

        this.metrics        = Arrays.asList(new JSD()); //Arrays.asList(new JSD(), new S2JSD(), new Hellinger());
        this.testingSize    = 10;
        this.trainingSize   = 10000;

        // Cordis
//        corpora.put("Cordis_100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ZG7TtJmbmRi9LDe/download");
//        corpora.put("Cordis_300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Ac5irmmxSkdgyGw/download");
//        corpora.put("Cordis_500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/tfQFqAYL9XS5NB6/download");
//        corpora.put("Cordis_800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KEE3fyFkM7Wq6Zq/download");
//        corpora.put("Cordis_1000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/87ega8bYMZH8T62/download");
//
//        // OpenResearchCorpus
        corpora.put("OpenResearch_100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download");
        corpora.put("OpenResearch_300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download");
        corpora.put("OpenResearch_500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download");
        corpora.put("OpenResearch_800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download");
        corpora.put("OpenResearch_1000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download");

        // Wikipedia
//        corpora.put("Wikipedia_100","");
        //        corpora.put("Wikipedia_300","");
        //        corpora.put("Wikipedia_500","");
        //        corpora.put("Wikipedia_800","");
        //        corpora.put("Wikipedia_1000","");


        // Patstat
//        corpora.put("Patstat_100","");
        //        corpora.put("Patstat_300","");
        //        corpora.put("Patstat_500","");
        //        corpora.put("Patstat_800","");

    }

    @After
    public void close() throws IOException {
    }

    @Test
    public void execute() throws IOException, InterruptedException {


        ConcurrentHashMap<String,Document> testSet = new ConcurrentHashMap<>();
        for(String corpusId: corpora.keySet().stream().sorted((a,b) -> Integer.valueOf(StringUtils.substringAfter(a,"_")).compareTo(Integer.valueOf(StringUtils.substringAfter(b,"_")))).collect(Collectors.toList())){

            LOG.info("Analysis of " + corpusId);

            String corpus = corpora.get(corpusId);

            // Algorithms
            BruteForceAlgorithm bruteForceAlgorithm   = new BruteForceAlgorithm();

            Integer seed = 11;

            LOG.info("Creating training/test sets from corpus '"+corpusId+"' .. ");

            ParallelExecutor exec1 = new ParallelExecutor();
            BufferedReader reader = ReaderUtils.from(corpus);
            String row;
            AtomicInteger counter = new AtomicInteger();
            while((row = reader.readLine()) != null){
                String[] values = row.split(",");
                String id = values[0];
                List<Double> vector = Arrays.stream(values).skip(1).mapToDouble(v -> Double.valueOf(v)).boxed().collect(Collectors.toList());
                final Document d1 = new Document(id, vector);

                exec1.submit(() -> {
                    try{
                        bruteForceAlgorithm.add(d1);
                    }catch(Exception e){
                        LOG.error("Unexpected error",e);
                    }
                });

                if (counter.incrementAndGet() % (trainingSize >10? trainingSize /10 : (trainingSize <0)? 1000 : trainingSize) == 0) LOG.info("Added " + counter.get() + " documents from: " + corpusId);
                if (testSet.containsKey(d1.getId())){
                    testSet.put(d1.getId(),d1);
                }
                if ((testSet.size() < testingSize) && (counter.get() % ((seed < testingSize)? seed : 2) == 0)) {
                    testSet.put(d1.getId(),d1);
                }
                if ((trainingSize > 0) && (counter.get() >= trainingSize)) break;
            }
            exec1.awaitTermination(1, TimeUnit.HOURS);
            exec1.shutdown();

            LOG.info("Corpus.Size = " + counter.get());
            LOG.info("Test.Size = " + testSet.size());

            bruteForceAlgorithm.commit();

            for(ComparisonMetric comparisonMetric: metrics){
                LOG.info("Creating test-set by '" + comparisonMetric.id()+"' from "+ testSet.size() + " test documents ..");

                ParallelExecutor ex2 = new ParallelExecutor();
                for(String dId : testSet.keySet().stream().sorted().collect(Collectors.toList())){

                    final ComparisonMetric metric   = comparisonMetric;
                    final Document query            = testSet.get(dId);
                    ex2.submit(() -> {
                        try{
                            AtomicInteger maxCounter    = new AtomicInteger();
                            List<Similarity> relatedDocs = bruteForceAlgorithm.findSimilarTo(query, metric, goldStandardTop, maxCounter).stream().collect(Collectors.toList());
                            String description = relatedDocs.size() + "docs["+relatedDocs.get(0).getScore()+"<->"+relatedDocs.get(relatedDocs.size()-1).getScore()+"]";
                            System.out.println("- " + query.getId() + ": " + description);
                        }catch (Exception e){
                            LOG.error("Unexpected error",e);
                        }
                    });


                }
                LOG.info("Waiting...");
                ex2.awaitTermination(1,TimeUnit.HOURS);
                LOG.info("completed!");

            }

        }
    }

}
