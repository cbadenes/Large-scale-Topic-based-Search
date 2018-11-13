package experiments;

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

public class SearchingExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(SearchingExperiment.class);


    Map<String,String> corpora = new HashMap<>();
    private List<ComparisonMetric> metrics;
    private List<Integer> accuracies;
    private int testingSize;
    private int trainingSize;
    private BufferedWriter tableWriter;
    private BufferedWriter evalWriter;
    private ObjectMapper jsonMapper;
    private int goldStandardTop;
    private double goldStandardThreshold;
    private int seed;

    @Before
    public void setup() throws IOException {

        this.goldStandardTop        = 100;
        this.goldStandardThreshold  = 0.0;

        this.seed           = 11;
        this.metrics        = Arrays.asList(new JSD(), new S2JSD(), new Hellinger());
        this.accuracies     = Arrays.asList(0,5,10,20);
        this.testingSize    = 100;
        this.trainingSize   = -1;
        long time = System.currentTimeMillis();
        this.tableWriter    = WriterUtils.to(Paths.get("results","searching-" + time + "-tables.md.gz").toFile().getAbsolutePath());
        this.evalWriter     = WriterUtils.to(Paths.get("results","searching-" + time + "-evaluations.jsonl.gz").toFile().getAbsolutePath());
        this.jsonMapper     = new ObjectMapper();

        // Cordis
        corpora.put("Cordis_100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ZG7TtJmbmRi9LDe/download");
        corpora.put("Cordis_300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Ac5irmmxSkdgyGw/download");
        corpora.put("Cordis_500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/tfQFqAYL9XS5NB6/download");
        corpora.put("Cordis_800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KEE3fyFkM7Wq6Zq/download");
        corpora.put("Cordis_1000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/87ega8bYMZH8T62/download");
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
        this.tableWriter.close();
        this.evalWriter.close();
    }

    @Test
    public void execute() throws IOException {

        LOG.info("Searching Test ");
        Map<String, List<Evaluation>> results = new HashMap<>();

        ConcurrentHashMap<String,Document> testSet = new ConcurrentHashMap<>();

        for(String corpusId: corpora.keySet().stream().sorted((a,b) -> Integer.valueOf(StringUtils.substringAfter(a,"_")).compareTo(Integer.valueOf(StringUtils.substringAfter(b,"_")))).collect(Collectors.toList())){

            String corpus = corpora.get(corpusId);

            // Algorithms
            BruteForceAlgorithm bruteForceAlgorithm   = new BruteForceAlgorithm();
            List<GroupsBasedAlgorithm> algorithms     = Arrays.asList(new DensityBasedAlgorithm(), new CentroidBasedAlgorithm(), new ThresholdBasedAlgorithm());

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
                        algorithms.forEach(a -> a.add(d1));
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

            LOG.info("Corpus.Size = " + counter.get());
            LOG.info("Test.Size = " + testSet.size());

            bruteForceAlgorithm.commit();
            algorithms.forEach(a -> a.commit());

            Integer maxResults          = accuracies.stream().reduce((a, b) -> (a > b) ? a : b).get();

            for(ComparisonMetric comparisonMetric: metrics){


                ParallelExecutor ex2 = new ParallelExecutor();
                ConcurrentHashMap<String,List<Evaluation>> partialEvaluations = new ConcurrentHashMap<>();
                AtomicInteger index = new AtomicInteger();
                for(String dId : testSet.keySet().stream().sorted().collect(Collectors.toList())){

                    final ComparisonMetric metric   = comparisonMetric;
                    final Document query            = testSet.get(dId);
                    ex2.submit(() -> {

                        LOG.info("Searching related docs to '" + dId+"' ["+ index.incrementAndGet()+"/"+testingSize+"] in corpus '"+corpusId+"' by using '"+metric.id()+"' as similarity metric .. ");
                        AtomicInteger maxCounter    = new AtomicInteger();
                        Instant s1 = Instant.now();
                        List<Similarity> relatedDocs = bruteForceAlgorithm.findSimilarTo(query, metric, goldStandardTop, maxCounter).stream().filter(s -> s.getScore() >= goldStandardThreshold).collect(Collectors.toList());
                        Instant e1 = Instant.now();
                        TimeUtils.print(s1,e1,"Brute-force ["+maxCounter.get()+ " comparisons] elapsed time: ");

                        for(GroupsBasedAlgorithm algorithm : algorithms){
                            List<Integer> levels = new ArrayList<Integer>();
                            switch (algorithm.id()){
                                case "density":
                                    levels = Arrays.asList(1,3,5);
                                    break;
                                case "centroid":
                                    levels = Arrays.asList(1,3,5);
                                    break;
                                case "threshold":
                                    levels = Arrays.asList(1);
                                    break;
                            }

                            for(Integer level: levels){
                                algorithm.setLevel(level);
                                List<Evaluation> algorithmEvaluations = evaluateAlgorithm(algorithm, query, metric, maxResults, relatedDocs, maxCounter, corpusId);
                                for(Evaluation eval : algorithmEvaluations){
                                    if (!partialEvaluations.containsKey(eval.getAlgorithm())){
                                        partialEvaluations.put(eval.getAlgorithm(),new ArrayList<>());
                                    }
                                    partialEvaluations.get(eval.getAlgorithm()).add(eval);
                                }
                            }


                        }
                    });


                }
                ex2.awaitTermination(1, TimeUnit.HOURS);

                for(String algorithm : partialEvaluations.keySet()){


                    List<Evaluation> testSetEvaluations = partialEvaluations.get(algorithm);

                    if (testSetEvaluations.isEmpty()) continue;

                    Evaluation evaluation = new Evaluation();
                    evaluation.setAveragePrecision(new Stats(testSetEvaluations.stream().filter(e -> e!=null).map(e -> e.getAveragePrecision()).collect(Collectors.toList())).getMean());
                    evaluation.setPrecision(new Stats(testSetEvaluations.stream().map(e -> e.getPrecision()).collect(Collectors.toList())).getMean());
                    evaluation.setRecall(new Stats(testSetEvaluations.stream().map(e -> e.getRecall()).collect(Collectors.toList())).getMean());
                    evaluation.setEfficiency(new Stats(testSetEvaluations.stream().map(e -> e.getEfficiency()).collect(Collectors.toList())).getMean());
                    evaluation.setAveragePrecision(new Stats(testSetEvaluations.stream().map(e -> e.getAveragePrecision()).collect(Collectors.toList())).getMean());
                    evaluation.setElapsedTime(Double.valueOf(new Stats(testSetEvaluations.stream().map(e -> Double.valueOf(e.getElapsedTime())).collect(Collectors.toList())).getMean()).longValue());
                    evaluation.setAlgorithm(testSetEvaluations.get(0).getAlgorithm());
                    evaluation.setDescription(testSetEvaluations.get(0).getDescription());
                    evaluation.setCorpus(testSetEvaluations.get(0).getCorpus());
                    evaluation.setMetric(testSetEvaluations.get(0).getMetric());
                    evaluation.setModel(testSetEvaluations.get(0).getModel());

                    String id = evaluation.getMetric()+"-"+evaluation.getCorpus();
                    if (!results.containsKey(id)){
                        results.put(id,new ArrayList<>());
                    }
                    results.get(id).add(evaluation);
                    LOG.info("Report created: " + evaluation);
                }

            }

        }

        LOG.info("Creating result tables ..");

        for(String tableName : results.keySet().stream().sorted().collect(Collectors.toList())){
            createTable(tableName+"-mAP", results.get(tableName), algorithm -> algorithm.contains("@0"), eval -> String.valueOf(eval.getAveragePrecision()));
            createTable(tableName+"-efficiency", results.get(tableName), algorithm -> algorithm.contains("@0"), eval -> String.valueOf(eval.getEfficiency()));
            for(Integer accuracy: accuracies.stream().filter(a -> a>0).sorted().collect(Collectors.toList())){
                createTable(tableName+"-fMeasure@"+accuracy, results.get(tableName), algorithm -> algorithm.contains("@"+accuracy), eval -> String.valueOf(eval.getFMeasure()));
                createTable(tableName+"-precision@"+accuracy, results.get(tableName), algorithm -> algorithm.contains("@"+accuracy), eval -> String.valueOf(eval.getPrecision()));
                createTable(tableName+"-recall@"+accuracy, results.get(tableName), algorithm -> algorithm.contains("@"+accuracy), eval -> String.valueOf(eval.getRecall()));
            }
        }

    }


    private List<Evaluation> evaluateAlgorithm (GroupsBasedAlgorithm algorithm, Document query, ComparisonMetric metric, Integer maxResults, List<Similarity> relatedDocs, AtomicInteger maxCounter, String corpusId){
        AtomicInteger dCounter = new AtomicInteger();
        Instant dS1 = Instant.now();
        List<Similarity> algorithmRelatedDocs   = algorithm.findSimilarTo(query, metric, relatedDocs.size()*5, dCounter);
        Instant dE1 = Instant.now();

        // -> Accuracies
        List<Evaluation> evaluations = new ArrayList<>();
        for(Integer accuracy : accuracies){
            try {
                Evaluation evaluation = new Evaluation();
                List<String> relevantList = (accuracy > 0)?
                        relatedDocs.stream().map(s -> s.getD2().getId()).collect(Collectors.toList()) :
                        relatedDocs.stream().map(s -> s.getD2().getId()).limit(5).collect(Collectors.toList()); // mAP@5
                List<String> retrieveList = (accuracy > 0)?
                        algorithmRelatedDocs.stream().map(s -> s.getD2().getId()).limit(accuracy).collect(Collectors.toList()) :
                        algorithmRelatedDocs.stream().map(s -> s.getD2().getId()).collect(Collectors.toList());
                evaluation.addResult(relevantList, retrieveList);
                evaluation.setCorpus(StringUtils.substringBefore(corpusId,"_"));
                evaluation.setModel(StringUtils.substringAfter(corpusId,"_"));
                evaluation.setAlgorithm(algorithm.getName() + "@" + accuracy);
                evaluation.setMetric(metric.id());
                evaluation.setAveragePrecision(AveragePrecision.from(relevantList, retrieveList));
                evaluation.setEfficiency(1.0 - (Double.valueOf(dCounter.get()) / Double.valueOf(maxCounter.get())));
                evaluation.setTime(dS1,dE1);
                evaluations.add(evaluation);
                evalWriter.write(jsonMapper.writeValueAsString(evaluation)+"\n");
            } catch (Exception e) {
                LOG.warn("Error creating evaluation",e);
            }
        }
        return evaluations;
    }


    private void createTable(String name, List<Evaluation> evals, Predicate<String> filter, Function<Evaluation, String> value) throws IOException {

        Map<Integer,List<String>> table = new HashMap<>();

        int columnId = 0;
        table.put(columnId++,Arrays.asList("topics","100","300","500","800","1000"));

        // group by algorithm
        Map<String, List<Evaluation>> algEvals = evals.stream().collect(Collectors.groupingBy(Evaluation::getAlgorithm));

        for(String alg : algEvals.keySet().stream().filter(filter).sorted().collect(Collectors.toList())){

            List<String> column = new ArrayList<>();
            column.add(StringUtils.substringBefore(alg,"@"));
            column.addAll(algEvals.get(alg).stream().map(value).collect(Collectors.toList()));
            table.put(columnId++,column);
        }

        printTable(name, table);
    }


    private void printTable(String name, Map<Integer,List<String>> table) throws IOException {
        System.out.println("#"+name);
        tableWriter.write("#"+name+"\n");
        for( int i=0; i< table.get(0).size();i++){

            final int index = i;
            String rowString = table.entrySet().stream().sorted((a, b) -> a.getKey().compareTo(b.getKey())).map(entry -> entry.getValue().get(index)).collect(Collectors.joining("\t"));
            System.out.println(rowString);
            tableWriter.write(rowString+"\n");
        }
    }

}
