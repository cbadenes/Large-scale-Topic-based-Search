package experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import oeg.lstbs.algorithms.*;
import oeg.lstbs.data.*;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.io.WriterUtils;
import oeg.lstbs.metrics.Hellinger;
import oeg.lstbs.metrics.JSD;
import oeg.lstbs.metrics.ComparisonMetric;
import oeg.lstbs.metrics.S2JSD;
import org.apache.commons.lang.StringUtils;
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

public class DuplicatesExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(DuplicatesExperiment.class);

    private BufferedWriter tableWriter;
    private BufferedWriter evalWriter;

    Map<String,String> corpora = new HashMap<>();
    private List<ComparisonMetric> metrics;
    private List<Integer> accuracies;
    private int sampleSize;
    private int partitions;
    private double threshold;
    private ObjectMapper jsonMapper;

    @Before
    public void setup() throws IOException {

        this.metrics        = Arrays.asList(new JSD(), new S2JSD(), new Hellinger());
        this.accuracies     = Arrays.asList(0,5,10,20);
        this.sampleSize     = 10000;
        this.partitions     = 1;
        this.threshold      = 0.95;
        this.jsonMapper     = new ObjectMapper();

        long time           = System.currentTimeMillis();
        this.tableWriter    = WriterUtils.to(Paths.get("results","duplicates-"+String.valueOf(threshold).replace(".","_")+"-" + time + "-tables.md.gz").toFile().getAbsolutePath());
        this.evalWriter     = WriterUtils.to(Paths.get("results","duplicates-"+String.valueOf(threshold).replace(".","_")+"-" + time + "-evaluations.jsonl.gz").toFile().getAbsolutePath());



        // Cordis
        corpora.put("Cordis_100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ZG7TtJmbmRi9LDe/download");
        corpora.put("Cordis_300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Ac5irmmxSkdgyGw/download");
        corpora.put("Cordis_500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/tfQFqAYL9XS5NB6/download");
        corpora.put("Cordis_800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KEE3fyFkM7Wq6Zq/download");
        corpora.put("Cordis_1000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/87ega8bYMZH8T62/download");

        // OpenResearchCorpus
        corpora.put("OpenResearch_100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download");
        corpora.put("OpenResearch_300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download");
        corpora.put("OpenResearch_500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download");
        corpora.put("OpenResearch_800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download");
        corpora.put("OpenResearch_1000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download");

    }


    @Test
    public void execute() throws IOException {

        LOG.info("Duplicate Detection Test with threshold=" + threshold);

        Map<String, List<Evaluation>> results = new HashMap<>();

        for(String corpusId: corpora.keySet().stream().sorted().collect(Collectors.toList())){

            String corpus = corpora.get(corpusId);
            int iterationsCompleted = 0;
            for(int i = 0; i< 100; i++){
                if (iterationsCompleted >= partitions) break;

                int iteration = i;

                // Algorithms
                BruteForceAlgorithm bruteForceAlgorithm             = new BruteForceAlgorithm(threshold);
                List<GroupsBasedAlgorithm> algorithms               = Arrays.asList(new DensityBasedAlgorithm(), new CentroidBasedAlgorithm(), new ThresholdBasedAlgorithm());


                LOG.info("Reading topic distributions in iteration '"+iteration+"' of corpus '"+corpusId+"' .. ");
                ParallelExecutor exec1 = new ParallelExecutor();
                BufferedReader reader = ReaderUtils.from(corpus);
                String row;
                AtomicInteger counter = new AtomicInteger();
                AtomicInteger offsetCounter = new AtomicInteger();
                int offset = i*sampleSize;
                while((row = reader.readLine()) != null){
                    if (offsetCounter.incrementAndGet() < offset) continue;
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

                    if (counter.incrementAndGet() % (sampleSize/10) == 0) LOG.info("Added " + counter.get() + " documents");
                    if (counter.get() >= sampleSize) break;
                }
                exec1.awaitTermination(1, TimeUnit.HOURS);

                LOG.info("Corpus.Size = " + counter.get());

                bruteForceAlgorithm.commit();
                algorithms.forEach(a -> a.commit());

                for(ComparisonMetric metric: metrics){

                    ConcurrentHashMap<String,List<Evaluation>> partialEvaluations = new ConcurrentHashMap<>();

                    LOG.info("Finding duplicates ("+iteration+"/100) by using '"+metric.id()+"' as similarity metric in " + corpusId + ".. ");
                    // -> Brute Force Approach
                    AtomicInteger maxCounter = new AtomicInteger();
                    List<Similarity> duplicates = bruteForceAlgorithm.findDuplicates(metric, maxCounter);
//                    if (duplicates.isEmpty()){
//                        LOG.warn("No duplicates found in corpus");
//                        continue;
//                    }
                    iterationsCompleted++;
                    LOG.info("Found " + duplicates.size() + " duplicates!");
//                    LOG.info("First Duplicated Pair: " + duplicates.get(0));
//                    LOG.info("Last Duplicated Pair: " + duplicates.get(duplicates.size()-1));

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
                            List<Evaluation> algorithmEvaluations = evaluateAlgorithm(algorithm, metric, duplicates, maxCounter, corpusId);
                            for(Evaluation eval : algorithmEvaluations){
                                if (!partialEvaluations.containsKey(eval.getAlgorithm())){
                                    partialEvaluations.put(eval.getAlgorithm(),new ArrayList<>());
                                }
                                partialEvaluations.get(eval.getAlgorithm()).add(eval);
                            }
                        }

                    }

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
                        evaluation.setTotalHits(testSetEvaluations.stream().map(e -> e.getTotalHits()).collect(Collectors.toList()));


                        String id = evaluation.getMetric()+"-"+evaluation.getCorpus();
                        if (!results.containsKey(id)){
                            results.put(id,new ArrayList<>());
                        }
                        results.get(id).add(evaluation);
                        LOG.info("Report created: " + evaluation);
                    }

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


    private List<Evaluation> evaluateAlgorithm (GroupsBasedAlgorithm algorithm, ComparisonMetric metric, List<Similarity> relatedDocs, AtomicInteger maxCounter, String corpusId){
        AtomicInteger dCounter = new AtomicInteger();
        Instant dS1 = Instant.now();
        List<Similarity> retrievedDocs   = algorithm.findDuplicates(metric, dCounter);
        Instant dE1 = Instant.now();

        // -> Accuracies
        List<Evaluation> evaluations = new ArrayList<>();
        for(Integer accuracy : accuracies){
            try {
                Evaluation evaluation = new Evaluation();
                List<String> relevantList = (accuracy > 0)?
                        relatedDocs.stream().map(s -> s.getPair()).collect(Collectors.toList()) :
                        relatedDocs.stream().map(s -> s.getPair()).limit(5).collect(Collectors.toList()); // mAP@5
                List<String> retrieveList = (accuracy > 0)?
                        retrievedDocs.stream().map(s -> s.getPair()).limit(accuracy).collect(Collectors.toList()) :
                        retrievedDocs.stream().map(s -> s.getPair()).collect(Collectors.toList());
                evaluation.addResult(relevantList, retrieveList);
                evaluation.setCorpus(StringUtils.substringBefore(corpusId,"_"));
                evaluation.setModel(StringUtils.substringAfter(corpusId,"_"));
                evaluation.setAlgorithm(algorithm.getName() + "@" + accuracy);
                evaluation.setMetric(metric.id());
                evaluation.setAveragePrecision(AveragePrecision.from(relevantList, retrieveList));
                evaluation.setEfficiency(1.0 - (Double.valueOf(dCounter.get()) / Double.valueOf(maxCounter.get())));
                evaluation.setTime(dS1,dE1);
                evaluation.setTotalHits(Arrays.asList(retrieveList.size()+"/"+relevantList.size()));
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
