package experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import oeg.lstbs.algorithms.BruteForceAlgorithm;
import oeg.lstbs.algorithms.CentroidBasedAlgorithm;
import oeg.lstbs.algorithms.DensityBasedAlgorithm;
import oeg.lstbs.algorithms.ThresholdBasedAlgorithm;
import oeg.lstbs.data.*;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.io.WriterUtils;
import oeg.lstbs.metrics.Hellinger;
import oeg.lstbs.metrics.JSD;
import oeg.lstbs.metrics.ComparisonMetric;
import oeg.lstbs.metrics.S2JSD;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class DuplicatesExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(DuplicatesExperiment.class);


    Map<String,String> corpora = new HashMap<>();
    private List<ComparisonMetric> metrics;
    private List<Integer> accuracies;
    private int sampleSize;
    private int iterations;
    private double threshold;

    @Before
    public void setup(){

        this.metrics        = Arrays.asList(new JSD(), new S2JSD(), new Hellinger());
        this.accuracies     = Arrays.asList(5,10,20);
        this.sampleSize     = 10000;
        this.iterations     = 10;
        this.threshold      = 0.95;


        // Cordis
        corpora.put("Cordis100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ZG7TtJmbmRi9LDe/download");
        corpora.put("Cordis300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Ac5irmmxSkdgyGw/download");
        corpora.put("Cordis500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/tfQFqAYL9XS5NB6/download");
        corpora.put("Cordis800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KEE3fyFkM7Wq6Zq/download");
        corpora.put("Cordis1000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/87ega8bYMZH8T62/download");

        // OpenResearchCorpus
        corpora.put("OpenResearch100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download");
        corpora.put("OpenResearch300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download");
        corpora.put("OpenResearch500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download");
        corpora.put("OpenResearch800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download");
        corpora.put("OpenResearch10000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download");

        // Wikipedia
//        corpora.put("Wikipedia100","");
        //        corpora.put("Wikipedia300","");
        //        corpora.put("Wikipedia500","");
        //        corpora.put("Wikipedia800","");
        //        corpora.put("Wikipedia1000","");


        // Patstat
//        corpora.put("Patstat100","");
        //        corpora.put("Patstat300","");
        //        corpora.put("Patstat500","");
        //        corpora.put("Patstat800","");

    }


    @Test
    public void execute() throws IOException {

        BufferedWriter writer = WriterUtils.to(Paths.get("results-duplicates-" + System.currentTimeMillis() + ".jsonl.gz").toFile().getAbsolutePath());
        ObjectMapper jsonMapper = new ObjectMapper();
        try{
            LOG.info("Duplicate Detection Test with threshold=" + threshold);
            for(String corpusId: corpora.keySet().stream().sorted().collect(Collectors.toList())){


                // 10 random training/test partitions. For each partition, we randomly select 1k articles

                String corpus = corpora.get(corpusId);
                Map<String,List<Evaluation>> results = new HashMap<>();

                for(int i=0;i<iterations;i++){
                    int iteration = i;

                    // Algorithms
                    BruteForceAlgorithm bruteForceAlgorithm             = new BruteForceAlgorithm(threshold);
                    DensityBasedAlgorithm densityBasedAlgorithm         = new DensityBasedAlgorithm();
                    CentroidBasedAlgorithm centroidBasedAlgorithm       = new CentroidBasedAlgorithm();
                    ThresholdBasedAlgorithm thresholdBasedAlgorithm     = new ThresholdBasedAlgorithm();

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
                                densityBasedAlgorithm.add(d1);
                                thresholdBasedAlgorithm.add(d1);
                                centroidBasedAlgorithm.add(d1);
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
                    densityBasedAlgorithm.commit();
                    thresholdBasedAlgorithm.commit();

                    for(ComparisonMetric metric: metrics){

                        String algorithmId;

                        LOG.info("Finding duplicates by using '"+metric.id()+"' as similarity metric.. ");
                        // -> Brute Force Approach
                        AtomicInteger maxCounter = new AtomicInteger();
                        List<Similarity> bruteForceDuplicates = bruteForceAlgorithm.findDuplicates(metric, maxCounter);
                        if (bruteForceDuplicates.isEmpty()){
                            LOG.warn("No duplicates found in corpus");
                            continue;
                        }
                        LOG.info("Found " + bruteForceDuplicates.size() + " duplicates!");
                        for(Similarity sim: bruteForceDuplicates.stream().limit(100).collect(Collectors.toList())){
                            LOG.info("["+metric.id()+"] Duplicated Candidate: " + sim);
                        }

                        List<String> groundTruth = bruteForceDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList());

                        // -> Density-based Approach
                        for(Integer level : Arrays.asList(1,2,3,4,5)){

                            AtomicInteger densityCounter = new AtomicInteger();
                            densityBasedAlgorithm.setLevel(level);
                            Instant start = Instant.now();
                            List<Similarity> densityDuplicates = densityBasedAlgorithm.findDuplicates(metric, densityCounter);
                            Instant end = Instant.now();
//                            for(Similarity sim: densityDuplicates){
//                                LOG.info("["+metric.id()+"] Density-based Duplicated Candidate: " + sim);
//                            }

                            List<String> densityRetrievedList = densityDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList());
                            double averagePrecision = AveragePrecision.from(groundTruth, densityRetrievedList);
                            double improvement = 1 - (Double.valueOf(densityCounter.get()) / Double.valueOf(maxCounter.get()));

                            // mean Average Precision
                            Evaluation eval = new Evaluation();
                            eval.setDescription("iteration"+i);
                            eval.setTime(start,end);
                            eval.setAveragePrecision(averagePrecision);
                            eval.addResult(groundTruth, densityRetrievedList.stream().collect(Collectors.toList()));
                            eval.setEfficiency(improvement);
                            algorithmId = "density"+level+"("+metric.id()+")";
                            if (!results.containsKey(algorithmId)) results.put(algorithmId, new ArrayList<>());
                            results.get(algorithmId).add(eval);

                            // Precision@n and Recall@n
                            for(Integer n: accuracies.stream().sorted().collect(Collectors.toList())){
                                Evaluation eval2 = new Evaluation();
                                eval2.setDescription("iteration"+i);
                                eval2.setTime(start,end);
                                eval2.addResult(groundTruth.stream().limit(n).collect(Collectors.toList()), densityRetrievedList.stream().limit(n).collect(Collectors.toList()));
                                eval2.setEfficiency(improvement);
                                algorithmId = "density"+level+"@"+n+"("+metric.id()+")";
                                if (!results.containsKey(algorithmId)) results.put(algorithmId, new ArrayList<>());
                                results.get(algorithmId).add(eval2);
                            }
                        }

                        // -> Centroid-based Approach
                        for(Integer level : Arrays.asList(2,3,5)){

                            AtomicInteger centroidCounter = new AtomicInteger();
                            centroidBasedAlgorithm.setLevel(level);
                            Instant start = Instant.now();
                            List<Similarity> centroidDuplicates = centroidBasedAlgorithm.findDuplicates(metric, centroidCounter);
                            Instant end = Instant.now();
//                            for(Similarity sim: densityDuplicates){
//                                LOG.info("["+metric.id()+"] Density-based Duplicated Candidate: " + sim);
//                            }

                            List<String> centroidRetrievedList = centroidDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList());
                            double averagePrecision = AveragePrecision.from(groundTruth, centroidRetrievedList);
                            double improvement = 1 - (Double.valueOf(centroidCounter.get()) / Double.valueOf(maxCounter.get()));

                            // mean Average Precision
                            Evaluation eval = new Evaluation();
                            eval.setDescription("iteration"+i);
                            eval.setTime(start,end);
                            eval.addResult(groundTruth, centroidRetrievedList.stream().collect(Collectors.toList()));
                            eval.setAveragePrecision(averagePrecision);
                            eval.setEfficiency(improvement);
                            algorithmId = "centroid"+level+"("+metric.id()+")";
                            if (!results.containsKey(algorithmId)) results.put(algorithmId, new ArrayList<>());
                            results.get(algorithmId).add(eval);

                            // Precision@n and Recall@n
                            for(Integer n: accuracies.stream().sorted().collect(Collectors.toList())){
                                Evaluation eval2 = new Evaluation();
                                eval2.setDescription("iteration"+i);
                                eval2.setTime(start,end);
                                eval2.addResult(groundTruth.stream().limit(n).collect(Collectors.toList()), centroidRetrievedList.stream().limit(n).collect(Collectors.toList()));
                                eval2.setEfficiency(improvement);
                                algorithmId = "centroid"+level+"@"+n+"("+metric.id()+")";
                                if (!results.containsKey(algorithmId)) results.put(algorithmId, new ArrayList<>());
                                results.get(algorithmId).add(eval2);
                            }
                        }

                        // -> Threshold-based Approach
                        AtomicInteger thresholdCounter = new AtomicInteger();
                        Instant start = Instant.now();
                        List<Similarity> thresholdDuplicates = thresholdBasedAlgorithm.findDuplicates(metric, thresholdCounter);
                        Instant end = Instant.now();
                        List<String> thresholdRetrieveList = thresholdDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList());
                        double averagePrecision = AveragePrecision.from(groundTruth, thresholdRetrieveList);
                        double improvement = 1 - (Double.valueOf(thresholdCounter.get()) / Double.valueOf(maxCounter.get()));

                        for(Integer n: accuracies.stream().sorted().collect(Collectors.toList())){
                            Evaluation eval2 = new Evaluation();
                            eval2.setDescription("iteration"+i);
                            eval2.setTime(start,end);
                            eval2.addResult(groundTruth, thresholdRetrieveList.stream().limit(n).collect(Collectors.toList()));
                            eval2.setAveragePrecision(averagePrecision);
                            eval2.setEfficiency(improvement);
                            algorithmId = "threshold@"+n+"("+metric.id()+")";
                            if (!results.containsKey(algorithmId)) results.put(algorithmId, new ArrayList<>());
                            results.get(algorithmId).add(eval2);
                        }

                    }
                }
                LOG.info("Creating reports .. ");
                for(String algorithm: results.keySet().stream().sorted().collect(Collectors.toList())){
                    List<Evaluation> evaluations = results.get(algorithm);
                    Report report = new Report(evaluations);
                    Result result = new Result();
                    result.setAlgorithm(algorithm);
                    result.setCorpus(corpusId);
                    result.setEfficiency(report.getEfficiency());
                    result.setfMeasure(report.getFMeasure());
                    result.setPrecision(report.getPrecision());
                    result.setRecall(report.getRecall());
                    result.setmAP(report.getMeanAveragePrecision());
                    result.setTotal(report.getTotalHits());

                    LOG.info("[" + corpusId+"] '" + algorithm + "' results: "
                            + "mAP=" + report.getMeanAveragePrecision()
                            + ", P=" + report.getPrecision()
                            + ", R=" + report.getRecall()
                            + ", eff=" + report.getEfficiency()
                            + ", fM=" + report.getFMeasure()
                            + ", total=" + report.getTotalHits()
                    );

                    writer.write(jsonMapper.writeValueAsString(result)+"\n");

//                for(Evaluation e : evaluations){
//                    LOG.info("\t [-] Evaluation: " + e);
//                }
                }

            }
        }finally{
            writer.close();
        }
    }

}
