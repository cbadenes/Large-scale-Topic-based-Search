package experiments;

import oeg.lstbs.algorithms.BruteForceAlgorithm;
import oeg.lstbs.algorithms.DensityBasedAlgorithm;
import oeg.lstbs.algorithms.ThresholdBasedAlgorithm;
import oeg.lstbs.data.Document;
import oeg.lstbs.data.Evaluation;
import oeg.lstbs.data.Report;
import oeg.lstbs.data.Similarity;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.metrics.Hellinger;
import oeg.lstbs.metrics.JSD;
import oeg.lstbs.metrics.ComparisonMetric;
import oeg.lstbs.metrics.S2JSD;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
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


    Map<Integer,String> models = new HashMap<>();

    @Before
    public void setup(){

        // OpenResearchCorpus
        models.put(100,"https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download");
        models.put(300,"https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download");
//        models.put(500,"https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download");
//        models.put(800,"https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download");
//        models.put(1000,"https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download");

        // Cordis
//        models.put(70,"https://delicias.dia.fi.upm.es/nextcloud/index.php/s/82z5WRNbYftqr2L/download");

    }


    @Test
    public void execute() throws IOException {

        for(Integer corpusId: models.keySet().stream().sorted().collect(Collectors.toList())){

            List<ComparisonMetric> metrics = Arrays.asList(new JSD());
            List<Integer> accuracies = Arrays.asList(1,3,5,10);
            int sampleSize  = 1000; //1000
            int iterations  = 2; //10
            // 10 random training/test partitions. For each partition, we randomly select 1k articles

            String corpus = models.get(corpusId);
            Map<String,List<Evaluation>> results = new HashMap<>();

            for(int i=0;i<iterations;i++){
                int iteration = i;

                // Algorithms
                BruteForceAlgorithm bruteForceAlgorithm = new BruteForceAlgorithm();
                DensityBasedAlgorithm densityBasedAlgorithm = new DensityBasedAlgorithm();
                ThresholdBasedAlgorithm thresholdBasedAlgorithm = new ThresholdBasedAlgorithm();

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


                    LOG.info("Finding duplicates .. ");
                    // -> Brute Force Approach
                    Evaluation eval1 = new Evaluation();
                    eval1.setStart(Instant.now());
                    AtomicInteger maxCounter = new AtomicInteger();
                    List<Similarity> bruteForceDuplicates = bruteForceAlgorithm.findDuplicates(metric, accuracies.stream().reduce((a,b) -> (a>b)? a: b).get(),0, maxCounter).stream().filter(sim -> sim.getScore()>0.95).collect(Collectors.toList());
                    eval1.setEnd(Instant.now());
                    for(Similarity sim: bruteForceDuplicates){
                        LOG.info("["+metric.id()+"] Duplicated Candidate: " + sim);
                    }

                    if (bruteForceDuplicates.isEmpty()){
                        LOG.warn("No duplicates found in corpus");
                        continue;
                    }


                    for(Integer n: accuracies.stream().sorted().collect(Collectors.toList())){

                        List<String> groundTruth = bruteForceDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList());
                        eval1.addResult(groundTruth, bruteForceDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList()));
                        String algorithmId = metric.id()+"-based bruteForce@"+n;
                        if (!results.containsKey(algorithmId)) results.put(algorithmId, new ArrayList<>());
                        results.get(algorithmId).add(eval1);

                        // -> Density-based Approach
                        for(Integer level : Arrays.asList(1,2,3,4,5)){
                            Evaluation eval2 = new Evaluation();
                            eval2.setStart(Instant.now());
                            AtomicInteger densityCounter = new AtomicInteger();
                            List<Similarity> densityDuplicates = densityBasedAlgorithm.findDuplicates(metric, n, level, densityCounter);
//                            for(Similarity sim: densityDuplicates){
//                                LOG.info("["+metric.id()+"] Density-based Duplicated Candidate: " + sim);
//                            }
                            eval2.setEnd(Instant.now());
                            eval2.addResult(groundTruth, densityDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList()));
                            double improvement = 1 - (Double.valueOf(densityCounter.get()) / Double.valueOf(maxCounter.get()));
                            eval2.setImprovement(improvement);
                            algorithmId = metric.id()+"-based density"+level+"@"+n;
                            if (!results.containsKey(algorithmId)) results.put(algorithmId, new ArrayList<>());
                            results.get(algorithmId).add(eval2);
//                            LOG.info("Evaluation "+ algorithmId + " -> " + eval2);
                        }

                        // -> Threshold-based Approach
                        Evaluation eval3 = new Evaluation();
                        eval3.setStart(Instant.now());
                        AtomicInteger thresholdCounter = new AtomicInteger();
                        List<Similarity> thresholdDuplicates = thresholdBasedAlgorithm.findDuplicates(metric, n,1,thresholdCounter);
                        eval3.setEnd(Instant.now());
                        eval3.addResult(groundTruth, thresholdDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList()));
                        double improvement = 1 - (Double.valueOf(thresholdCounter.get()) / Double.valueOf(maxCounter.get()));
                        eval3.setImprovement(improvement);
                        algorithmId = metric.id()+"-based threshold"+"@"+n;
                        if (!results.containsKey(algorithmId)) results.put(algorithmId, new ArrayList<>());
                        results.get(algorithmId).add(eval3);

                    }

                }
            }
            LOG.info("Creating reports .. ");
            for(String algorithm: results.keySet()){
                LOG.info("Algorithm '" + algorithm + "' in Corpus " + corpusId + ": " );
                Report report = new Report(results.get(algorithm));
                LOG.info("\t- mAP: " + report.getmAP());
                LOG.info("\t- precision: " + report.getPrecision());
                LOG.info("\t- improvement: " + report.getImprovement());
                LOG.info("\t- fmeasure: " + report.getFMeasure());
            }

        }

    }

}
