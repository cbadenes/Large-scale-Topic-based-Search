package experiments;

import oeg.lstbs.algorithms.BruteForceAlgorithm;
import oeg.lstbs.algorithms.DensityBasedAlgorithm;
import oeg.lstbs.algorithms.ThresholdBasedAlgorithm;
import oeg.lstbs.data.Document;
import oeg.lstbs.data.Evaluation;
import oeg.lstbs.data.Similarity;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.metrics.Hellinger;
import oeg.lstbs.metrics.JSD;
import oeg.lstbs.metrics.ComparisonMetric;
import oeg.lstbs.metrics.S2JSD;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class DuplicatesExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(DuplicatesExperiment.class);

    int numPairs    = 10; // 10

    List<ComparisonMetric> metrics = Arrays.asList(new JSD(), new S2JSD(), new Hellinger());

    List<String> models = Arrays.asList(
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download",    // 100 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/nRWxmYH6AjHqA6N/download",    // 200 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download",    // 300 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KYtEqTB8zyaKb8N/download",    // 400 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download",    // 500 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/7wYmtWNiYnbHoZP/download",    // 600 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/xF7K3jtnTaWFjrN/download",    // 700 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download",    // 800 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/TQK5aHWREPSdDLQ/download",    // 900 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download",    // 1000 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/MG7Zn94J4xTaRqL/download"     // 1100 topics
    );


    @Test
    public void execute() throws IOException {


        Map<String,Evaluation> results = new HashMap<>();

        // Algorithms
        BruteForceAlgorithm bruteForceAlgorithm = new BruteForceAlgorithm();
        DensityBasedAlgorithm densityBasedAlgorithm = new DensityBasedAlgorithm();
        ThresholdBasedAlgorithm thresholdBasedAlgorithm = new ThresholdBasedAlgorithm();


        for(String corpus: models){

            LOG.info("Reading topic distributions .. ");
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
                        densityBasedAlgorithm.add(d1);
                        thresholdBasedAlgorithm.add(d1);
                    }catch(Exception e){
                        LOG.error("Unexpected error",e);
                    }
                });

                if (counter.incrementAndGet() % 100 == 0) LOG.info("Added " + counter.get() + " documents");
            }
            exec1.awaitTermination(1, TimeUnit.HOURS);

            LOG.info("Finally Added " + counter.get() + " documents");

            bruteForceAlgorithm.commit();
            densityBasedAlgorithm.commit();
            thresholdBasedAlgorithm.commit();

            LOG.info("Finding duplicates .. ");

            for(ComparisonMetric metric: metrics){
                // -> Brute Force Approach
                Evaluation eval1 = new Evaluation();
                eval1.setStart(Instant.now());
                List<Similarity> bruteForceDuplicates = bruteForceAlgorithm.findDuplicates(metric, numPairs);
                eval1.setEnd(Instant.now());
                for(Similarity sim: bruteForceDuplicates){
                    LOG.info("["+metric.id()+"] Duplicated Candidate: " + sim);
                }
                List<String> groundTruth = bruteForceDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList());
                eval1.addResult(groundTruth, bruteForceDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList()));
                results.put(metric.id()+"-based bruteForce", eval1);

                // -> Density-based Approach
                Evaluation eval2 = new Evaluation();
                eval2.setStart(Instant.now());
                List<Similarity> densityDuplicates = densityBasedAlgorithm.findDuplicates(metric, numPairs);
                eval2.setEnd(Instant.now());
                eval2.addResult(groundTruth, densityDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList()));
                results.put(metric.id()+"-based density", eval2);

                // -> Threshold-based Approach
                Evaluation eval3 = new Evaluation();
                eval3.setStart(Instant.now());
                List<Similarity> thresholdDuplicates = thresholdBasedAlgorithm.findDuplicates(metric, numPairs);
                eval3.setEnd(Instant.now());
                eval3.addResult(groundTruth, thresholdDuplicates.stream().map(d -> d.getPair()).collect(Collectors.toList()));
                results.put(metric.id()+"-based thresold", eval2);

            }


            LOG.info("Creating report .. ");
            for(String algorithm: results.keySet()){
                LOG.info("Algorithm '" + algorithm + "': " + results.get(algorithm));
            }

        }

    }

}
