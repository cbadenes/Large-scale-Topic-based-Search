package experiments;

import oeg.lstbs.algorithms.BruteForceAlgorithm;
import oeg.lstbs.algorithms.DensityBasedAlgorithm;
import oeg.lstbs.algorithms.ThresholdBasedAlgorithm;
import oeg.lstbs.data.Document;
import oeg.lstbs.data.Evaluation;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.metrics.JSD;
import oeg.lstbs.metrics.S2JSD;
import oeg.lstbs.metrics.SimilarityMetric;
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



    @Test
    public void execute() throws IOException {


        List<String> corpora = Arrays.asList("");

        List<SimilarityMetric> metrics = Arrays.asList(new JSD(), new S2JSD());


        Map<String,Evaluation> results = new HashMap<>();

        // Algorithms
        BruteForceAlgorithm bruteForceAlgorithm = new BruteForceAlgorithm();
        DensityBasedAlgorithm densityBasedAlgorithm = new DensityBasedAlgorithm();
        ThresholdBasedAlgorithm thresholdBasedAlgorithm = new ThresholdBasedAlgorithm();


        for(String corpus: corpora){

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

                exec1.submit(() -> bruteForceAlgorithm.add(d1));
                exec1.submit(() -> densityBasedAlgorithm.add(d1));
                exec1.submit(() -> thresholdBasedAlgorithm.add(d1));
                if (counter.incrementAndGet() % 500 == 0) LOG.info("Added " + counter.get() + " documents");
            }
            exec1.awaitTermination(1, TimeUnit.HOURS);

            bruteForceAlgorithm.commit();
            densityBasedAlgorithm.commit();
            thresholdBasedAlgorithm.commit();

            LOG.info("Finding duplicates .. ");

            for(SimilarityMetric metric: metrics){
                // -> Brute Force Approach
                Evaluation eval1 = new Evaluation();
                eval1.setStart(Instant.now());
                List<Document> bruteForceDuplicates = bruteForceAlgorithm.findDuplicates(metric);
                eval1.setEnd(Instant.now());
                List<String> groundTruth = bruteForceDuplicates.stream().map(d -> d.getId()).collect(Collectors.toList());
                eval1.addResult(groundTruth, bruteForceDuplicates.stream().map(d -> d.getId()).collect(Collectors.toList()));
                results.put("bruteForce-"+metric.id(), eval1);

                // -> Density-based Approach
                Evaluation eval2 = new Evaluation();
                eval2.setStart(Instant.now());
                List<Document> densityDuplicates = densityBasedAlgorithm.findDuplicates(metric);
                eval2.setEnd(Instant.now());
                eval2.addResult(groundTruth, densityDuplicates.stream().map(d -> d.getId()).collect(Collectors.toList()));
                results.put("density"+metric.id(), eval2);

                // -> Threshold-based Approach
                Evaluation eval3 = new Evaluation();
                eval3.setStart(Instant.now());
                List<Document> thresholdDuplicates = thresholdBasedAlgorithm.findDuplicates(metric);
                eval3.setEnd(Instant.now());
                eval3.addResult(groundTruth, thresholdDuplicates.stream().map(d -> d.getId()).collect(Collectors.toList()));
                results.put("thresold"+metric.id(), eval2);

            }


            LOG.info("Creating report .. ");
            for(String algorithm: results.keySet()){
                LOG.info("Algorithm '" + algorithm + "': " + results.get(algorithm));
            }

        }

    }

}
