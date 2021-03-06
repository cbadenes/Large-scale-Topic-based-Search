package experiments;

import oeg.lstbs.data.*;
import oeg.lstbs.hash.CentroidHHM;
import oeg.lstbs.hash.DensityHHM;
import oeg.lstbs.hash.HierarchicalHashMethod;
import oeg.lstbs.hash.ThresholdHHM;
import oeg.lstbs.io.VectorReader;
import oeg.lstbs.io.WriterUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

    public class SizeExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(SizeExperiment.class);


    static final List<Dataset> DATASETS = Arrays.asList(
            new Dataset(new Corpus("cordis-70", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/aMBsQaTM4oBi3Ga/download"),       100000,    100,    500),
            new Dataset(new Corpus("cordis-150", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/WWHprbHxWigBMEC/download"),      100000,    100,    500),
            new Dataset(new Corpus("openresearch-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download"),500000,    100,    2500),
            new Dataset(new Corpus("patents-250", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/mG5Lwsii2CosERa/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-750", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/kTD8QEagJEyff3z/download"),     1000000,   100,    5000)
    );


    private static final String PERFORMANCE     = "performance";

    private static final String THRESHOLD_METHOD    = "threshold";
    private static final String CENTROID_METHOD     = "centroid";
    private static final String DENSITY_METHOD      = "density";

    static final Integer N                      = 5;
    static final List<Integer> DEPTH_LEVELS     = Arrays.asList(2,3,4,5,6);


    @Test
    public void execute(){


        for(Dataset dataset : DATASETS){

            ConcurrentHashMap<Integer,Map<String,Double>> performanceTable  = new ConcurrentHashMap<>();

            for(Integer depth : DEPTH_LEVELS){

                performanceTable.put(depth, new ConcurrentHashMap<String,Double>());


                // Threshold-based
                HierarchicalHashMethod thhm                         = new ThresholdHHM(depth);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> thResults    = new ConcurrentHashMap<>();
                evaluateMethod(dataset, thhm, depth, thResults);
                performanceTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(PERFORMANCE)).getMean());

                // Centroid-based
                HierarchicalHashMethod chhm                         = new CentroidHHM(depth,1000);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> cResults     = new ConcurrentHashMap<>();
                evaluateMethod(dataset, chhm, depth, cResults);
                performanceTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(PERFORMANCE)).getMean());


                // Density-based
                HierarchicalHashMethod dhhm                         = new DensityHHM(depth);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> dResults     = new ConcurrentHashMap<>();
                evaluateMethod(dataset, dhhm, depth, dResults);
                performanceTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(PERFORMANCE)).getMean());

            }


            saveResults("performance", dataset, performanceTable);

        }

    }


    private void saveResults(String id, Dataset dataset, Map<Integer,Map<String,Double>> results){
        try {
            File output = Paths.get("results",dataset.getCorpus().getId()+"-"+id+".md").toFile();
            if (output.exists()) output.delete();
            output.getParentFile().mkdirs();
            BufferedWriter writer = WriterUtils.to(output.getAbsolutePath());
            writer.write("########   Table: " + id + " for dataset:  " + dataset + "\n");
            writer.write("#Groups\tThresholdHHM\tCentroidHHM\tDensityHHM"+ "\n");
            for(Integer depth: DEPTH_LEVELS){
                Map<String, Double> methodResults = results.get(depth);
                writer.write(depth + "\t" + methodResults.get(THRESHOLD_METHOD) + "\t" + methodResults.get(CENTROID_METHOD) + "\t" + methodResults.get(DENSITY_METHOD)+ "\n");
            }
            writer.close();
        } catch (IOException e) {
            LOG.error("Unexpected error",e);
        }

    }

    private void evaluateMethod(Dataset dataset, HierarchicalHashMethod method, Integer depth, Map<String,ConcurrentLinkedQueue<Double>> results){
        String repositoryName = dataset.getCorpus().getId()+"_"+depth+"_"+ StringUtils.substringAfterLast(method.getClass().getCanonicalName(),".");
        Repository repository = new Repository(repositoryName);
        LOG.info("Evaluating method " + method  + " in dataset: " + dataset + " with depth level equals to " + depth +  "...");
        VectorReader.VectorAction validateSimilarity = (docId, topicDistribution) -> evaluateDocumentSimilarity(repository, topicDistribution, method, dataset.getRelevantSize(), results);
        VectorReader.VectorValidation predicate = (id, td) -> true;
        VectorReader.from(dataset.getCorpus().getPath(), dataset.getIndexSize(), validateSimilarity, predicate,  Double.valueOf(Math.ceil(dataset.getTestSize() / 100.0)).intValue(), dataset.getTestSize());
    }


    private void evaluateDocumentSimilarity(Repository repository, List<Double> vector, HierarchicalHashMethod method, Integer relevantSize, Map<String,ConcurrentLinkedQueue<Double>> results){
        Map<Integer, List<String>> hash = method.hash(vector);
        Double hitsRatio                = repository.getRatioHitsTo(hash);
        updateResult(results, PERFORMANCE, hitsRatio);

    }

    private synchronized void updateResult(Map<String,ConcurrentLinkedQueue<Double>> results, String category, Double value){
        if (!results.containsKey(category)) results.put(category, new ConcurrentLinkedQueue<Double>());
        results.get(category).add(value);
        LOG.debug(category+"@"+ N +"= " + value);
    }


}
