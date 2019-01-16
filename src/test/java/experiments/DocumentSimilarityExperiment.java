package experiments;

import com.sun.org.apache.regexp.internal.RE;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

    public class DocumentSimilarityExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentSimilarityExperiment.class);


    static final List<Dataset> DATASETS = Arrays.asList(
            new Dataset(new Corpus("cordis-70", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/aMBsQaTM4oBi3Ga/download"),       100000,     100,    500),
            new Dataset(new Corpus("cordis-150", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/WWHprbHxWigBMEC/download"),      100000,     100,    500),
            new Dataset(new Corpus("openresearch-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download"),1000000,    100,    5000),
            new Dataset(new Corpus("openresearch-500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download"),1000000,    100,    5000),
            new Dataset(new Corpus("patents-250", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/mG5Lwsii2CosERa/download"),     10000000,   100,    50000),
            new Dataset(new Corpus("patents-750", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/kTD8QEagJEyff3z/download"),     10000000,   100,    50000)
    );


    private static final String PRECISION       = "precision";
    private static final String RECALL          = "recall";
    private static final String fMEASURE        = "fmeasure";

    private static final String THRESHOLD_METHOD    = "threshold";
    private static final String CENTROID_METHOD     = "centroid";
    private static final String DENSITY_METHOD      = "density";

    static final Integer N                      = 5;
    static final List<Integer> DEPTH_LEVELS     = Arrays.asList(2,3,4,5,6);


    @Test
    public void execute(){


        for(Dataset dataset : DATASETS){

            ConcurrentHashMap<Integer,Map<String,Double>> precisionTable    = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer,Map<String,Double>> recallTable       = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer,Map<String,Double>> fMeasureTable     = new ConcurrentHashMap<>();

            for(Integer depth : DEPTH_LEVELS){

                precisionTable.put(depth, new ConcurrentHashMap<String,Double>());
                recallTable.put(depth, new ConcurrentHashMap<String,Double>());
                fMeasureTable.put(depth, new ConcurrentHashMap<String,Double>());


                // Threshold-based
                HierarchicalHashMethod thhm                         = new ThresholdHHM(depth);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> thResults    = new ConcurrentHashMap<>();
                evaluateMethod(dataset, thhm, depth, thResults);
                precisionTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(PRECISION)).getMean());
                recallTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(RECALL)).getMean());
                fMeasureTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(fMEASURE)).getMean());

                // Centroid-based
                HierarchicalHashMethod chhm                         = new CentroidHHM(depth,1000);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> cResults     = new ConcurrentHashMap<>();
                evaluateMethod(dataset, chhm, depth, cResults);
                precisionTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(PRECISION)).getMean());
                recallTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(RECALL)).getMean());
                fMeasureTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(fMEASURE)).getMean());


                // Density-based
                HierarchicalHashMethod dhhm                         = new DensityHHM(depth);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> dResults     = new ConcurrentHashMap<>();
                evaluateMethod(dataset, dhhm, depth, dResults);
                precisionTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(PRECISION)).getMean());
                recallTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(RECALL)).getMean());
                fMeasureTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(fMEASURE)).getMean());

            }


            saveResults("p"+N, dataset, precisionTable);
            saveResults("r"+N, dataset, recallTable);
            saveResults("f"+N, dataset, fMeasureTable);

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
        LOG.info("Evaluating method " + method  + " in dataset: " + dataset + " with depth level equals to " + depth +  "...");
        String repositoryName = dataset.getCorpus().getId()+"_"+depth+"_"+ StringUtils.substringAfterLast(method.getClass().getCanonicalName(),".");
        Index index = new Index(repositoryName, dataset.getCorpus().getPath() , dataset.getIndexSize(), method);
        VectorReader.VectorAction validateSimilarity = (docId, topicDistribution) -> evaluateDocumentSimilarity(index.getRepository(), topicDistribution, method, dataset.getRelevantSize(), results);
        VectorReader.from(dataset.getCorpus().getPath(), dataset.getIndexSize(), validateSimilarity, Double.valueOf(Math.ceil(dataset.getTestSize() / 100.0)).intValue(), dataset.getTestSize());
    }


    private void evaluateDocumentSimilarity(Repository repository, List<Double> vector, HierarchicalHashMethod method, Integer relevantSize, Map<String,ConcurrentLinkedQueue<Double>> results){
        Map<String,Double> simDocs = repository.getSimilarTo(vector, relevantSize);
        Map<Integer, List<String>> hash = method.hash(vector);
        Map<String,Double> relDocs = repository.getSimilarTo(hash, relevantSize);

        SimilarityResult simResult = new SimilarityResult(simDocs, relDocs);
        updateResult(results, PRECISION, simResult.getPrecisionAt(N));
        updateResult(results, RECALL, simResult.getRecallAt(N));
        updateResult(results, fMEASURE, simResult.getFMeasureAt(N));

    }

    private synchronized void updateResult(Map<String,ConcurrentLinkedQueue<Double>> results, String category, Double value){
        if (!results.containsKey(category)) results.put(category, new ConcurrentLinkedQueue<Double>());
        if (value == null){
            LOG.warn("null value in category: " + category);
            return;
        }
        results.get(category).add(value);
        LOG.debug(category+"@"+ N +"= " + value);
    }


}
