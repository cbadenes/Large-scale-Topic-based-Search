package experiments;

import oeg.lstbs.data.*;
import oeg.lstbs.hash.CentroidHHM;
import oeg.lstbs.hash.DensityHHM;
import oeg.lstbs.hash.HierarchicalHashMethod;
import oeg.lstbs.hash.ThresholdHHM;
import oeg.lstbs.io.VectorReader;
import oeg.lstbs.io.WriterUtils;
import oeg.lstbs.metrics.JSD;
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

    public class DocumentSimilarityExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentSimilarityExperiment.class);


    static final List<Dataset> DATASETS = Arrays.asList(
            new Dataset(new Corpus("cordis-70", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/aMBsQaTM4oBi3Ga/download"),       100000,    100,    500),
            new Dataset(new Corpus("cordis-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ZG7TtJmbmRi9LDe/download"),       100000,    100,    500),
            new Dataset(new Corpus("cordis-150", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/WWHprbHxWigBMEC/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-200", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/dyCEmY8TKdSMWn6/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-300", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Ac5irmmxSkdgyGw/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-400", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/LaMRrzcXyD2opgs/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/tfQFqAYL9XS5NB6/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-600", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Hps7z49cYFwGo3J/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-700", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ywQNySzFdrqcqQx/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-800", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KEE3fyFkM7Wq6Zq/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-900", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/56bWjK7pk7sQcNS/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-1000", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/87ega8bYMZH8T62/download"),      100000,    100,    500),
            new Dataset(new Corpus("openresearch-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download"),500000,    100,    2500),
            new Dataset(new Corpus("patents-250", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/mG5Lwsii2CosERa/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-750", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/kTD8QEagJEyff3z/download"),     1000000,   100,    5000)
    );


    private static final String PRECISION       = "precision";
    private static final String RECALL          = "recall";
    private static final String fMEASURE        = "fmeasure";
    private static final String PERFORMANCE     = "performance";

    private static final String THRESHOLD_METHOD    = "threshold";
    private static final String CENTROID_METHOD     = "centroid";
    private static final String DENSITY_METHOD      = "density";

    static final Integer N                      = 5;
    static final List<Integer> DEPTH_LEVELS     = Arrays.asList(2,3,4,5,6);


    @Test
    public void execute(){


        for(Dataset dataset : DATASETS){

            ConcurrentHashMap<Integer,Map<String,Double>> precisionVarianceTable    = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer,Map<String,Double>> precisionTable            = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer,Map<String,Double>> recallTable               = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer,Map<String,Double>> fMeasureTable             = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer,Map<String,Double>> performanceTable          = new ConcurrentHashMap<>();

            for(Integer depth : DEPTH_LEVELS){

                precisionVarianceTable.put(depth, new ConcurrentHashMap<String,Double>());
                precisionTable.put(depth, new ConcurrentHashMap<String,Double>());
                recallTable.put(depth, new ConcurrentHashMap<String,Double>());
                fMeasureTable.put(depth, new ConcurrentHashMap<String,Double>());
                performanceTable.put(depth, new ConcurrentHashMap<String,Double>());


                // Threshold-based
                HierarchicalHashMethod thhm                         = new ThresholdHHM(depth);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> thResults    = new ConcurrentHashMap<>();
                evaluateMethod(dataset, thhm, depth, thResults);
                precisionVarianceTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(PRECISION)).getVariance());
                precisionTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(PRECISION)).getMean());
                recallTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(RECALL)).getMean());
                fMeasureTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(fMEASURE)).getMean());
                performanceTable.get(depth).put(THRESHOLD_METHOD, new Stats(thResults.get(PERFORMANCE)).getMean());

                // Centroid-based
                HierarchicalHashMethod chhm                         = new CentroidHHM(depth,1000);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> cResults     = new ConcurrentHashMap<>();
                evaluateMethod(dataset, chhm, depth, cResults);
                precisionVarianceTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(PRECISION)).getVariance());
                precisionTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(PRECISION)).getMean());
                recallTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(RECALL)).getMean());
                fMeasureTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(fMEASURE)).getMean());
                performanceTable.get(depth).put(CENTROID_METHOD, new Stats(cResults.get(PERFORMANCE)).getMean());


                // Density-based
                HierarchicalHashMethod dhhm                         = new DensityHHM(depth);
                ConcurrentHashMap<String,ConcurrentLinkedQueue<Double>> dResults     = new ConcurrentHashMap<>();
                evaluateMethod(dataset, dhhm, depth, dResults);
                precisionVarianceTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(PRECISION)).getVariance());
                precisionTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(PRECISION)).getMean());
                recallTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(RECALL)).getMean());
                fMeasureTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(fMEASURE)).getMean());
                performanceTable.get(depth).put(DENSITY_METHOD, new Stats(dResults.get(PERFORMANCE)).getMean());

            }


            saveResults("vp"+N, dataset, precisionVarianceTable);
            saveResults("p"+N, dataset, precisionTable);
            saveResults("r"+N, dataset, recallTable);
            saveResults("f"+N, dataset, fMeasureTable);
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
        Index index = new Index(repositoryName, dataset.getCorpus().getPath() , dataset.getIndexSize(), method);
        LOG.info("Evaluating method " + method  + " in dataset: " + dataset + " with depth level equals to " + depth +  "...");
        VectorReader.VectorAction validateSimilarity = (docId, topicDistribution) -> evaluateDocumentSimilarity(index.getRepository(), topicDistribution, method, dataset.getRelevantSize(), results);
        VectorReader.VectorValidation predicate = (id, td) -> true;
        VectorReader.from(dataset.getCorpus().getPath(), dataset.getIndexSize(), validateSimilarity, predicate, Double.valueOf(Math.ceil(dataset.getTestSize() / 100.0)).intValue(), dataset.getTestSize());
    }


    private void evaluateDocumentSimilarity(Repository repository, List<Double> vector, HierarchicalHashMethod method, Integer relevantSize, Map<String,ConcurrentLinkedQueue<Double>> results){
        Map<String,Double> simDocs      = repository.getSimilarTo(vector, relevantSize, new JSD());
        Map<Integer, List<String>> hash = method.hash(vector);
        Map<String,Double> relDocs      = repository.getSimilarTo(hash, relevantSize);
        Double hitsRatio                = repository.getRatioHitsTo(hash);

        SimilarityResult simResult = new SimilarityResult(simDocs, relDocs);
        updateResult(results, PRECISION, simResult.getPrecisionAt(N));
        updateResult(results, RECALL, simResult.getRecallAt(N));
        updateResult(results, fMEASURE, simResult.getFMeasureAt(N));
        updateResult(results, PERFORMANCE, hitsRatio);

    }

    private synchronized void updateResult(Map<String,ConcurrentLinkedQueue<Double>> results, String category, Double value){
        if (!results.containsKey(category)) results.put(category, new ConcurrentLinkedQueue<Double>());
        results.get(category).add(value);
        LOG.debug(category+"@"+ N +"= " + value);
    }


}
