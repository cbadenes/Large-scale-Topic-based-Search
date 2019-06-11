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
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
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
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

    public class DocumentSimilarityExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentSimilarityExperiment.class);


    static final List<Dataset> DATASETS = Arrays.asList(
            new Dataset(new Corpus("cordis-70", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/aMBsQaTM4oBi3Ga/download"),       100000,    100,    500),
            new Dataset(new Corpus("cordis-150", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/WWHprbHxWigBMEC/download"),      100000,    100,    500),
            new Dataset(new Corpus("openresearch-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download"),500000,    100,    2500),
            new Dataset(new Corpus("patents-250", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/mG5Lwsii2CosERa/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-750", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/kTD8QEagJEyff3z/download"),     1000000,   100,    5000),

            new Dataset(new Corpus("cordis-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ZG7TtJmbmRi9LDe/download"),       100000,    100,    500),
            new Dataset(new Corpus("cordis-200", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/dyCEmY8TKdSMWn6/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-300", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Ac5irmmxSkdgyGw/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-400", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/LaMRrzcXyD2opgs/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/tfQFqAYL9XS5NB6/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-600", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Hps7z49cYFwGo3J/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-700", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ywQNySzFdrqcqQx/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-800", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KEE3fyFkM7Wq6Zq/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-900", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/56bWjK7pk7sQcNS/download"),      100000,    100,    500),
            new Dataset(new Corpus("cordis-1000", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/87ega8bYMZH8T62/download"),      100000,    100,    500),

            new Dataset(new Corpus("openresearch-200", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/nRWxmYH6AjHqA6N/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-300", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-400", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KYtEqTB8zyaKb8N/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-600", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/7wYmtWNiYnbHoZP/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-700", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/xF7K3jtnTaWFjrN/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-800", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-900", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/TQK5aHWREPSdDLQ/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1000", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/MG7Zn94J4xTaRqL/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1200", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/i8P5JKJ5RCZG3Xe/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1300", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/SaPFLK2gb2KiCMr/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1400", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/aaaCQpwQRKgyFQ3/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/PCFjBFfwfzf2WM9/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1600", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/8ifFKeypCsdttCs/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1700", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ntM5KzW6X9m5kcW/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1800", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/R3aztCF49xykeLS/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-1900", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/FGBLtESCEr7SAbN/download"),500000,    100,    2500),
            new Dataset(new Corpus("openresearch-2000", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/itkmdeESJeLQPXY/download"),500000,    100,    2500),


            new Dataset(new Corpus("patents-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/XJXTfSXNeYeHioj/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-200", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/NCXxPPXc2xDrDky/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-300", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/idriqdGxLSoXmai/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-400", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/GLFEwjDicrBt3YR/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/bXwFfLQfctiDJqa/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-600", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/EHXepb7H98yz6KX/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-700", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/tXcndTbqGKyRRDH/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-800", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zeRFwbKpjZyT7Dg/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-900", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Nbo7NW7s8R2e5Te/download"),     1000000,   100,    5000),
            new Dataset(new Corpus("patents-1000", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Cxx8orc8g6Y53qR/download"),     1000000,   100,    5000)

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

            ConcurrentHashMap<Integer,Map<String,SummaryStatistics>> precisionTable            = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer,Map<String,SummaryStatistics>> performanceTable          = new ConcurrentHashMap<>();

            for(Integer depth : DEPTH_LEVELS){

                precisionTable.put(depth, new ConcurrentHashMap<String,SummaryStatistics>());
                performanceTable.put(depth, new ConcurrentHashMap<String,SummaryStatistics>());


                // Threshold-based
                HierarchicalHashMethod thhm                              = new ThresholdHHM(depth);
                ConcurrentHashMap<String,SummaryStatistics> thResults    = new ConcurrentHashMap<>();
                evaluateMethod(dataset, thhm, depth, thResults);
                precisionTable.get(depth).put(THRESHOLD_METHOD, thResults.get(PRECISION));
                performanceTable.get(depth).put(THRESHOLD_METHOD, thResults.get(PERFORMANCE));

                // Centroid-based
                HierarchicalHashMethod chhm                              = new CentroidHHM(depth,1000);
                ConcurrentHashMap<String,SummaryStatistics> cResults     = new ConcurrentHashMap<>();
                evaluateMethod(dataset, chhm, depth, cResults);
                precisionTable.get(depth).put(CENTROID_METHOD, cResults.get(PRECISION));
                performanceTable.get(depth).put(CENTROID_METHOD, cResults.get(PERFORMANCE));


                // Density-based
                HierarchicalHashMethod dhhm                              = new DensityHHM(depth);
                ConcurrentHashMap<String,SummaryStatistics> dResults     = new ConcurrentHashMap<>();
                evaluateMethod(dataset, dhhm, depth, dResults);
                precisionTable.get(depth).put(DENSITY_METHOD, dResults.get(PRECISION));
                performanceTable.get(depth).put(DENSITY_METHOD, dResults.get(PERFORMANCE));

            }


            saveResults("p"+N, dataset, precisionTable);
            saveResults("performance", dataset, performanceTable);

        }

    }


    private void saveResults(String id, Dataset dataset, Map<Integer,Map<String,SummaryStatistics>> results){
        try {
            File output = Paths.get("results",dataset.getCorpus().getId()+"-"+id+".md").toFile();
            if (output.exists()) output.delete();
            output.getParentFile().mkdirs();
            BufferedWriter writer = WriterUtils.to(output.getAbsolutePath());
            writer.write("########   Table: " + id + " for dataset:  " + dataset + "\n");
            writer.write("#Groups\tThresholdHHM\tCentroidHHM\tDensityHHM"+ "\n");
            for(Integer depth: DEPTH_LEVELS){
                Map<String, SummaryStatistics> methodResults = results.get(depth);
                writer.write(depth + "\t" + getSummary(methodResults.get(THRESHOLD_METHOD)) + "\t" + getSummary(methodResults.get(CENTROID_METHOD)) + "\t" + getSummary(methodResults.get(DENSITY_METHOD)) + "\n");
            }
            writer.close();
        } catch (IOException e) {
            LOG.error("Unexpected error",e);
        }

    }

    private String getSummary(SummaryStatistics summaryStatistics){
        if (summaryStatistics == null) return "[]";
        return "["+summaryStatistics.toString().replace("\n","|")+"]";
    }

    private void saveResults(String id, Dataset dataset, DescriptiveStatistics results){
        try {
            File output = Paths.get("results",dataset.getCorpus().getId()+"-"+id+".md").toFile();
            if (output.exists()) output.delete();
            output.getParentFile().mkdirs();
            BufferedWriter writer = WriterUtils.to(output.getAbsolutePath());

            writer.write(results.toString() +"\n");
            writer.write("values: " + "\n");

            String row = Arrays.stream(results.getValues()).boxed().map(d -> String.valueOf(d)).collect(Collectors.joining(" , "));
            writer.write(row +"\n");
            writer.close();
        } catch (IOException e) {
            LOG.error("Unexpected error",e);
        }

    }

    private void evaluateMethod(Dataset dataset, HierarchicalHashMethod method, Integer depth, Map<String,SummaryStatistics> results){
        String repositoryName = dataset.getCorpus().getId()+"_"+depth+"_"+ StringUtils.substringAfterLast(method.getClass().getCanonicalName(),".");
        Index index = new Index(repositoryName, dataset.getCorpus().getPath() , dataset.getIndexSize(), method);
        LOG.info("Evaluating method " + method  + " in dataset: " + dataset + " with depth level equals to " + depth +  "...");
        VectorReader.VectorAction validateSimilarity = (docId, topicDistribution) -> evaluateDocumentSimilarity(index.getRepository(), topicDistribution, method, dataset.getRelevantSize(), results);
        VectorReader.VectorValidation predicate = (id, td) -> true;
        VectorReader.from(dataset.getCorpus().getPath(), dataset.getIndexSize(), validateSimilarity, predicate, Double.valueOf(Math.ceil(dataset.getTestSize() / 100.0)).intValue(), dataset.getTestSize());
    }


    private void evaluateDocumentSimilarity(Repository repository, List<Double> vector, HierarchicalHashMethod method, Integer relevantSize, Map<String,SummaryStatistics> results){
        Map<String,Double> simDocs      = repository.getSimilarTo(vector, relevantSize, new JSD());
//        Map<String,Double> simDocs      = repository.getSimilarToByThreshold(vector, 0.8, new JSD());
        Map<Integer, List<String>> hash = method.hash(vector);
        Map<String,Double> relDocs      = repository.getSimilarTo(hash, relevantSize);
        Double hitsRatio                = repository.getRatioHitsTo(hash);

        SimilarityResult simResult = new SimilarityResult(simDocs, relDocs);
        updateResult(results, PRECISION, simResult.getPrecisionAt(N));
        updateResult(results, RECALL, simResult.getRecallAt(N));
        updateResult(results, fMEASURE, simResult.getFMeasureAt(N));
        updateResult(results, PERFORMANCE, hitsRatio);

    }

    private synchronized void updateResult(Map<String,SummaryStatistics> results, String category, Double value){
        if (!results.containsKey(category)) results.put(category, new SummaryStatistics());
        results.get(category).addValue(value);
        LOG.debug(category+"@"+ N +"= " + value);
    }


}
