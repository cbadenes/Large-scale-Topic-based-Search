import oeg.lstbs.data.Repository;
import oeg.lstbs.data.SimilarityResult;
import oeg.lstbs.data.Stats;
import oeg.lstbs.hash.CentroidHHM;
import oeg.lstbs.hash.DensityHHM;
import oeg.lstbs.hash.HierarchicalHashMethod;
import oeg.lstbs.hash.ThresholdHHM;
import oeg.lstbs.io.ParallelExecutor;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.io.SerializationUtils;
import org.apache.jute.Index;
import org.apache.lucene.document.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class SimilarityExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityExperiment.class);

    private static final Integer TEST_SIZE      = 100;

    private static final Integer MAX_RELEVANT   = 100;

    private static final Integer N              = 5;

    List<Double> precisionList  = new ArrayList<>();
    List<Double> recallList     = new ArrayList<>();
    List<Double> fmeasureList   = new ArrayList<>();

    Map<Integer,List<Double>> precisionCurve = new HashMap<>();
    Map<Integer,List<Double>> recallCurve = new HashMap<>();

    @Test
    public void exploreByThreshold(){

        exploreBy(IndexBuilder.CORPUS,new Repository("threshold-based"), new ThresholdHHM(IndexBuilder.DEPTH));
    }

    @Test
    public void exploreByCentroid(){

        exploreBy(IndexBuilder.CORPUS,new Repository("centroid-based"), new CentroidHHM(IndexBuilder.DEPTH,1000));
    }

    @Test
    public void exploreByDensity(){

        //exploreBy(new Repository("density-based"), new DensityHHM(IndexBuilder.DEPTH));

        exploreBy(IndexBuilder.CORPUS, new Repository("density-based"), new DensityHHM(IndexBuilder.DEPTH));
    }

    private void exploreBy(Repository repository, HierarchicalHashMethod method){

        Random random = new Random();

        for(int i = 0; i< TEST_SIZE; i++){
            Integer index = random.nextInt(repository.getSize());
            Document doc = repository.getDocument(index);
            LOG.info("Document: " + doc.get("id") );
            for(int j=0;j<IndexBuilder.DEPTH;j++){
                LOG.info("hash"+j+": " + doc.get("hash"+j));
            }
            List<Double> vector = (List<Double>) SerializationUtils.deserialize(doc.getBinaryValue("vector").bytes);
            LOG.info("Vector: " + vector);

            evaluate(repository, vector, method);
        }

        showResults();
    }

    private void exploreBy(String path, Repository repository, HierarchicalHashMethod method){


        try{
            BufferedReader reader = ReaderUtils.from(path);
            String row;
            AtomicInteger counter = new AtomicInteger();
            ParallelExecutor executor = new ParallelExecutor();
            Integer ratio = Double.valueOf(Math.ceil(Double.valueOf(TEST_SIZE)/ 100.0)).intValue();
            while((row = reader.readLine()) != null){
                String[] values = row.split(",");
                String dId = values[0];
                List<Double> vector = Arrays.stream(values).skip(1).mapToDouble(v -> Double.valueOf(v)).boxed().collect(Collectors.toList());

                if (repository.contains(dId)) continue;

                evaluate(repository, vector, method);

                if (counter.incrementAndGet() % ratio == 0) LOG.info(counter.get() + " vectors read" );
                if (counter.get() >= TEST_SIZE) break;
            }
            executor.awaitTermination(1l, TimeUnit.HOURS);
            reader.close();
            LOG.info(counter.get() + " vectors finally read" );
        }catch (Exception e){
            LOG.error("",e);
            throw new RuntimeException(e);
        }
        showResults();
    }


    private void showResults(){
        LOG.info("######################");
        LOG.info("Index Depth: " + IndexBuilder.DEPTH);
        LOG.info("Relevant Size: " + MAX_RELEVANT);
        LOG.info("Test Size: " + TEST_SIZE);
        LOG.info("Precision@"+N+" ratio: " + new Stats(precisionList));
        LOG.info("Recall@"+N+" ratio: " + new Stats(recallList));
        LOG.info("fMeasure@"+N+" ratio: " + new Stats(fmeasureList));

        LOG.info(""+new Stats(precisionList).getMean() + " " + new Stats(recallList).getMean() + " " + new Stats(fmeasureList).getMean());
    }

    private void evaluate(Repository repository, List<Double> vector, HierarchicalHashMethod method){
        Map<String,Double> simDocs = repository.getSimilarTo(vector, MAX_RELEVANT);
        Map<Integer, List<String>> hash = method.hash(vector);
        Map<String,Double> relDocs = repository.getSimilarTo(hash, MAX_RELEVANT*2);

        SimilarityResult simResult = new SimilarityResult(simDocs, relDocs);
        Double precision = simResult.getPrecisionAt(N);
        precisionList.add(precision);
        LOG.info("Precision@"+ N +"= " + precision);

        Double recall = simResult.getRecallAt(N);
        recallList.add(recall);
        LOG.info("Recall@"+ N +" = " + recall);

        Double fmeasure = simResult.getFMeasureAt(N);
        fmeasureList.add(fmeasure);
        LOG.info("FMeasure@"+ N +" = " + fmeasure);



//        List<SimilarityResult.PrecisionRecall> curve = simResult.getCurve();
//        for(int i=0;i<MAX_RELEVANT;i++){
//
//            if (!precisionCurve.containsKey(i)) precisionCurve.put(i,new ArrayList<>());
//            if (!recallCurve.containsKey(i)) recallCurve.put(i,new ArrayList<>());
//
//            SimilarityResult.PrecisionRecall item = curve.get(i);
//            precisionCurve.get(i).add(item.getPrecision());
//            recallCurve.get(i).add(item.getRecall());
//
//        }
//        LOG.info("Curve = " + curve);

//        simDocs.entrySet().stream().sorted( (a,b) -> -a.getValue().compareTo(b.getValue())).forEach(entry -> LOG.info("Sim-by-JSD: [" + entry.getValue() + "] - " + entry.getKey()));
//        relDocs.entrySet().stream().sorted( (a,b) -> -a.getValue().compareTo(b.getValue())).forEach(entry -> LOG.info("Sim-by-Hash: [" + entry.getValue() + "] - " + entry.getKey()));

    }

}
