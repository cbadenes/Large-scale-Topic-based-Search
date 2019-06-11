package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class SimilarityResult {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityResult.class);
    private final Map<String,Double> goldstandard;
    private final Map<String,Double> returned;
    private final Double maxScore;
    private final Double minScore;


    public SimilarityResult(Map<String,Double> referenced, Map<String,Double> returned) {
        this.goldstandard = referenced;
        this.maxScore = referenced.entrySet().stream().map(e -> e.getValue()).reduce((a,b) -> (a>b)? a: b).get();
        this.minScore = referenced.entrySet().stream().map(e -> e.getValue()).reduce((a,b) -> (a<b)? a: b).get();
        this.returned   = returned;
    }

    public Double getPrecisionAt(Integer n){
        Double tp = Double.valueOf(returned.entrySet().stream().sorted((a,b) -> -a.getValue().compareTo(b.getValue())).limit(n).filter(e -> goldstandard.containsKey(e.getKey())).count());
        Double fp = n - tp;

        double score = tp / (tp + fp);
        LOG.info("P@"+n+"=" + score + " / maxScore=" + maxScore + " / minScore=" + minScore);

        return score;
    }

    public Double getRecallAt(Integer n){
        Double tp = Double.valueOf(returned.entrySet().stream().sorted((a,b) -> -a.getValue().compareTo(b.getValue())).limit(n).filter(e -> goldstandard.containsKey(e.getKey())).count());
        return tp / Double.valueOf(goldstandard.size());
    }

    public Double getFMeasureAt(Integer n){
        Double precision    = getPrecisionAt(n);
        Double recall       = getRecallAt(n);

        if (precision+recall == 0.0) return 0.0;

        return 2 * ( (precision*recall) / (precision+recall) );
    }


    public List<PrecisionRecall> getCurve(){
        List<PrecisionRecall> curve = new ArrayList<>();

        List<Map.Entry<String, Double>> sortedValues = returned.entrySet().stream().sorted((a, b) -> -a.getValue().compareTo(b.getValue())).collect(Collectors.toList());

        Double tp = 0.0;

        for(int i=0;i<sortedValues.size();i++){

            tp = goldstandard.containsKey(sortedValues.get(i).getKey())? tp+1 : tp;
            curve.add(new PrecisionRecall(tp/Double.valueOf(i+1),tp/Double.valueOf(goldstandard.size())));
        }

        return curve;
    }

    public class PrecisionRecall{
        double precision;
        double recall;

        public PrecisionRecall(double precision, double recall) {
            this.precision = precision;
            this.recall = recall;
        }

        public double getPrecision() {
            return precision;
        }

        public double getRecall() {
            return recall;
        }

        @Override
        public String toString() {
            return "{" +
                    "p=" + precision +
                    ",r=" + recall +
                    '}';
        }
    }


}
