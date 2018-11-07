package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Report {

    private static final Logger LOG = LoggerFactory.getLogger(Report.class);
    private final List<Evaluation> evaluations;

    DecimalFormat df = new DecimalFormat("#.#");



    public Report(List<Evaluation> evaluations) {
        this.evaluations = evaluations;


    }

    public double getmAP(){
        Map<Double,Double> curve = new HashMap<>();

        for(Evaluation evaluation: evaluations){
            Double precision    = evaluation.getPrecision();
            Double recall       = evaluation.getRecall();

            Double key          = Double.valueOf(df.format(recall));
            if (!curve.containsKey(key)) {
                curve.put(key,precision);
                continue;
            }

            if (curve.get(key) > precision) continue;

            curve.put(key,precision);
        }

        double sum  = 0.0;
        for(double i=0.0;i <=1.0; i += 0.1){
            Double key          = Double.valueOf(df.format(i));
            Optional<Double> max = curve.entrySet().stream().filter(entry -> entry.getKey() >= key).map(entry -> entry.getValue()).sorted((a, b) -> -a.compareTo(b)).findFirst();
            Double val          = (max.isPresent()? max.get() : 0.0);
            sum += val;

        }
        return sum / 11.0;
    }

    public double getPrecision(){
        return evaluations.stream().map(e -> e.getPrecision()).reduce((a,b) -> a+b).get() / Double.valueOf(evaluations.size());

    }

    public double getRecall(){
        return evaluations.stream().map(e -> e.getRecall()).reduce((a,b) -> a+b).get() / Double.valueOf(evaluations.size());

    }

    public double getFMeasure(){
        return evaluations.stream().map(e -> e.getFMeasure()).reduce((a,b) -> a+b).get() / Double.valueOf(evaluations.size());

    }

    public double getImprovement(){
        return evaluations.stream().map(e -> e.getImprovement()).reduce((a,b) -> a+b).get() / Double.valueOf(evaluations.size());

    }


    public static void main(String[] args) {

        List<Evaluation> evaluations = Arrays.asList(
                new Evaluation().setPrecision(1.0).setRecall(0.2),
                new Evaluation().setPrecision(1.0).setRecall(0.4),
                new Evaluation().setPrecision(0.67).setRecall(0.4),
                new Evaluation().setPrecision(0.5).setRecall(0.4),
                new Evaluation().setPrecision(0.5).setRecall(0.6),
                new Evaluation().setPrecision(0.57).setRecall(0.8),
                new Evaluation().setPrecision(0.5).setRecall(0.8),
                new Evaluation().setPrecision(0.44).setRecall(0.8),
                new Evaluation().setPrecision(0.5).setRecall(1.0)
        );

        LOG.info("mAP = " + new Report(evaluations).getmAP());

    }
}
