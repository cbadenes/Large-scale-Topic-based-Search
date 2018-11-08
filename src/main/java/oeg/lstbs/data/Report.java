package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

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

    public double getMeanAveragePrecision(){
        return new Stats(evaluations.stream().map(e -> e.getAveragePrecision()).collect(Collectors.toList())).getMean();
    }

    public double getPrecision(){
        return new Stats(evaluations.stream().map(e -> e.getPrecision()).collect(Collectors.toList())).getMean();
    }

    public double getRecall(){
        return new Stats(evaluations.stream().map(e -> e.getRecall()).collect(Collectors.toList())).getMean();
    }

    public double getFMeasure(){
        return new Stats(evaluations.stream().map(e -> e.getFMeasure()).collect(Collectors.toList())).getMean();
    }

    public double getEfficiency(){
        return new Stats(evaluations.stream().map(e -> e.getEfficiency()).collect(Collectors.toList())).getMean();
    }

    public String getTotalHits() {
        return evaluations.stream().map(e -> e.getTotalHits()).collect(Collectors.joining("|"));
    }
}
