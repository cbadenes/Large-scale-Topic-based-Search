package oeg.lstbs.data;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class Evaluation {

    private long truePositive    = 0;

    private long falsePositive   = 0;

    private long falseNegative   = 0;

    private Instant start;

    private Instant end;

    private double precision    = 0.0;

    private double recall       = 0.0;

    private double improvement  = 0.0;

    private String description  = "";

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public synchronized void addResult(List<String> reference, List<String> value){

        truePositive    += value.stream().filter( e -> reference.contains(e)).count();

        falsePositive   += value.stream().filter( e -> !reference.contains(e)).count();

        falseNegative   += reference.stream().filter( e -> !value.contains(e)).count();

    }

    public double getImprovement() {
        return improvement;
    }

    public void setImprovement(double improvement) {
        this.improvement = improvement;
    }

    public Instant getStart() {
        return start;
    }

    public void setStart(Instant start) {
        this.start = start;
    }

    public Instant getEnd() {
        return end;
    }

    public void setEnd(Instant end) {
        this.end = end;
    }

    public Double getPrecision(){

        if (precision != 0.0) return precision;

        double positive = (Double.valueOf(truePositive) + Double.valueOf(falsePositive));

        if (positive == 0.0) return 0.0;

        return Double.valueOf(truePositive) / positive;
    }

    public Evaluation setPrecision(double precision) {
        this.precision = precision;
        return this;
    }

    public Evaluation setRecall(double recall) {
        this.recall = recall;
        return this;
    }

    public Double getRecall(){

        if (precision != 0.0) return recall;

        double positive = (Double.valueOf(truePositive)+ Double.valueOf(falseNegative));

        if (positive == 0.0) return 0.0;

        return Double.valueOf(truePositive) / positive;
    }

    public Double getFMeasure(){
        Double precision = getPrecision();
        Double recall = getRecall();
        if ((precision == 0) && (recall == 0)) return 0.0;
        return 2 * (precision*recall) / (precision+recall);
    }

    @Override
    public String toString() {
        return "Evaluation{" +
                "description="     + description +
                "truePositive="     + truePositive +
                ", falsePositive="  + falsePositive +
                ", falseNegative="  + falseNegative +
                ", precision="      + getPrecision()+
                ", recall="         + getRecall() +
                ", fMeasure="       + getFMeasure() +
                ", improvement="       + getImprovement() +
                ", elapsedTime="       + ((start!= null && end!=null)?  Time.print(start,end,"") : "")+
                '}';
    }
}
