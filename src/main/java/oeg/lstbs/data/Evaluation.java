package oeg.lstbs.data;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class Evaluation {

    private String corpus;

    private String model;

    private String metric;

    private String algorithm;

    private long truePositive    = 0;

    private long falsePositive   = 0;

    private long falseNegative   = 0;

    private Instant start;

    private Instant end;

    private Double precision;

    private Double recall;

    private double efficiency  = 0.0;

    private String description  = "";

    private List<String> totalHits    = new ArrayList<>();

    private double averagePrecision = 0.0;

    private Long elapsedTime;

    private Double fMeasure;

    public double getAveragePrecision() {
        return averagePrecision;
    }

    public void setAveragePrecision(double averagePrecision) {
        this.averagePrecision = averagePrecision;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public synchronized void addResult(List<String> reference, List<String> value){
        if (reference.isEmpty()){
            recall = 1.0;
            if (value.isEmpty()){
                precision = 1.0;
            }
        }

        truePositive    += value.stream().filter( e -> reference.contains(e)).count();

        falsePositive   += value.stream().filter( e -> !reference.contains(e)).count();

        falseNegative   += reference.stream().filter( e -> !value.contains(e)).count();

        totalHits.add(value.size()+"/"+reference.size());

    }

    public double getEfficiency() {
        return efficiency;
    }

    public void setEfficiency(double efficiency) {
        this.efficiency = efficiency;
    }

    public Instant getStart() {
        return start;
    }

    public void setStart(InstantTime time){
        this.start = Instant.ofEpochSecond(time.getEpochSecond(), time.getNano());
    }

    public void setTime(Instant start, Instant end){
        this.start = start;
        this.end = end;
    }

    public Instant getEnd() {
        return end;
    }

    public void setEnd(InstantTime time){
        this.end = Instant.ofEpochSecond(time.getEpochSecond(), time.getNano());
    }

    public Double getPrecision(){

        if (precision != null) return precision;

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

        if (recall != null) return recall;

        double positive = (Double.valueOf(truePositive)+ Double.valueOf(falseNegative));

        if (positive == 0.0) return 0.0;

        return Double.valueOf(truePositive) / positive;
    }

    public Double getFMeasure(){

        if (fMeasure != null) return fMeasure;

        Double precision = getPrecision();
        Double recall = getRecall();
        if ((precision == 0) && (recall == 0)) return 0.0;
        return 2 * (precision*recall) / (precision+recall);
    }

    public String getTotalHits() {
        return totalHits.stream().collect(Collectors.joining("_"));
    }

    public String getCorpus() {
        return corpus;
    }

    public void setCorpus(String corpus) {
        this.corpus = corpus;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public long getElapsedTime() {
        if ( elapsedTime != null) return elapsedTime;
        return ChronoUnit.MILLIS.between(start, end) % 1000;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public void setFMeasure(double value){
        this.fMeasure = value;
    }

    public void setTotalHits(List<String> totalHits) {
        this.totalHits = totalHits;
    }

    @Override
    public String toString() {
        return "Evaluation{" +
                "description="      + description +
                ", algorithm="      + algorithm +
                ", corpus="         + corpus +
                ", model="          + model +
                ", truePositive="   + truePositive +
                ", falsePositive="  + falsePositive +
                ", falseNegative="  + falseNegative +
                ", precision="      + getPrecision()+
                ", recall="         + getRecall() +
                ", fMeasure="       + getFMeasure() +
                ", averagePrecision="     + averagePrecision+
                ", efficiency="       + getEfficiency() +
                ", total-hits="       + getTotalHits() +
                ", elapsedTime="       + ((start!= null && end!=null)?  Time.print(start,end,"") : "")+
                '}';
    }
}
