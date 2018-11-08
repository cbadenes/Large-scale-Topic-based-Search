package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Result {

    private static final Logger LOG = LoggerFactory.getLogger(Result.class);

    private String corpus;

    private String algorithm;

    private Double mAP;

    private Double precision;

    private Double recall;

    private Double efficiency;

    private Double fMeasure;

    private String total;

    public Result() {
    }

    public String getCorpus() {
        return corpus;
    }

    public void setCorpus(String corpus) {
        this.corpus = corpus;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public Double getmAP() {
        return mAP;
    }

    public void setmAP(Double mAP) {
        this.mAP = mAP;
    }

    public Double getPrecision() {
        return precision;
    }

    public void setPrecision(Double precision) {
        this.precision = precision;
    }

    public Double getRecall() {
        return recall;
    }

    public void setRecall(Double recall) {
        this.recall = recall;
    }

    public Double getEfficiency() {
        return efficiency;
    }

    public void setEfficiency(Double efficiency) {
        this.efficiency = efficiency;
    }

    public Double getfMeasure() {
        return fMeasure;
    }

    public void setfMeasure(Double fMeasure) {
        this.fMeasure = fMeasure;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "Result{" +
                "corpus='" + corpus + '\'' +
                ", algorithm='" + algorithm + '\'' +
                ", mAP=" + mAP +
                ", precision=" + precision +
                ", recall=" + recall +
                ", efficiency=" + efficiency +
                ", fMeasure=" + fMeasure +
                ", total=" + total +
                '}';
    }
}
