package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class AveragePrecision {

    private static final Logger LOG = LoggerFactory.getLogger(AveragePrecision.class);


    public static double from(List<String> relevantList, List<String> retrieveList){

        if (retrieveList.isEmpty()) return 0.0;

        Map<String,Integer> groundTruth = new HashMap<>();
        relevantList.forEach(el -> groundTruth.put(el,1));

        int found                   = 0;
        List<Double> precisionList  = new ArrayList<>();
        List<Double> recallList     = new ArrayList<>();
        for(int i=1;i<=retrieveList.size();i++){

            String element = retrieveList.get(i-1);
            boolean isRelevant = groundTruth.containsKey(element);
            found = (isRelevant)? found +1 : found;
            double precision    = Double.valueOf(found) / Double.valueOf(i);
            double recall       = Double.valueOf(found) / Double.valueOf(relevantList.size());

            if (isRelevant){
                precisionList.add(precision);
                recallList.add(recall);
            }

        }
        if (recallList.isEmpty() || (recallList.get(recallList.size()-1) != 1.0)) return 0.0;
        return new Stats(precisionList).getMean();
    }


    public static void main(String[] args) {

        List<String> relevant = Arrays.asList("1","2","3","4","5","6","7","8","9","0");
        List<String> retrieve = Arrays.asList("1","aa","2","3","4","5","6","aa","7","aa","8","aa","aa","9","aa","aa","aa","aa","aa","0");
        LOG.info("average-precision: " + AveragePrecision.from(relevant,retrieve));

    }
}
