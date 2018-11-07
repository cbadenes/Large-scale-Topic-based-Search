package oeg.lstbs.metrics;

import cc.mallet.util.Maths;
import com.google.common.primitives.Doubles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class JSD implements ComparisonMetric {

    private static final Logger LOG = LoggerFactory.getLogger(JSD.class);


    @Override
    public String id() {
        return "JSD";
    }

    @Override
    public Double distance(List<Double> v1, List<Double> v2) {
        assert (v1.size() == v2.size());

        List<Double> avg = new ArrayList<>();
        for(int i=0;i<v1.size();i++){
            Double pq = v1.get(i) + v2.get(i);
            Double pq_2 = pq / 2.0;
            avg.add(pq_2);
        }

        return (0.5 * new KL().distance(v1,avg)) + (0.5 * new KL().distance(v2, avg));
    }

    @Override
    public Double similarity(List<Double> v1, List<Double> v2) {
        return 1-divergence(v1,v2);
    }

    private double divergence(List<Double> v1, List<Double> v2) {
        return Maths.jensenShannonDivergence(Doubles.toArray(v1),Doubles.toArray(v2));

    }

}
