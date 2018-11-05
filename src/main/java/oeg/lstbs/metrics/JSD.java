package oeg.lstbs.metrics;

import cc.mallet.util.Maths;
import com.google.common.primitives.Doubles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class JSD implements SimilarityMetric{

    private static final Logger LOG = LoggerFactory.getLogger(JSD.class);


    @Override
    public String id() {
        return "JSD";
    }

    @Override
    public Double compare(List<Double> v1, List<Double> v2) {
        assert (v1.size() == v2.size());

        List<Double> avg = new ArrayList<>();
        for(int i=0;i<v1.size();i++){
            Double pq = v1.get(i) + v2.get(i);
            Double pq_2 = pq / 2.0;
            avg.add(pq_2);
        }

        return (0.5 * new KL().compare(v1,avg)) + (0.5 * new KL().compare(v2, avg));
    }

}
