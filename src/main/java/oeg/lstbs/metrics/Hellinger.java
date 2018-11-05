package oeg.lstbs.metrics;

import cc.mallet.util.Maths;
import com.google.common.primitives.Doubles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Hellinger implements SimilarityMetric{

    private static final Logger LOG = LoggerFactory.getLogger(Hellinger.class);

    @Override
    public String id() {
        return "Hellinger";
    }

    @Override
    public Double compare(List<Double> v1, List<Double> v2) {

        assert (v1.size() == v2.size());

        double sum = 0;
        for(int i=0; i<v1.size(); i++){

            double sqrtv1 = Math.sqrt(v1.get(i));
            double sqrtv2 = Math.sqrt(v2.get(i));

            double pow2 = Math.pow(sqrtv1 - sqrtv2, 2.0);
            sum += pow2;
        }

        return sum;
    }
}
