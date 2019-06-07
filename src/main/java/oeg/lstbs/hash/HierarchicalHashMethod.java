package oeg.lstbs.hash;

import java.util.List;
import java.util.Map;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public interface HierarchicalHashMethod {

    String id();

    Map<Integer,List<String>> hash(List<Double> topicDistribution);
}
