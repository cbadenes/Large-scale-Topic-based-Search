package experiments;

import oeg.lstbs.data.*;
import oeg.lstbs.hash.CentroidHHM;
import oeg.lstbs.hash.DensityHHM;
import oeg.lstbs.hash.HierarchicalHashMethod;
import oeg.lstbs.hash.ThresholdHHM;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

    public class ExplorationExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(ExplorationExperiment.class);


    static final Corpus CORPUS = new Corpus("openresearch-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download");

    static final Integer depth  = 3;

    static final Integer size   = 500000;

    static final List<String> domain = Arrays.asList("t2");

    static final Integer random_index = new Random().nextInt(size);

    @Test
    public void execute(){

        evaluateMethod(CORPUS, new ThresholdHHM(depth), depth);
        evaluateMethod(CORPUS, new CentroidHHM(depth,1000), depth);
        evaluateMethod(CORPUS, new DensityHHM(depth), depth);
    }

    private void evaluateMethod(Corpus corpus, HierarchicalHashMethod method, Integer depth){
        String repositoryName = corpus.getId()+"_"+depth+"_"+ StringUtils.substringAfterLast(method.getClass().getCanonicalName(),".");
        Index index = new Index(repositoryName, corpus.getPath() , size, method);
        LOG.info("Evaluating method " + method  + " in corpus: " +  corpus + " with depth level equals to " + depth +  "...");

        Map<Integer, List<String>> refHashCode = index.getRepository().getHashcodeOf(random_index, depth);

        BooleanQuery.Builder q1 = index.getRepository().getSimilarToQuery(refHashCode);

        Long q1Hits = index.getRepository().getTotalHitsTo(q1.build());
        LOG.info("Q1 Hits :" + q1Hits);

        BooleanQuery.Builder q2 = q1;

        for (String topic: domain){
            Query termQuery             = new TermQuery(new Term("hash0",topic));
            BooleanClause booleanClause = new BooleanClause(termQuery, BooleanClause.Occur.MUST);
            q2.add(booleanClause);
        }

        Long q2Hits = index.getRepository().getTotalHitsTo(q2.build());
        LOG.info("Q2 Hits :" + q2Hits);


        double reduction = q1Hits - q2Hits;
        double ratio = (reduction * 100.0) / q1Hits;
        LOG.info("Reduction: " + ratio);


    }


}
