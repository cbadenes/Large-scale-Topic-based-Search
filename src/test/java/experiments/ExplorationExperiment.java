package experiments;

import oeg.lstbs.data.*;
import oeg.lstbs.data.Document;
import oeg.lstbs.hash.CentroidHHM;
import oeg.lstbs.hash.DensityHHM;
import oeg.lstbs.hash.HierarchicalHashMethod;
import oeg.lstbs.hash.ThresholdHHM;
import oeg.lstbs.io.WriterUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

    public class ExplorationExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(ExplorationExperiment.class);


    static final Corpus CORPUS = new Corpus("openresearch-100", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download");

    static final Integer depth  = 3;

    static final Integer size   = 10000;

    static List<String> domain = Arrays.asList("t48"); //t2

    static final Integer random_index = new Random().nextInt(size); //1186

    @Test
    public void execute(){

//        evaluateMethod(CORPUS, new ThresholdHHM(depth), depth);
//        evaluateMethod(CORPUS, new CentroidHHM(depth,1000), depth);
        evaluateMethod(CORPUS, new DensityHHM(depth), depth);
    }

    private void evaluateMethod(Corpus corpus, HierarchicalHashMethod method, Integer depth){
        String repositoryName = corpus.getId()+"_"+depth+"_"+ StringUtils.substringAfterLast(method.getClass().getCanonicalName(),".");
        Index index = new Index(repositoryName, corpus.getPath() , size, method);
        LOG.info("Evaluating method " + method  + " in corpus: " +  corpus + " with depth level equals to " + depth +  "...");

        Map<Integer, List<String>> refHashCode = index.getRepository().getHashcodeOf(random_index, depth);

        LOG.info("Document: " + random_index);
        LOG.info("Hash0 reference: " + refHashCode.get(0));


        BooleanQuery.Builder q1 = new BooleanQuery.Builder();
        for(String t : refHashCode.get(0)){
            Query termQuery             = new TermQuery(new Term("hash0",t));
            BooleanClause booleanClause = new BooleanClause(termQuery, BooleanClause.Occur.MUST);
            q1.add(booleanClause);
        }
        for(int i=1;i<refHashCode.size();i++){
            for(String t : refHashCode.get(i)){
                Integer boost               = refHashCode.size()-i;
                Query termQuery             = new TermQuery(new Term("hash"+i,t));
                Query boostedQuery          = new BoostQuery(termQuery,boost*boost);
                BooleanClause booleanClause = new BooleanClause(boostedQuery, BooleanClause.Occur.SHOULD);
                q1.add(booleanClause);
            }
        }

//        BooleanQuery.Builder q1 = index.getRepository().getSimilarToQuery(refHashCode);


        Map<String, Integer> q1Map = saveResult(index.getRepository(), q1.build());


        List<String> keys = q1Map.entrySet().stream().sorted((a, b) -> -a.getValue().compareTo(b.getValue())).limit(2).map(e -> e.getKey()).collect(Collectors.toList());

        Long q1Hits = index.getRepository().getTotalHitsTo(q1.build());
        LOG.info("Q1 Hits :" + q1Hits);

        BooleanQuery.Builder q2 = q1;

        domain = new ArrayList();
        domain.add(keys.get(1));
        LOG.info("Domain Topic/s: " + domain);

        for (String topic: domain){
            Query termQuery             = new TermQuery(new Term("hash0",topic));
            BooleanClause booleanClause = new BooleanClause(termQuery, BooleanClause.Occur.MUST);
            q2.add(booleanClause);
        }

        Map<String, Integer> q2Map = saveResult(index.getRepository(), q2.build());

        Long q2Hits = index.getRepository().getTotalHitsTo(q2.build());
        LOG.info("Q2 Hits :" + q2Hits);


        double reduction = q1Hits - q2Hits;
        double ratio = (reduction * 100.0) / q1Hits;
        LOG.info("Reduction: " + ratio);


        File output = Paths.get("results", "exploration.md").toFile();
        if (output.exists()) output.delete();
        output.getParentFile().mkdirs();
        try {
            BufferedWriter writer = WriterUtils.to(output.getAbsolutePath());
            Double accQ1 = Double.valueOf(q1Map.entrySet().stream().map(e -> e.getValue()).reduce((a, b) -> a + b).get());
            Double accQ2 = Double.valueOf(q2Map.entrySet().stream().map(e -> e.getValue()).reduce((a, b) -> a + b).get());
            writer.write("Topic\tQ1\tQ2\n");
            for(String topic: q1Map.keySet().stream().sorted( (a,b) -> Integer.valueOf(a.replace("t","0")).compareTo(Integer.valueOf(b.replace("t","0")))).collect(Collectors.toList())){
                double r1 =  (Double.valueOf(q1Map.get(topic)) * 100.0) / accQ1;
                double r2 = (q2Map.containsKey(topic)? (Double.valueOf(q2Map.get(topic)) * 100.0) / accQ2 : 0.0);
                writer.write(topic + "\t" + r1 + "\t" + r2 +"\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    private Map<String, Integer> saveResult(Repository repository, Query query){
        TopDocs q1Results = repository.getBy(query, repository.getSize());

        HashMap<String, Integer> q1Map = new HashMap<String, Integer>();
        for(int i=0;i<q1Results.totalHits;i++){
            ScoreDoc d = q1Results.scoreDocs[i];
            org.apache.lucene.document.Document doc = repository.getDocument(d.doc);
            String hash0 = doc.get("hash0");
            for(String topic: hash0.split(" ")){
                if (!q1Map.containsKey(topic)) q1Map.put(topic,0);
                q1Map.put(topic, q1Map.get(topic)+1);
            }
        }

        return q1Map;
    }

}
