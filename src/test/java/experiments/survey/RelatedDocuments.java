package experiments.survey;

import com.fasterxml.jackson.databind.ObjectMapper;
import oeg.lstbs.data.*;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.io.WriterUtils;
import oeg.lstbs.metrics.Hellinger;
import oeg.lstbs.metrics.JSD;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

    public class RelatedDocuments {

    private static final Logger LOG = LoggerFactory.getLogger(RelatedDocuments.class);

    private static final Integer TOP_N = 5;

    private static final String CORPUS  = IndexDocuments.TEXTSET.getCorpus().getId();
    private static final String DATASET = IndexDocuments.DATASET.getCorpus().getId();

    private final Random random = new Random();

    private ObjectMapper jsonMapper;
    private BufferedReader reader;
    private BufferedWriter relDocsWriter;
    private BufferedWriter relationsWriter;

    @Before
    public void setup() throws IOException {

        jsonMapper  = new ObjectMapper();
        reader = ReaderUtils.from(Paths.get("survey", DATASET, "query-docs.jsonl").toFile().getAbsolutePath(), false);
        relDocsWriter = WriterUtils.to(Paths.get("survey", DATASET, "rel-docs.jsonl").toFile().getAbsolutePath());
        relationsWriter = WriterUtils.to(Paths.get("survey", DATASET, "relations.jsonl").toFile().getAbsolutePath());
    }

    @After
    public void close() throws IOException {
        reader.close();
        relDocsWriter.close();
        relationsWriter.close();
    }


    @Test
    public void execute() throws IOException {

        try{

            // Load repositories
            Repository densityRepository    = new Repository(DATASET + "-density");
            Repository centroidRepository   = new Repository(DATASET + "-centroid");
            Repository txtRepository        = new Repository(CORPUS);

            String row = null;

            while((row = reader.readLine()) != null){

                TopicDistribution td = jsonMapper.readValue(row, TopicDistribution.class);

                String queryDoc = td.getDocId();

                Set<String> relatedDocs = new TreeSet<>();
                relatedDocs.add(queryDoc);

//                // by density
                LOG.info("getting similar docs by density..");
                relatedDocs.addAll(getAndSaveRelatedDocs(densityRepository, queryDoc, "density"));

                // by centroid
                LOG.info("getting similar docs by centroid..");
                relatedDocs.addAll(getAndSaveRelatedDocs(centroidRepository, queryDoc, "centroid"));

                // by JSD
                LOG.info("getting similar docs by JSD..");
                relatedDocs.addAll(save(queryDoc,densityRepository.getSimilarTo(td.getVector(), TOP_N+1, new JSD()), "jensen-shannon"));

                // by Hellinger
                LOG.info("getting similar docs by Hellinger..");
                relatedDocs.addAll(save(queryDoc,densityRepository.getSimilarTo(td.getVector(), TOP_N+1, new Hellinger()), "hellinger"));

                // by BM25
                LOG.info("getting similar docs by bm25..");
                Integer dId = txtRepository.getDocumentIdBy(queryDoc).get();
                Map<String, Double> rels = txtRepository.getMoreLikeThis(dId, new String[]{"txt"}, TOP_N);
                relatedDocs.addAll(save(queryDoc, rels, "bm25"));

                for(String relId : relatedDocs){
                    Text text = new Text();
                    text.setId(relId);
                    Optional<Document> d = txtRepository.getDocumentBy(relId);
                    if (!d.isPresent()){
                        LOG.warn("No document text found by id: " + relId);
                        continue;
                    }
                    String content  = d.get().getField("txt").stringValue();
                    String name     = d.get().getField("name").stringValue();
                    text.setContent(content);
                    text.setName(name);
                    relDocsWriter.write(jsonMapper.writeValueAsString(text)+"\n");
                }
            }


        }finally {
            relDocsWriter.close();
            relationsWriter.close();
            reader.close();
        }

    }


    private List<String> getAndSaveRelatedDocs(Repository repository, String queryDoc, String metric){

        Optional<Document> doc = repository.getDocumentBy(queryDoc);

        if (!doc.isPresent()){
            LOG.warn("Document '" + queryDoc + "' is missing!!");
            return Collections.emptyList();
        }

        Map<Integer,List<String>> topics = new HashMap<>();

        Document document = doc.get();
        for(int i=0;i<3;i++){
            String hash = document.getField("hash" + i).stringValue();
            topics.put(i,Arrays.asList(hash.split(" ")));
        }


        Map<String, Double> relatedDocs = repository.getSimilarTo(topics, TOP_N+1);
        return save(queryDoc, relatedDocs, metric);
    }

    private List<String> save(String queryDoc, Map<String,Double> relDocs, String metric){

        List<String> docs = new ArrayList<>();
        for(String relDoc : relDocs.entrySet().stream().filter(e -> !e.getKey().equalsIgnoreCase(queryDoc)).sorted((a,b) -> -a.getValue().compareTo(b.getValue())).map(e -> e.getKey()).collect(Collectors.toList())){
            docs.add(relDoc);
            Relation relationship = new Relation();
            relationship.setQueryDoc(queryDoc);
            relationship.setRelDoc(relDoc);
            relationship.setScore(relDocs.get(relDoc));
            relationship.setMetric(metric);
            try {
                relationsWriter.write(jsonMapper.writeValueAsString(relationship)+"\n");
            } catch (IOException e) {
                LOG.error("Unexpected error",e);
            }
        }
        return docs;
    }

    private void writeRelatedDoc(String relDoc){


    }


}
