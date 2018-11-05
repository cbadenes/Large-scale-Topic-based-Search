package experiments;

import oeg.lstbs.data.Document;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.metrics.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 * Open Research Corpus: https://labs.semanticscholar.org/corpus/
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class TopicBasedSimilarityExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(TopicBasedSimilarityExperiment.class);


    SimilarityMetric metric = new Hellinger();

    long seed = 1546;

    int numPairs    = 10; // 10

    List<String> models = Arrays.asList(
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download",    // 100 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/nRWxmYH6AjHqA6N/download",    // 200 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download",    // 300 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KYtEqTB8zyaKb8N/download",    // 400 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download",    // 500 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/7wYmtWNiYnbHoZP/download",    // 600 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/xF7K3jtnTaWFjrN/download",    // 700 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download",    // 800 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/TQK5aHWREPSdDLQ/download",    // 900 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download",     // 1000 topics
            "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/MG7Zn94J4xTaRqL/download"     // 1100 topics
    );

    @Test
    public void execute() throws IOException {


        List<String> points = new ArrayList<>();
        // Load test points
        BufferedReader r = ReaderUtils.from(models.get(0));
        String l;
        while ( (l = r.readLine()) != null){
            String id = l.split(",")[0];
            if (Math.abs(id.hashCode() - seed) % 10 == 0) {
                points.add(id);
                LOG.info("Article '" + id + "' added to test set");
            }
            if (points.size() >= (2*numPairs)) break;

        }


        System.out.println("Topics\t"+ IntStream.range(0,numPairs).mapToObj(i -> "Pair"+i).collect(Collectors.joining("\t")));
        int index = 1;
        for(String model: models){

            Map<String,List<Double>> vectors = new HashMap<>();

            BufferedReader reader = ReaderUtils.from(model);
            String line;
            while ( (line = reader.readLine()) != null){
                String id = line.split(",")[0];
                if (points.contains(id)){
                    vectors.put(id, Document.from(line).getVector());
                }
                if (vectors.size() >= (2*numPairs)) break;
            }
            reader.close();


            List<Double> scoreList = new ArrayList<>();
            for(int i=0; i<(points.size()-1);i=i+2){

                List<Double> v1 = vectors.get(points.get(i));
                List<Double> v2 = vectors.get(points.get(i+1));

                scoreList.add(metric.compare(v1, v2));
            }

            int topics = 100 * index++;
            System.out.println(topics +"\t" + scoreList.stream().map(s -> ""+s).collect(Collectors.joining("\t")));

        }


    }
}
