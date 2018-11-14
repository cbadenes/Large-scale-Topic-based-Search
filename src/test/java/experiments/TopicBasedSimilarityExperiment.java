package experiments;

import oeg.lstbs.data.Document;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.metrics.*;
import org.junit.Before;
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

    long seed = 1546;

    int numPairs    = 10; // 10

    ComparisonMetric metric = new S2JSD();

    Map<Integer,String> topicModels = new HashMap<>();

    @Before
    public void setup() {
        topicModels.put(10, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/inDm3rNkZABoBPS/download");
        topicModels.put(20, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/FBXEEfb6iQrSKFP/download");
        topicModels.put(30, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ob4NBfpXiMRJKME/download");
        topicModels.put(40, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/gnbbFaCwHT2CxtR/download");
        topicModels.put(50, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/rqGWCbbKiMQKyMq/download");
        topicModels.put(60, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/mQzaTN5ji6B8NXF/download");
        topicModels.put(70, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/X4yKFaEnqspyFKz/download");
        topicModels.put(80, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/eaFGSCbQtmmTf34/download");
        topicModels.put(90, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/xHK632ddrrFmtMm/download");
        topicModels.put(100, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download");
        topicModels.put(200, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/nRWxmYH6AjHqA6N/download");
        topicModels.put(300, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download");
        topicModels.put(400, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KYtEqTB8zyaKb8N/download");
        topicModels.put(500, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download");
        topicModels.put(600, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/7wYmtWNiYnbHoZP/download");
        topicModels.put(700, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/xF7K3jtnTaWFjrN/download");
        topicModels.put(800, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download");
        topicModels.put(900, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/TQK5aHWREPSdDLQ/download");
        topicModels.put(1000, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download");
        topicModels.put(1100, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/MG7Zn94J4xTaRqL/download");
        topicModels.put(1200, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/i8P5JKJ5RCZG3Xe/download");
        topicModels.put(1300, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/SaPFLK2gb2KiCMr/download");
        topicModels.put(1400, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/aaaCQpwQRKgyFQ3/download");
        topicModels.put(1500, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/PCFjBFfwfzf2WM9/download");
        topicModels.put(1600, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/8ifFKeypCsdttCs/download");
        topicModels.put(1700, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ntM5KzW6X9m5kcW/download");
        topicModels.put(1800, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/R3aztCF49xykeLS/download");
        topicModels.put(1900, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/FGBLtESCEr7SAbN/download");
        topicModels.put(2000, "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/itkmdeESJeLQPXY/download");
    }

    @Test
    public void execute() throws IOException {


        List<String> points = new ArrayList<>();
        // Load test points
        BufferedReader r = ReaderUtils.from(topicModels.entrySet().iterator().next().getValue());
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
        for(Integer topics: topicModels.keySet().stream().sorted((a,b) -> a.compareTo(b)).collect(Collectors.toList())){

            String model = topicModels.get(topics);
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

                scoreList.add(metric.distance(v1, v2));
            }

            System.out.println(topics +"\t" + scoreList.stream().map(s -> ""+s).collect(Collectors.joining("\t")));

        }


    }
}
