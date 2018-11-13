package experiments;

import oeg.lstbs.data.*;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.io.WriterUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class GoldStandardExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(GoldStandardExperiment.class);


    List<Corpus> corpora = new ArrayList();

    @Before
    public void setup() throws IOException {


        // Cordis
        corpora.add(new Corpus("Cordis_100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ZG7TtJmbmRi9LDe/download"));
        corpora.add(new Corpus("Cordis_200","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/dyCEmY8TKdSMWn6/download"));
        corpora.add(new Corpus("Cordis_300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Ac5irmmxSkdgyGw/download"));
        corpora.add(new Corpus("Cordis_400","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/LaMRrzcXyD2opgs/download"));
        corpora.add(new Corpus("Cordis_500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/tfQFqAYL9XS5NB6/download"));
        corpora.add(new Corpus("Cordis_600","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/Hps7z49cYFwGo3J/download"));
        corpora.add(new Corpus("Cordis_700","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/ywQNySzFdrqcqQx/download"));
        corpora.add(new Corpus("Cordis_800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KEE3fyFkM7Wq6Zq/download"));
        corpora.add(new Corpus("Cordis_900","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/56bWjK7pk7sQcNS/download"));
        corpora.add(new Corpus("Cordis_1000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/87ega8bYMZH8T62/download"));

//        // OpenResearchCorpus
//        corpora.add(new Corpus("OpenResearch_100","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/fd9XkHNHX5D8C3Y/download"));
//        corpora.add(new Corpus("OpenResearch_200","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/nRWxmYH6AjHqA6N/download"));
//        corpora.add(new Corpus("OpenResearch_300","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/RWgGDE2TKZZqcJc/download"));
//        corpora.add(new Corpus("OpenResearch_400","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/KYtEqTB8zyaKb8N/download"));
//        corpora.add(new Corpus("OpenResearch_500","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download"));
//        corpora.add(new Corpus("OpenResearch_600","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/7wYmtWNiYnbHoZP/download"));
//        corpora.add(new Corpus("OpenResearch_700","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/xF7K3jtnTaWFjrN/download"));
//        corpora.add(new Corpus("OpenResearch_800","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/d94eCryDqbZtMd4/download"));
//        corpora.add(new Corpus("OpenResearch_900","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/TQK5aHWREPSdDLQ/download"));
//        corpora.add(new Corpus("OpenResearch_1000","https://delicias.dia.fi.upm.es/nextcloud/index.php/s/zSgR5H4CsPnPmHG/download"));

    }

    @After
    public void close() throws IOException {
    }

    @Test
    public void execute() throws IOException, InterruptedException {

        int corpusSize          = -1;
        int testSize            = 10;
        int maxSimilar          = 20;
        int seed                = corpusSize>0? corpusSize/testSize : 11;

        // Create test-set
        BufferedReader reader = ReaderUtils.from(corpora.get(0).getPath());
        String row;
        List<String> testSet = new ArrayList<>();
        while((row = reader.readLine()) != null){
            String id = row.split(",")[0];
            if (id.hashCode() % seed == 0) {
                testSet.add(id);
                LOG.info("Set '"+id+"' as test document");
            }
            if (testSet.size() >= testSize) break;
        }

        // Create raters
        List<Rater> raters = corpora.stream().map(corpus -> new Rater(corpus, corpusSize)).collect(Collectors.toList());

        // Add test documents
        raters.forEach(r -> LOG.info("Rater '" + r.getId()+"' size = " + r.getSize()));
        testSet.forEach(d -> raters.parallelStream().forEach(r -> r.add(d,maxSimilar)));
        List<Integer> categories    = Arrays.asList(0,1);
        List<String> pairs          = raters.stream().flatMap(r -> r.getPairs().stream()).distinct().collect(Collectors.toList());
        raters.forEach(rater -> rater.add(pairs));
        // Pi
        List<Double> piList = new ArrayList<>();
        for(int i=0;i<pairs.size();i++){
            String pair = pairs.get(i);
            Map<Integer, Long> result = raters.stream().map(rater -> rater.rate(pair)).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            double sumPi = result.entrySet().stream().map(e -> e.getValue()).map(v -> Math.pow(v,2)).reduce((a,b) -> a+b).get();
            double den = raters.size() * (raters.size()-1);
            double Pi = (sumPi - raters.size())*(1.0 / den);
            piList.add(Pi);
        }


        // pj
        List<Double> pjList = new ArrayList<>();
        for(int i=0;i<categories.size();i++){
            Integer category = categories.get(i);
            Map<String, Long> result = raters.stream().flatMap(rater -> rater.getByCategory(category).stream()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            long sum = (result.isEmpty())? 0l : result.entrySet().stream().map(e -> e.getValue()).reduce((a,b) -> a+b).get();
            double den = raters.size() * pairs.size();
            double pj = Double.valueOf(sum) / Double.valueOf(den);
            pjList.add(pj);
        }

        Double sumPi    = piList.stream().reduce((a, b) -> a + b).get();
        Double P        = sumPi / Double.valueOf(pairs.size());
        Double sumPj    = pjList.stream().map(v -> Math.pow(v, 2)).reduce((a, b) -> a + b).get();

        double fleissKappa = (P - sumPj) / (1 - sumPj);
        LOG.info("FleissKappa = " + fleissKappa);

        Map<String,List<String>> goldStandard = new HashMap<>();
        for(Rater rater: raters){
            List<String> simPairs = rater.getByCategory(1);
            for(String simPair: simPairs){
                String[] values = simPair.split("-");
                String d1 = values[0];
                String d2 = values[1];
                if (!goldStandard.containsKey(d1)){
                    goldStandard.put(d1,new ArrayList<>());
                }
                goldStandard.get(d1).add(d2);
            }
        }

        String corpusName = StringUtils.substringBefore(corpora.get(0).getId(),"_");
        BufferedWriter writer = WriterUtils.to("results/goldstandard-" + corpusName +"-"+testSize+".csv.gz");
        for(String d1: goldStandard.keySet()){
            Map<String, Long> simDocs = goldStandard.get(d1).stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

            for(String d2 : simDocs.keySet()){
                double ratio = Double.valueOf(simDocs.get(d2)) / Double.valueOf(raters.size());
                String simRow = d1+","+d2+","+ratio+","+fleissKappa;
                System.out.println(simRow);
                writer.write(simRow+"\n");
            }

        }
        writer.close();


    }

}
