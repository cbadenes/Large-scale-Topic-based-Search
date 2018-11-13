package tools;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import oeg.lstbs.data.Evaluation;
import oeg.lstbs.io.ReaderUtils;
import oeg.lstbs.io.WriterUtils;
import org.apache.commons.lang.StringUtils;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class AnalyzeEvaluations {

    private static final Logger LOG = LoggerFactory.getLogger(AnalyzeEvaluations.class);

    private static final String TEST = "searching-1542051150459-evaluations.jsonl.gz";

    private static final List<Integer> accuracies     = Arrays.asList(0,5,10,20);
    private BufferedWriter tableWriter;

    @Before
    public void setup() throws IOException {
        String testType = StringUtils.substringBefore(TEST,"-");
        String time     = StringUtils.substringBetween(TEST,"-","-");
        this.tableWriter    = WriterUtils.to(Paths.get("results",testType+"-" + time + "-tables-from-analysis.md.gz").toFile().getAbsolutePath());
    }

    @After
    public void close() throws IOException {
        this.tableWriter.close();
    }

    @Test
    public void execute() throws IOException {

        ObjectMapper jsonMapper = new ObjectMapper()
                .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule()); // new module, NOT JSR310Module



        BufferedReader reader = ReaderUtils.from("results/" + TEST);
        String row;
        Map<String, List<Evaluation>> results = new HashMap<>();
        AtomicInteger counter = new AtomicInteger();
        try{
            while ((row = reader.readLine()) != null) {
                Evaluation evaluation = jsonMapper.readValue(row, Evaluation.class);
                String id = evaluation.getMetric() + "-" + evaluation.getCorpus();
                if (!results.containsKey(id)) {
                    results.put(id, new ArrayList<>());
                }
                results.get(id).add(evaluation);
                if (counter.incrementAndGet() % 100 == 0) LOG.info(counter.get() + " evaluations analyzed");
            }

        }catch (Exception e){
            LOG.warn("Error reading file: " + e.getMessage());
        }

        LOG.info("Creating result tables ..");

        for (String tableName : results.keySet().stream().sorted().collect(Collectors.toList())) {
            createTable(tableName + "-mAP", results.get(tableName), algorithm -> algorithm.contains("@0"), eval -> String.valueOf(eval.getAveragePrecision()));
            createTable(tableName + "-efficiency", results.get(tableName), algorithm -> algorithm.contains("@0"), eval -> String.valueOf(eval.getEfficiency()));
            for (Integer accuracy : accuracies.stream().filter(a -> a > 0).sorted().collect(Collectors.toList())) {
                createTable(tableName + "-fMeasure@" + accuracy, results.get(tableName), algorithm -> algorithm.contains("@" + accuracy), eval -> String.valueOf(eval.getFMeasure()));
                createTable(tableName + "-precision@" + accuracy, results.get(tableName), algorithm -> algorithm.contains("@" + accuracy), eval -> String.valueOf(eval.getPrecision()));
                createTable(tableName + "-recall@" + accuracy, results.get(tableName), algorithm -> algorithm.contains("@" + accuracy), eval -> String.valueOf(eval.getRecall()));
            }
        }
    }

    private void createTable(String name, List<Evaluation> evals, Predicate<String> filter, Function<Evaluation, String> value) throws IOException {

        Map<Integer,List<String>> table = new HashMap<>();

        int columnId = 0;
        table.put(columnId++, Arrays.asList("topics","100","300","500","800","1000"));

        // group by algorithm
        Map<String, List<Evaluation>> algEvals = evals.stream().collect(Collectors.groupingBy(Evaluation::getAlgorithm));

        for(String alg : algEvals.keySet().stream().filter(filter).sorted().collect(Collectors.toList())){

            List<String> column = new ArrayList<>();
            column.add(StringUtils.substringBefore(alg,"@"));
            column.addAll(algEvals.get(alg).stream().map(value).collect(Collectors.toList()));
            table.put(columnId++,column);
        }

        printTable(name, table);
    }


    private void printTable(String name, Map<Integer,List<String>> table) throws IOException {
        System.out.println("#"+name);
        tableWriter.write("#"+name+"\n");
        for( int i=0; i< table.get(0).size();i++){

            final int index = i;
            String rowString = table.entrySet().stream().sorted((a, b) -> a.getKey().compareTo(b.getKey())).map(entry -> entry.getValue().get(index)).collect(Collectors.joining("\t"));
            System.out.println(rowString);
            tableWriter.write(rowString+"\n");
        }
    }

}
