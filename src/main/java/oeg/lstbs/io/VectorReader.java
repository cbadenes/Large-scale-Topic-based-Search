package oeg.lstbs.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class VectorReader {

    private static final Logger LOG = LoggerFactory.getLogger(VectorReader.class);

    public static void from(String path, Integer offset, VectorAction action, VectorValidation predicate, Integer ratio, Integer max){
        try{
            BufferedReader reader = ReaderUtils.from(path);
            String row;
            AtomicInteger counter = new AtomicInteger();
            AtomicInteger rowNumber = new AtomicInteger();
            ParallelExecutor executor = new ParallelExecutor();
            while((row = reader.readLine()) != null){
                if (rowNumber.incrementAndGet() < offset) continue;
                String[] values = row.split(",");
                String dId = values[0];
                List<Double> vector = Arrays.stream(values).skip(1).mapToDouble(v -> Double.valueOf(v)).boxed().collect(Collectors.toList());
                if (!predicate.isValid(dId, vector)) continue;
                executor.submit(() -> action.handle(dId,vector));
                if (counter.incrementAndGet() % ratio == 0) LOG.debug(counter.get() + " vectors read" );
                if ((max> 0) && (counter.get() >= max)) break;
            }
            executor.awaitTermination(1l, TimeUnit.HOURS);
            reader.close();
            LOG.debug(counter.get() + " vectors finally read" );
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }


    public interface VectorAction {
        void handle(String id, List<Double> vector);
    }

    public interface VectorValidation {
        boolean isValid(String id, List<Double> vector);
    }

}
