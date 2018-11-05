package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Document {

    private static final Logger LOG = LoggerFactory.getLogger(Document.class);

    private String id;

    List<Double> vector;

    public Document(String id, List<Double> vector) {
        this.id = id;
        this.vector = vector;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Double> getVector() {
        return vector;
    }

    public void setVector(List<Double> vector) {
        this.vector = vector;
    }


    public static Document from(String row){
        String[] values = row.split(",");
        String id = values[0];
        List<Double> vector = new ArrayList<>();
        for(int i=1; i<values.length; i++){
            vector.add(Double.valueOf(values[i]));
        }
        return new Document(id,vector);
    }

    @Override
    public String toString() {
        return "Document{" +
                "id='" + id + '\'' +
                ", vector=" + vector +
                '}';
    }
}
