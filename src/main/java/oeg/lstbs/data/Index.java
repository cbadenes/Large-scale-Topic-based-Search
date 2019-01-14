package oeg.lstbs.data;

import oeg.lstbs.hash.HierarchicalHashMethod;
import oeg.lstbs.io.VectorReader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class Index {

    private static final Logger LOG = LoggerFactory.getLogger(Index.class);

    private final Repository repository;

    public Index(String id, String path, Integer size, HierarchicalHashMethod method) {

        LOG.info("Creating index for " + id + " with hash algorithm: " + StringUtils.substringAfterLast(method.getClass().getCanonicalName(),"."));
        this.repository = new Repository(id);
        VectorReader.VectorAction action = (x, vector) -> repository.add(x, method.hash(vector),vector);

        Integer interval = size >0? Double.valueOf(Math.ceil(Double.valueOf(size) / 100.0)).intValue() : 100;
        VectorReader.from(path, 0, action, interval, size);
        repository.close();

    }

    public Repository getRepository() {
        return repository;
    }

    public void clean(){
        this.repository.delete();
    }

}
