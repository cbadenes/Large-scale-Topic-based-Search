import oeg.lstbs.data.Repository;
import oeg.lstbs.hash.CentroidHHM;
import oeg.lstbs.hash.DensityHHM;
import oeg.lstbs.hash.HierarchicalHashMethod;
import oeg.lstbs.hash.ThresholdHHM;
import oeg.lstbs.io.VectorReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class IndexBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(IndexBuilder.class);

//    public static final String CORPUS  = "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/aMBsQaTM4oBi3Ga/download"; // cordis-70

    public static final String CORPUS   = "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/WWHprbHxWigBMEC/download"; // cordia-150

    public static final Integer MAX    = 10000;

    public static final Integer DEPTH  = 2;

    @Test
    public void create(){

        // create threshold-based lucene index
        createBy("threshold-based", new ThresholdHHM(DEPTH));

        // create centroid-based lucene index
        createBy("centroid-based", new CentroidHHM(DEPTH,1000));

        // create density-based lucene index
        createBy("density-based", new DensityHHM(DEPTH));

    }

    private void createBy(String repId, HierarchicalHashMethod method){
        LOG.info("Creating index for " + repId + " hash algorithm");
        Repository repository = new Repository(repId);
        VectorReader.VectorAction action = (id, vector) -> repository.add(id, method.hash(vector),vector);
        VectorReader.VectorValidation predicate = (id, vector) -> true;
        VectorReader.from(CORPUS, 0, action, predicate,  Double.valueOf(Math.ceil(Double.valueOf(MAX) / 100.0)).intValue(), MAX);
        repository.close();
    }

}
