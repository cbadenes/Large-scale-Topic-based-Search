package experiments.survey;

import oeg.lstbs.data.*;
import oeg.lstbs.hash.CentroidHHM;
import oeg.lstbs.hash.DensityHHM;
import oeg.lstbs.hash.HierarchicalHashMethod;
import oeg.lstbs.hash.ThresholdHHM;
import oeg.lstbs.io.CorpusReader;
import oeg.lstbs.io.LanguageDetector;
import oeg.lstbs.io.VectorReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

    public class IndexDocuments {

    private static final Logger LOG = LoggerFactory.getLogger(IndexDocuments.class);

    private static final Integer MIN_WORDS  = 100;
    private static final Integer MAX_WORDS  = 500;

    private static final Integer SIZE       = 5000;

//    static final Dataset DATASET = new Dataset(new Corpus("cordis-70", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/aMBsQaTM4oBi3Ga/download"),       SIZE,    100,    500);
//    static final Textset TEXTSET = new Textset(new Corpus("cordis", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/AZiWRo2n3KxrkLR/download"),       SIZE,    "id",   "title",  "objective");

    static final Dataset DATASET = new Dataset(new Corpus("openresearch-500", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/F3yKtY84LRTHxYK/download"),SIZE,    100,    2500);
    static final Textset TEXTSET = new Textset(new Corpus("openresearch", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/66SCkoGo2sHpzJS/download"),SIZE,    "id",    "title", "paperAbstract");

//    static final Dataset DATASET = new Dataset(new Corpus("patents-750", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/kTD8QEagJEyff3z/download"),     SIZE,   100,    5000);
//    static final Textset TEXTSET = new Textset(new Corpus("patents", "https://delicias.dia.fi.upm.es/nextcloud/index.php/s/SEdjnweQG3boBYK/download"),     SIZE,   "0",    "0", "2");



    static final Integer DEPTH_LEVEL    = 3;

    List<HierarchicalHashMethod> algorithms = Arrays.asList(
//            new ThresholdHHM(DEPTH_LEVEL),
            new CentroidHHM(DEPTH_LEVEL,1000),
            new DensityHHM(DEPTH_LEVEL)
    );


    @Test
    public void execute(){


        Repository textualRepository = new Repository(TEXTSET.getCorpus().getId());
        CorpusReader.CorpusAction corpusAction = (id, name, txt) -> textualRepository.add(id, name, txt);
        CorpusReader.CorpusValidation corpusValidation = (id,name,txt) -> txt.split(" ").length > MIN_WORDS &&  txt.split(" ").length < MAX_WORDS && LanguageDetector.identifyLanguage(txt).equalsIgnoreCase("en");
        CorpusReader.from(TEXTSET.getCorpus().getPath(), 0, corpusAction, corpusValidation, Double.valueOf(Math.ceil(Double.valueOf(TEXTSET.getIndexSize()) / 100.0)).intValue(), TEXTSET.getIndexSize(), TEXTSET.getIdField(), TEXTSET.getNameField(), TEXTSET.getTxtField());
        LOG.info("Added " + textualRepository.getSize() + " documents");


        for(HierarchicalHashMethod algorithm : algorithms){
            LOG.info("Indexing documents by " + algorithm.id() + " algorithm");
            Repository hashRepository = new Repository(DATASET.getCorpus().getId()+"-"+algorithm.id());
            VectorReader.VectorAction vectorAction = (id, vector) -> hashRepository.add(id, algorithm.hash(vector), vector);
            VectorReader.VectorValidation vectorValidation = (id, vector) -> textualRepository.contains(id);
            VectorReader.from(DATASET.getCorpus().getPath(), 0, vectorAction, vectorValidation, Double.valueOf(Math.ceil(Double.valueOf(DATASET.getIndexSize()) / 100.0)).intValue(), DATASET.getIndexSize());
        }

        LOG.info("Indexing textual documents ");

    }

}
