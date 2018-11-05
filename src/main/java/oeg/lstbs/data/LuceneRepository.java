package oeg.lstbs.data;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.DelimitedTermFrequencyTokenFilter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class LuceneRepository {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneRepository.class);
    private final IndexWriter writer;
    private final FSDirectory directory;
    private DirectoryReader reader;
    private AtomicInteger counter = new AtomicInteger();
    private IndexSearcher searcher;

    public LuceneRepository(String id) {
        try {
            File indexFile = File.createTempFile(id,".tmp");
            indexFile.getParentFile().mkdirs();
            this.directory = FSDirectory.open(indexFile.toPath());
            IndexWriterConfig writerConfig = new IndexWriterConfig(new RepositoryAnalyzer());
            writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            writerConfig.setRAMBufferSizeMB(500.0);
            this.writer = new IndexWriter(directory, writerConfig);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error",e);
        }
    }

    public void add(org.apache.lucene.document.Document doc){
        try {
            writer.addDocument(doc);
            if (counter.incrementAndGet() % 100 == 0 ) {
                LOG.info("added " + counter.get() + " documents");
                commit();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void commit(){
        try {
            writer.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public org.apache.lucene.document.Document getDocument(int docId){
        try {
            if (reader == null) {
                writer.commit();
                writer.close();
                reader = DirectoryReader.open(directory);
                searcher  = new IndexSearcher(reader);
            }

            return reader.document(docId);

        } catch (IOException e) {
            throw new RuntimeException("Unexpected error",e);
        }
    }

    public TopDocs getBy(Query query, int max){
        try {
            if (reader == null) {
                writer.commit();
                writer.close();
                reader = DirectoryReader.open(directory);
                searcher  = new IndexSearcher(reader);
            }

            return searcher.search(new MatchAllDocsQuery(), (max<0? reader.numDocs() : max));

        } catch (IOException e) {
            throw new RuntimeException("Unexpected error",e);
        }
    }

    public class RepositoryAnalyzer extends Analyzer {

        @Override
        protected TokenStreamComponents createComponents(String s) {
            Tokenizer tokenizer = new WhitespaceTokenizer();
            TokenFilter filters = new DelimitedTermFrequencyTokenFilter(tokenizer);
            return new TokenStreamComponents(tokenizer, filters);
        }
    }
}
