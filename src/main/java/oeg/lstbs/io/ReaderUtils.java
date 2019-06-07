package oeg.lstbs.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.zip.GZIPInputStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class ReaderUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderUtils.class);


    public static BufferedReader from(String path) throws IOException {

        return from(path, true);
    }

    public static BufferedReader from(String path, Boolean gzip) throws IOException {

        InputStreamReader inputStreamReader;

        InputStream inputStream = path.startsWith("http")? new URL(path).openStream() : new FileInputStream(path);

        inputStreamReader = gzip? new InputStreamReader(new GZIPInputStream(inputStream)) : new InputStreamReader(inputStream);

        return new BufferedReader(inputStreamReader);
    }

}
