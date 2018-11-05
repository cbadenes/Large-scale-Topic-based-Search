package oeg.lstbs.data;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class TopicSummary {

    private static final Logger LOG = LoggerFactory.getLogger(TopicSummary.class);

    private final List<TopicPoint> groups;

    private final HashFunction hf = Hashing.murmur3_32();

    private static final String GROUP_SEPARATOR = "#";


    public TopicSummary(List<TopicPoint> groups) {
        this.groups = groups;
        Collections.sort(this.groups, (a, b) -> -a.getScore().compareTo(b.getScore()));
    }

    public String getReducedHashTopicsBy(int num) {
        if (groups.size()<=num) return getTopHashTopicsBy(1);
        return groups.subList(0,groups.size()-num).stream().map(tp -> tp.getId()).collect(Collectors.joining(GROUP_SEPARATOR));
    }

    public Integer getReducedHashCodeBy(int num){
        if (groups.size()<=num) return getTopHashCodeBy(1);
        return hf.hashString(groups.subList(0,groups.size()-num).stream().map(tp -> tp.getId()).collect(Collectors.joining(GROUP_SEPARATOR)), Charset.defaultCharset()).asInt();
    }

    public String getTopHashTopicsBy(int num) {
        if (groups.size()<=num) return getReducedHashTopicsBy(0);
        return groups.subList(0,num).stream().map(tp -> tp.getId()).collect(Collectors.joining(GROUP_SEPARATOR));
    }

    public Integer getTopHashCodeBy(int num){
        if (groups.size()<=num) return getReducedHashCodeBy(0);
        return hf.hashString(groups.subList(0,num).stream().map(tp -> tp.getId()).collect(Collectors.joining(GROUP_SEPARATOR)), Charset.defaultCharset()).asInt();
    }


    public String getHashExpression(){
        return this.groups.stream().map(tp -> tp.getId()).collect(Collectors.joining("\n"));
    }

    public int getSize(){
        return groups.size();
    }

}