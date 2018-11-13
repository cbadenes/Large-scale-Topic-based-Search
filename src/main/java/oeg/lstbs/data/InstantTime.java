package oeg.lstbs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class InstantTime {

    private static final Logger LOG = LoggerFactory.getLogger(InstantTime.class);


    // {"nano":611000000,"epochSecond":1542051811}

    private long nano;

    private long epochSecond;

    public InstantTime() {
    }

    public long getNano() {
        return nano;
    }

    public void setNano(long nano) {
        this.nano = nano;
    }

    public long getEpochSecond() {
        return epochSecond;
    }

    public void setEpochSecond(long epochSecond) {
        this.epochSecond = epochSecond;
    }
}
