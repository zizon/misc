package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;

public class FilteredDistcp extends DistCp {

    public void setConf(Configuration conf) {
        conf.setClass(DistCpConstants.
                CONF_LABEL_COPY_LISTING_CLASS, CopyWithSkipListing.class, CopyListing.class);
        super.setConf(conf);
    }
}
