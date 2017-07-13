package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class FilteredDistcp extends DistCp {

    public FilteredDistcp() {
        super();
    }

    public void setConf(Configuration conf) {
        if (conf != null) {
            conf.setClass(DistCpConstants.
                    CONF_LABEL_COPY_LISTING_CLASS, CopyWithSkipListing.class, CopyListing.class);
        }
        super.setConf(conf);
    }

    public static void main(String argv[]) {
        Configuration configuration = new Configuration();
        configuration.addResource("distcp-default.xml");
        try {
            ToolRunner.run(configuration, new FilteredDistcp(), argv);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
