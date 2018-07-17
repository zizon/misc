package com.sf.misc.yarn;

import org.apache.hadoop.security.UserGroupInformation;

public interface UGIAware {

    public UserGroupInformation ugi();
}
