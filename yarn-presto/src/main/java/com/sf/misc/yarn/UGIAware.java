package com.sf.misc.yarn;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

public interface UGIAware {

    public UserGroupInformation ugi();

    default public <T> T doAS(Callable<T> callable) {
        try {
            return ugi().doAs(new PrivilegedExceptionAction<T>() {
                @Override
                public T run() throws Exception {
                    return callable.call();
                }
            });
        } catch (Throwable e) {
            throw new RuntimeException("fail to do as ugi:" + ugi(), e);
        }
    }
}
