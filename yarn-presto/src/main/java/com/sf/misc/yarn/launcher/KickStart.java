package com.sf.misc.yarn.launcher;

import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.log.Logger;
import org.apache.hadoop.io.retry.RetryPolicies;

import java.io.File;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class KickStart {

    public static final Logger LOGGER = Logger.get(KickStart.class);

    public static void main(String args[]) {
        try {
            String clazz = args[0];
            ClassLoader loader = new URLClassLoader( //
                    new URL[]{ //
                            new File(".").toURI().toURL(),
                            new File("./").toURI().toURL(),
                    },
                    Thread.currentThread().getContextClassLoader()
            ) {
                protected Class<?> findClass(final String name) throws ClassNotFoundException {
                    return Promises.retry( //
                            () -> {
                                try {
                                    Class<?> clazz = super.findClass(name);
                                    return Optional.of(clazz);
                                } catch (ClassNotFoundException e) {
                                    return Optional.empty();
                                }
                            },
                            RetryPolicies.retryUpToMaximumCountWithFixedSleep(10, 10, TimeUnit.MILLISECONDS)
                    ).unchecked();
                }
            };
            Thread.currentThread().setContextClassLoader(loader);

            // start
            Arrays.stream(loader.loadClass(clazz) //
                    .getMethods())
                    .filter( //
                            (method) -> method.getName() == "main" //
                                    && Modifier.isStatic(method.getModifiers()) //
                                    && method.getParameterCount() == 1 //
                                    && method.getParameterTypes()[0].equals(String[].class)
                    ) //
                    .findFirst() //
                    .orElseThrow(() -> new NoSuchMethodException("found no static main(String[] args) function of class:" + clazz)) //
                    .invoke(null, new Object[]{new String[0]});
        } catch (Throwable exception) {
            exception.printStackTrace();
            LockSupport.park();
        }
    }
}
