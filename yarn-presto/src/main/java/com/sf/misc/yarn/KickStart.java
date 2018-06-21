package com.sf.misc.yarn;

import java.io.File;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

public class KickStart {

    public static final String HTTP_CLASSLOADER_URL = "HTTP_CLASSLOADER_URL";
    public static final String KICKSTART_CLASS = "KICKSTART_CLASS";

    public static void main(String args[]) {
        try {
            ClassLoader loader = new URLClassLoader( //
                    new URL[]{ //
                            new URL(System.getenv().get(HTTP_CLASSLOADER_URL)),
                            new File(".").toURI().toURL(),
                            new File("./").toURI().toURL(),
                    }
            );
            Thread.currentThread().setContextClassLoader(loader);

            // start
            Arrays.stream(loader.loadClass(System.getenv(KICKSTART_CLASS)) //
                    .getMethods())
                    .filter( //
                            (method) -> method.getName() == "main" //
                                    && Modifier.isStatic(method.getModifiers()) //
                                    && method.getParameterCount() == 1 //
                                    && method.getParameterTypes()[0].equals(String[].class)
                    ) //
                    .findFirst() //
                    .orElseThrow(() -> new NoSuchMethodException("found no main function of class:" + KICKSTART_CLASS)) //
                    .invoke(null, new Object[]{new String[0]});
        } catch (Throwable exception) {
            exception.printStackTrace();
            LockSupport.park();
        }
    }
}
