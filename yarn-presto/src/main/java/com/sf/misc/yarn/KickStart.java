package com.sf.misc.yarn;

import com.sf.misc.classloaders.HttpClassloader;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class KickStart {

    public static final String HTTP_CLASSLOADER_URL = "HTTP_CLASSLOADER_URL";
    public static final String KICKSTART_CLASS = "KICKSTART_CLASS";

    public static void main(String args[]) throws Exception {
        ClassLoader loader = new URLClassLoader( //
                new URL[]{ //
                        new URL(System.getenv().get(HTTP_CLASSLOADER_URL)),
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
    }
}
