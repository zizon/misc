package com.sf.misc.yarn;

import java.io.File;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

public class KickStart {

    public static void main(String args[]) {
        try {
            String clazz = args[0];
            ClassLoader loader = new URLClassLoader( //
                    new URL[]{ //
                            new File(".").toURI().toURL(),
                            new File("./").toURI().toURL(),
                    },
                    Thread.currentThread().getContextClassLoader()
            );
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
                    .orElseThrow(() -> new NoSuchMethodException("found no main(String[] args) function of class:" + clazz)) //
                    .invoke(null, new Object[]{new String[0]});
        } catch (Throwable exception) {
            exception.printStackTrace();
            LockSupport.park();
        }
    }
}
