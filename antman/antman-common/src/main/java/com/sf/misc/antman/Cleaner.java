package com.sf.misc.antman;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;

public class Cleaner {


    protected final Object real = null;

    public Cleaner(Object object) {

        //this.real = CLEANER_CLASS.cast(object);
    }

    public static Cleaner create(Object ob, Runnable thunk) {
        //return new Cleaner(CLEANER_CREATE.apply(ob, thunk));
        return null;
    }

    public void clean() {
        //CLEANER_CLEAN.accept(real);
    }
}
