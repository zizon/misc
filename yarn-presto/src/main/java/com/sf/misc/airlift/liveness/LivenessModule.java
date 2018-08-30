package com.sf.misc.airlift.liveness;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class LivenessModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(Liveness.class).in(Scopes.SINGLETON);
    }
}
