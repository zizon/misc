package com.sf.misc.airlift.inventory;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.jaxrs.JaxrsBinder;

public class DiscoveryInventoryModule implements Module {

    @Override
    public void configure(Binder binder) {
        JaxrsBinder.jaxrsBinder(binder).bind(DiscoveryInventoryResource.class);
    }
}
