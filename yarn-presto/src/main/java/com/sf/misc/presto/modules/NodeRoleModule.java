package com.sf.misc.presto.modules;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.discovery.client.DiscoveryBinder;
import io.airlift.discovery.client.ServiceDescriptor;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.Optional;

public class NodeRoleModule implements Module {

    public static final String SERVICE_TYPE = "presto-node-role";

    public static enum ContainerRole {
        Coordinator("presto-coordinator"),
        Worker("presto"),
        Unknown("unknown");

        protected final String service_type;

        ContainerRole(String service_type) {
            this.service_type = service_type;
        }

        public String serviceType() {
            return this.service_type;
        }
    }

    protected static final String CONTAINER_ID = "container_id";
    protected static final String CONTAINER_ROLE = "coordinator_role";

    protected final ContainerId container_id;
    protected final ContainerRole container_role;

    public NodeRoleModule(ContainerId container_id, ContainerRole role) {
        this.container_id = container_id;
        this.container_role = role;
    }

    public static ContainerRole role(ServiceDescriptor service) {
        return ContainerRole.valueOf( //
                Optional.ofNullable(service.getProperties() //
                        .get(CONTAINER_ROLE)) //
                        .orElseGet(() -> ContainerRole.Unknown.name()) //
        );
    }

    public static ContainerId containerId(ServiceDescriptor service) {
        String container_id = service.getProperties().get(CONTAINER_ID);
        if (container_id != null) {
            return ConverterUtils.toContainerId(service.getProperties().get(CONTAINER_ID));
        }

        return null;
    }

    @Override
    public void configure(Binder binder) {
        DiscoveryBinder.discoveryBinder(binder).bindHttpAnnouncement(SERVICE_TYPE)
                .addProperty(CONTAINER_ID, container_id.toString())
                .addProperty(CONTAINER_ROLE, container_role.name());
    }
}
