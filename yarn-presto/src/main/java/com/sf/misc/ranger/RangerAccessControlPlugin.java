package com.sf.misc.ranger;

import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.google.common.util.concurrent.Futures;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RangerAccessControlPlugin implements Plugin {

    public static final Logger LOGGER = Logger.get(RangerAccessControlPlugin.class);

    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories() {
        return Collections.singleton(new SystemAccessControlFactory() {
            @Override
            public String getName() {
                return "ranger";
            }

            @Override
            public SystemAccessControl create(Map<String, String> property) {
                try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(this.getClass().getClassLoader())) {
                    return internalCreate(property);
                }
            }

            public SystemAccessControl internalCreate(Map<String, String> property) {
                property.entrySet().parallelStream().forEach((entry) -> {
                    LOGGER.info("access control config:" + entry.getKey() + " value:" + entry.getValue());
                });

                LOGGER.info("current:" + Thread.currentThread().getContextClassLoader());
                LOGGER.info("GroupMappingServiceProvider class loader:" + GroupMappingServiceProvider.class.getClassLoader());
                LOGGER.info("JniBasedUnixGroupsMappingWithFallback  class loader:" + JniBasedUnixGroupsMappingWithFallback.class.getClassLoader());
                LOGGER.info("ok?" + GroupMappingServiceProvider.class.isAssignableFrom(JniBasedUnixGroupsMappingWithFallback.class));
                RangerPolicy policy = new RangerPolicy.RangerPolicyBuilder( //
                        new ConfigurationFactory(property) //
                                .build(RangerConfig.class) //
                ).build();

                return new SystemAccessControl() {
                    @Override
                    public void checkCanSetUser(Principal principal, String userName) {
                    }

                    @Override
                    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName) {
                    }

                    @Override
                    public void checkCanAccessCatalog(Identity identity, String catalogName) {
                    }

                    @Override
                    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns) {
                        RangerPolicy.RangerAccess.RangerPriviledgeAccess.RangerDatabaseAccess.RangerTableAccess access = policy.newAccess(identity.getUser()) //
                                .privilege("write") //
                                .accessDatabase(table.getSchemaTableName().getSchemaName()) //
                                .accessTable(table.getSchemaTableName().getTableName());
                        columns.parallelStream().forEach(access::accessColumn);

                        boolean allow = Futures.getUnchecked(access.access().isAccessAllowed());
                        LOGGER.info("identify:" + identity + " table:" + table + " columns:" + columns + " allow:" + allow);
                        if (!allow) {
                            AccessDeniedException.denySelectTable(table.toString());
                        } else {
                            AccessDeniedException.denySelectTable(table.toString());
                        }
                    }

                    @Override
                    public void checkCanSelectFromTable(Identity identity, CatalogSchemaTableName table) {
                        this.checkCanSelectFromColumns(identity, table, Collections.emptySet());
                    }
                };
            }
        });
    }
}
