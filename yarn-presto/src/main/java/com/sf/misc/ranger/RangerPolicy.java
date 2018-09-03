package com.sf.misc.ranger;

import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.log.Logger;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RangerPolicy {

    public static final Logger LOGGER = Logger.get(RangerPolicy.class);

    protected static final String RANGER_APP_TYPE = "ranger-standalone-acl";
    protected static final String PROPERTY_PREFIX = "ranger.plugin.hive";
    protected static LoadingCache<String, RangerPolicyBundle> POLICY_CACHE = CacheBuilder.newBuilder() //
            .refreshAfterWrite(1, TimeUnit.MINUTES) //
            .build(new CacheLoader<String, RangerPolicyBundle>() {
                @Override
                public RangerPolicyBundle load(String policy_name) throws Exception {
                    RangerAdminRESTClient admin = new RangerAdminRESTClient();
                    admin.init(policy_name, RANGER_APP_TYPE, PROPERTY_PREFIX);

                    return new RangerPolicyBundle(admin);
                }

                @Override
                public ListenablePromise<RangerPolicyBundle> reload(String policy_name, RangerPolicyBundle bundle) throws Exception {
                    RangerAdminClient client = bundle.clien();
                    return bundle.engine().transform( (engine) -> {
                        try {
                            ServicePolicies policy = client.getServicePoliciesIfUpdated(engine.getPolicyVersion(), System.currentTimeMillis());
                            if (policy != null) {
                                bundle.update(new RangerPolicyEngineImpl(RANGER_APP_TYPE, policy, new RangerPolicyEngineOptions()));
                            }
                        } catch (Exception e) {
                            LOGGER.error(e, "fail when refreshing policy:" + policy_name, e);
                        } finally {
                            return bundle;
                        }
                    });
                }

            });

    public static class RangerPolicyBuilder {
        protected String policy_name;
        protected ImmutableMap.Builder<String, String> configuration;

        public RangerPolicyBuilder() {
            this.configuration = ImmutableMap.builder();
        }

        public RangerPolicyBuilder(RangerConfig config) {
            this();
            this.policy(config.getPolicyName()) //
                    .admin(config.getAdminURL()) //
                    .solr(config.getSolrURL()) //
                    .collection(config.getSolrCollection());
        }

        public RangerPolicyBuilder policy(String policy_name) {
            this.policy_name = policy_name;
            return this;
        }

        public RangerPolicyBuilder admin(String admin) {
            try {
                configuration.put(PROPERTY_PREFIX + ".policy.rest.url", new URL(admin).toExternalForm());
            } catch (MalformedURLException e) {
                throw new UncheckedIOException("admin url:" + admin, e);
            }
            return this;
        }

        public RangerPolicyBuilder solr(String solr_server) {
            try {
                configuration.put(AuditProviderFactory.AUDIT_DEST_BASE + ".solr.urls", new URL(solr_server).toExternalForm());
            } catch (MalformedURLException e) {
                throw new UncheckedIOException(e);
            }

            return this;
        }

        public RangerPolicyBuilder collection(String collection) {
            configuration.put(AuditProviderFactory.AUDIT_DEST_BASE + ".solr.collection", collection);
            return this;
        }

        public RangerPolicy build() {
            configuration.put(AuditProviderFactory.AUDIT_DEST_BASE + ".log4j", "true");
            configuration.put(AuditProviderFactory.AUDIT_DEST_BASE + ".solr", "true");

            ImmutableMap<String, String> final_configuration = configuration.build();
            Arrays.asList(
                    PROPERTY_PREFIX + ".policy.rest.url",
                    AuditProviderFactory.AUDIT_DEST_BASE + ".solr.urls",
                    AuditProviderFactory.AUDIT_DEST_BASE + ".solr.collection"
            ).forEach((key) -> {
                if (!final_configuration.containsKey(key)) {
                    throw new IllegalStateException("missing configuration:" + key);
                }
            });

            RangerConfiguration ranger_configuration = RangerConfiguration.getInstance();
            boolean same = final_configuration.entrySet().stream().map((entry) -> {
                String old = ranger_configuration.get(entry.getKey());
                if (old != null) {
                    if (old.compareTo(entry.getValue()) == 0) {
                        return true;
                    }
                }
                ranger_configuration.set(entry.getKey(), entry.getValue());
                return false;

            }).reduce(Boolean::logicalAnd).orElse(true);

            if (!same) {
                LOGGER.info("reload ranger configuration");
                if (ranger_configuration.isAuditInitDone()) {
                    LOGGER.info("reinitialize ranger audit system");
                    // reinitialize audit
                    Properties properties = new Properties();
                    properties.putAll(StreamSupport.stream(ranger_configuration.spliterator(), false) //
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

                    AuditProviderFactory.getInstance().init(properties, RANGER_APP_TYPE);
                } else {
                    ranger_configuration.initAudit(RANGER_APP_TYPE);
                }

                // invalidate policys
                POLICY_CACHE.invalidateAll();
            }

            return new RangerPolicy( //
                    Optional.of(policy_name).get() //
            );
        }
    }

    public class RangerAccess {

        public class RangerPriviledgeAccess {
            public class RangerDatabaseAccess {
                public class RangerTableAccess {

                    protected ConcurrentSkipListSet<String> columns;

                    protected RangerTableAccess() {
                        this.columns = new ConcurrentSkipListSet<>();
                    }

                    public RangerTableAccess accessColumn(String column) {
                        this.columns.add(Optional.of(column).get());
                        return this;
                    }

                    public RangerDatabaseAccess database() {
                        return RangerDatabaseAccess.this;
                    }

                    public RangerPriviledgeAccess restricted() {
                        return RangerPriviledgeAccess.this;
                    }

                    public RangerAccess access() {
                        return RangerAccess.this;
                    }
                }

                protected ConcurrentMap<String, RangerTableAccess> tables;

                protected RangerDatabaseAccess() {
                    this.tables = Maps.newConcurrentMap();
                }

                public RangerTableAccess accessTable(String table) {
                    return tables.compute(table, (key, old) -> {
                        return Optional.ofNullable(old).orElse(new RangerTableAccess());
                    });
                }

                public RangerPriviledgeAccess priviledge() {
                    return RangerPriviledgeAccess.this;
                }

                public RangerAccess access() {
                    return RangerAccess.this;
                }
            }

            protected ConcurrentMap<String, RangerDatabaseAccess> databases;

            protected RangerPriviledgeAccess() {
                this.databases = Maps.newConcurrentMap();
            }

            public RangerDatabaseAccess accessDatabase(String database) {
                return databases.compute(database, (key, old) -> {
                    return Optional.ofNullable(old).orElse(new RangerDatabaseAccess());
                });
            }
        }

        protected ConcurrentMap<String, RangerPriviledgeAccess> privileges;
        protected String user;

        protected RangerAccess(String user) {
            this.privileges = Maps.newConcurrentMap();
            this.user = user;
        }

        public RangerPriviledgeAccess privilege(String access_type) {
            return this.privileges.compute(access_type, (key, old) -> {
                return Optional.ofNullable(old).orElse(new RangerPriviledgeAccess());
            });
        }

        public ListenablePromise<Boolean> isAccessAllowed() {
            return POLICY_CACHE.getUnchecked(RangerPolicy.this.policy).engine().transform((engine) -> {
                List<RangerAccessRequest> requests = this.privileges.entrySet().parallelStream().flatMap((privilege) -> {
                    return privilege.getValue().databases.entrySet().parallelStream().flatMap((database) -> {
                        if (database.getValue().tables.isEmpty()) {
                            RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
                            resource.setValue("database", database.getKey());
                            resource.setValue("table", "*");
                            return Stream.of(resource);
                        }

                        return database.getValue().tables.entrySet().parallelStream().map((table) -> {
                            RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
                            resource.setValue("database", database.getKey());
                            resource.setValue("table", table.getKey());

                            if (table.getValue().columns.isEmpty()) {
                                resource.setValue("column", "*");
                            } else {
                                resource.setValue("column", table.getValue().columns.stream().collect(Collectors.joining(",")));
                            }
                            return resource;
                        });
                    }).map((resource) -> {
                        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
                        request.setResource(resource);
                        request.setUser(user);
                        request.setAccessType(privilege.getKey());
                        request.setAction(privilege.getKey().toUpperCase());
                        return request;
                    });
                }).collect(Collectors.toList());

                RangerDefaultAuditHandler audit = new RangerDefaultAuditHandler();
                engine.preProcess(requests);
                return engine.isAccessAllowed(requests, null) //
                        .parallelStream() //
                        .map((result) -> {
                            audit.processResult(result);
                            return result.getIsAllowed();
                        })//
                        .reduce(Boolean::logicalAnd) //
                        .orElse(false);
            });
        }
    }

    protected static class RangerPolicyBundle {
        protected RangerAdminClient client;
        protected RangerPolicyEngine engine;

        protected RangerPolicyBundle(RangerAdminClient client) {
            this.client = client;
            this.engine = null;
        }

        protected ListenablePromise<RangerPolicyEngine> engine() {
            RangerPolicyEngine engine = this.engine;
            if (engine == null) {
                return Promises.submit(() -> {
                    // walk around for classlaoder problem
                    try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(client.getClass().getClassLoader())) {
                        ServicePolicies policy = client.getServicePoliciesIfUpdated(-1, System.currentTimeMillis());
                        this.engine = new RangerPolicyEngineImpl(RANGER_APP_TYPE, policy, new RangerPolicyEngineOptions());
                        return this.engine;
                    } catch (Throwable throwable) {
                        URL core = Thread.currentThread().getContextClassLoader().getResource("core-site.xml");
                        URL root = Thread.currentThread().getContextClassLoader().getResource(System.mapLibraryName("hadoop"));
                        throw new RuntimeException("core:" + core + " root:" + root, throwable);
                    }
                });
            } else {
                return Promises.immediate(engine);
            }
        }

        protected RangerAdminClient clien() {
            return this.client;
        }

        protected RangerPolicyBundle update(RangerPolicyEngine engine) {
            this.engine = Optional.ofNullable(engine).orElse(engine);
            return this;
        }
    }

    protected String policy;

    protected RangerPolicy(String policy) {
        this.policy = policy;
    }

    public RangerAccess newAccess(String user) {
        return new RangerAccess(user);
    }
}
