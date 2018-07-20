package com.sf.misc.yarn;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.async.SettablePromise;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class YarnApplication {

    public static final Logger LOGGER = Logger.get(YarnApplication.class);

    public static final String APPLICATION_TYPE = "unmanaged-next";

    protected final ListenablePromise<YarnRMProtocol> master;
    protected final String application_name;
    protected final SettablePromise<ApplicationId> application;
    protected final UserGroupInformation ugi;
    protected final Configuration configuration;
    protected final ContainerLauncher launcher;

    public YarnApplication(YarnApplicationConfig yarn_config, URI classloader) {
        this.application_name = yarn_config.getApplicaitonName();

        this.launcher = createLauncher(classloader);
        this.ugi = createUGI(yarn_config);
        this.configuration = generateConfiguration(yarn_config);
        this.master = createMasterClient(configuration, ugi);
        this.application = createNew();
    }

    public ListenablePromise<ApplicationId> getApplication() {
        return this.application;
    }

    public ListenablePromise<YarnRMProtocol> getRPCProtocol() {
        return master;
    }

    public UserGroupInformation getUGI() {
        return this.ugi;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    protected ContainerLauncher createLauncher(URI classloader) {
        return null;
        //return new ContainerLauncher(this, classloader);
    }

    protected SettablePromise<ApplicationId> createNew() {
        SettablePromise<ApplicationId> result = SettablePromise.create();

        // submit application
        ListenablePromise<ApplicationId> application = this.master.transformAsync((master) -> {
            LOGGER.info("request application...");
            GetNewApplicationResponse response = master.getNewApplication(GetNewApplicationRequest.newInstance());
            ApplicationId app_id = response.getApplicationId();

            LOGGER.info("create application submit context for:" + app_id);

            // prepare context
            ApplicationSubmissionContext context = Records.newRecord
                    (ApplicationSubmissionContext.class);
            context.setApplicationName(application_name);
            context.setApplicationId(app_id);
            context.setApplicationType(APPLICATION_TYPE);
            context.setKeepContainersAcrossApplicationAttempts(true);

            // set appmaster launch spec
            context.setAMContainerSpec(ContainerLaunchContext.newInstance(null, null, null, null, null, null));

            // unmanaged app master
            context.setUnmanagedAM(true);

            return Promises.submit(() -> {
                // submit
                LOGGER.info("submit application:" + context);
                master.submitApplication(SubmitApplicationRequest.newInstance(context));

                return app_id;
            });
        });

        // find master token
        Promises.chain(application, master).call((app_id, protocol) -> {
            GetApplicationReportRequest request = GetApplicationReportRequest.newInstance(app_id);
            for (; ; ) {
                GetApplicationReportResponse response = protocol.getApplicationReport(request);
                switch (response.getApplicationReport().getYarnApplicationState()) {
                    case ACCEPTED:
                        LOGGER.info("application accepted,set master token");
                        org.apache.hadoop.yarn.api.records.Token token = response.getApplicationReport().getAMRMToken();
                        if (token == null) {
                            LOGGER.info("no token found");
                            break;
                        }

                        Token<AMRMTokenIdentifier> master_toekn = new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(
                                token.getIdentifier().array(), //
                                token.getPassword().array(), //
                                new Text(token.getKind()), //
                                new Text(token.getService()) //
                        );

                        // add totken
                        ugi.addToken(master_toekn);

                        LOGGER.info("register application master with tracking url:");
                        return Futures.immediateFuture(protocol.registerApplicationMaster( //
                                RegisterApplicationMasterRequest.newInstance( //
                                        InetAddress.getLocalHost().getHostName(), //
                                        0, //
                                        ""//
                                ) //
                        ));
                    case FAILED:
                    case FINISHED:
                        return Futures.immediateFailedFuture(new RuntimeException(response.getApplicationReport().getDiagnostics()));
                    default:
                        Thread.yield();
                        break;
                }
            }
        }).callback((response, throwable) -> {
            if (throwable != null) {
                result.setException(throwable);
                return;
            }

            result.setFuture(application);
        });
        return result;
    }

    protected ListenablePromise<YarnRMProtocol> createMasterClient(Configuration conf, UserGroupInformation ugi) {
        return Promises.submit(() -> ugi.doAs(new PrivilegedExceptionAction<YarnRMProtocol>() {
            @Override
            public YarnRMProtocol run() throws Exception {
                ConcurrentMap<String, Map.Entry<Method, Object>> method_cache = Stream.of( //
                        ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class), //
                        ClientRMProxy.createRMProxy(conf, ApplicationMasterProtocol.class)).parallel() //
                        .flatMap((instance) -> {
                            return findInterfaces(instance.getClass()).parallelStream() //
                                    .flatMap((iface) -> {
                                        return Arrays.stream(iface.getMethods());
                                    }).map((method) -> {
                                        return Entrys.newImmutableEntry(method.getName(), new AbstractMap.SimpleImmutableEntry<>(method, instance));
                                    });
                        }).collect(
                                Maps::newConcurrentMap, //
                                (map, entry) -> {
                                    map.put(entry.getKey(), entry.getValue());
                                }, //
                                Map::putAll
                        );

                return (YarnRMProtocol) Proxy.newProxyInstance(
                        Thread.currentThread().getContextClassLoader(), //
                        new Class[]{ //
                                YarnRMProtocol.class, //
                                ApplicationClientProtocol.class, //
                                ApplicationMasterProtocol.class //
                        }, //
                        new InvocationHandler() {
                            @Override
                            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                                Map.Entry<Method, Object> method_instance = method_cache.get(method.getName());
                                return ugi.doAs( //
                                        (PrivilegedExceptionAction<Object>) () -> method_instance.getKey()//
                                                .invoke(method_instance.getValue(), args)
                                );
                            }
                        });
            }
        }));
    }

    protected Set<Class<?>> findInterfaces(Class<?> to_infer) {
        Stream<Class<?>> indrect = Arrays.stream(to_infer.getInterfaces()) //
                .flatMap((iface) -> {
                    return findInterfaces(iface).parallelStream();
                });
        return Stream.concat(Arrays.stream(to_infer.getInterfaces()), indrect).collect(Collectors.toSet());
    }

    protected Configuration generateConfiguration(YarnApplicationConfig config) {
        Configuration configuration = new Configuration();
        ConfigurationGenerator generator = new ConfigurationGenerator();

        // hdfs ha
        config.getNameservices().forEach((uri) -> {
            generator.generateHdfsHAConfiguration(uri).entrySet() //
                    .forEach((entry) -> {
                        configuration.set(entry.getKey(), entry.getValue());
                    });
        });

        // resource managers
        generator.generateYarnConfiguration(config.getResourceManagers()).entrySet() //
                .forEach((entry) -> {
                    configuration.set(entry.getKey(), entry.getValue());
                });

        return configuration;
    }

    protected UserGroupInformation createUGI(YarnApplicationConfig config) {
        return UserGroupInformation.createProxyUser(config.getProxyUser(), UserGroupInformation.createRemoteUser(config.getRealUser()));
    }
}
