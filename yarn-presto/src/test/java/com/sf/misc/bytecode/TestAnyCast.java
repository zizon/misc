package com.sf.misc.bytecode;

import com.sf.misc.async.ListenablePromise;
import com.sf.misc.classloaders.ClassResolver;
import com.sf.misc.yarn.ConfigurationAware;
import com.sf.misc.yarn.rpc.UGIAware;
import com.sf.misc.yarn.rpc.YarnNMProtocol;
import com.sf.misc.yarn.rpc.YarnRPCAware;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.PrivilegedAction;
import java.util.stream.Stream;

public class TestAnyCast {

    public static final Logger LOGGER = Logger.get(TestAnyCast.class);


    protected void printClass(Class<?> clazz) throws Throwable {
        ClassReader reader = new ClassReader(ClassResolver.locate(clazz).get().openConnection().getInputStream());
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    @Test
    public void test() throws Throwable {
        Configuration configuration = new Configuration();
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("anyone");
        YarnRPC rpc = ugi.doAs((PrivilegedAction<YarnRPC>) () -> YarnRPC.create(configuration));

        AnyCast cast = new AnyCast();
        Stream.of( //
                new ConfigurationAware<Configuration>() {
                    @Override
                    public Configuration config() {
                        return configuration;
                    }
                },
                new UGIAware() {
                    @Override
                    public UserGroupInformation ugi() {
                        return ugi;
                    }
                },
                new YarnRPCAware() {
                    @Override
                    public YarnRPC rpc() {
                        return rpc;
                    }
                }
        ).forEach(cast::adopt);

        AnyCast.CodeGenContext context = cast.codegen(YarnNMProtocol.class);
        context.printClass();

        LOGGER.info("" + MethodHandles.lookup().findVirtual(YarnNMProtocol.class, "connect", MethodType.methodType(ListenablePromise.class, String.class, int.class)));

        YarnNMProtocol protocol = YarnNMProtocol.class.cast(context.newInstance());
        LOGGER.info("" + protocol.ugi());
        protocol.connect("test", 22).unchecked();

    }
}
