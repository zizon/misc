package com.sf.misc.presto;

import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.inject.Module;
import com.sf.misc.airlift.federation.DiscoveryUpdateModule;
import com.sf.misc.classloaders.ClassResolver;
import com.sf.misc.presto.plugins.PrestoServerBuilder;
import io.airlift.log.Logger;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.util.concurrent.locks.LockSupport;

public class TestSkipPrestoJVMCheck {

    public static final Logger LOGGER = Logger.get(TestSkipPrestoJVMCheck.class);

    public static class Template extends PrestoServerBuilder.PrestoServerRunner {
        protected Iterable _modules_;
        protected SqlParserOptions options;

        public Template(Iterable<Module> modules, SqlParserOptions parserOptions) {
            super();
            this._modules_ = modules;
            this.options = parserOptions;
        }

        @Override
        protected Iterable<Module> getAdditionalModules() {
            return this._modules_;
        }

        @Override
        public void run() {

        }
    }

    protected void printClass(Class<?> clazz) throws Throwable {
        ClassReader reader = new ClassReader(ClassResolver.locate(clazz).get().openConnection().getInputStream());
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    protected void printClassBytes(byte[] bytes) throws Throwable {
        ClassReader reader = new ClassReader(bytes);
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    @Test
    public void test() throws Throwable {
        //printClass(Template.class);
        new PrestoServerBuilder(true) //
                //.add(new DiscoveryUpdateModule()) //
                .build() //
                .unchecked()
                .run();

        LockSupport.park();
        ;
    }
}
