package com.sf.misc.hadoop.sasl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jdk.internal.org.objectweb.asm.util.TraceClassVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import javax.security.auth.login.Configuration;
import java.io.PrintWriter;
import java.lang.invoke.MethodType;
import java.util.Collection;

public class TokenSelectorGenerator {

    public static final Log LOGGER = LogFactory.getLog(TokenSelectorGenerator.class);

    protected static final LoadingCache<String, Class<?>> TOKEN_SELECTORS = CacheBuilder.newBuilder() //
            .build(new CacheLoader<String, Class<?>>() {
                @Override
                public Class<?> load(String key) throws Exception {
                    return make(Class.forName(key), new SASLTokenChecker(null));
                }
            });

    protected static void printClass(byte[] clazz) {
        jdk.internal.org.objectweb.asm.ClassReader reader = new jdk.internal.org.objectweb.asm.ClassReader(clazz);
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    protected static class Geneartor extends ClassLoader {

        public Geneartor(ClassLoader parent) {
            super(parent);
        }

        public Class<?> genearte(String className, byte[] bytecode) {
            printClass(bytecode);
            return defineClass(className, bytecode, 0, bytecode.length);
        }
    }

    protected static final Geneartor GENEARTOR = new Geneartor(Thread.currentThread().getContextClassLoader());

    public static class SASLTokenChecker {

        protected final Configuration configuration;

        public SASLTokenChecker(Configuration configuration) {
            this.configuration = configuration;
        }

        public boolean check(Text service, Collection<Token> tokens) {
            LOGGER.info("invoke check");
            return tokens.parallelStream()
                    .filter((token) -> token.getKind().equals(SaslTokenIdentifier.TOKEN_KIND))
                    .map(SaslTokenIdentifier.class::cast)
                    .filter(SaslTokenIdentifier::integrityCheck)
                    .findAny().isPresent();
        }
    }

    public static class TemplateTokenSelector implements TokenSelector {

        protected final SASLTokenChecker checker;
        protected final TokenSelector delegate;

        public TemplateTokenSelector(SASLTokenChecker checker, TokenSelector selector) {
            this.checker = checker;
            this.delegate = selector;
        }

        @Override
        public Token selectToken(Text service, Collection tokens) {
            LOGGER.info("invoke?");
            if (checker.check(service, tokens)) {
                return delegate.selectToken(service, tokens);
            }

            throw new RuntimeException(new IllegalAccessException("fail to pass sasl token check"));
        }
    }


    public static Class<? extends TokenSelector<? extends TokenIdentifier>> selector(TokenInfo token_info) {
        try {
            Class<? extends TokenSelector> selector;
            if (token_info == null) {
                selector = make(new SASLTokenChecker(null));
            } else {
                selector = make(token_info.value(), new SASLTokenChecker(null));
            }

            return (Class<? extends TokenSelector<? extends TokenIdentifier>>) selector;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    protected static Class<? extends TokenSelector> make(SASLTokenChecker checker) throws Exception {
        ClassReader reader = new ClassReader(TemplateTokenSelector.class.getName());
        ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

        // copy template
        String raw = Type.getDescriptor(TokenSelector.class);
        raw = raw.substring(0, raw.length() - 2) + "Checked;";
        LOGGER.info(raw);
        Type this_type = Type.getType(raw);
        LOGGER.info(this_type);
        // public class $template.getName()+"Checked" extends $template.getName()
        writer.visit(
                Opcodes.V1_6,
                Opcodes.ACC_PUBLIC,
                this_type.getInternalName(),
                null,
                Type.getInternalName(TemplateTokenSelector.class),
                new String[0]
        );

        // constructor
        MethodVisitor method_writer = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "<init>",
                MethodType.methodType(void.class).toMethodDescriptorString(),
                null,
                new String[0]
        );

        method_writer.visitVarInsn(Opcodes.ALOAD, 0);
        method_writer.visitMethodInsn(
                Opcodes.INVOKESPECIAL,
                Type.getInternalName(TemplateTokenSelector.class),
                "<init>",
                MethodType.methodType(void.class).toMethodDescriptorString()
        );

        // end
        method_writer.visitMaxs(0, 0);
        method_writer.visitInsn(Opcodes.RETURN);
        method_writer.visitEnd();

        // set field
        writer.visitField(
                Opcodes.PUTFIELD,
                "checker",
                Type.getInternalName(SASLTokenChecker.class),
                null,
                null
        );

        writer.visitEnd();

        return (Class<? extends TokenSelector>) GENEARTOR.genearte(this_type.getClassName(), writer.toByteArray());
    }

    protected static Class<? extends TokenSelector> make(Class<?> template, SASLTokenChecker checker) throws Exception {
        ClassReader reader = new ClassReader(TemplateTokenSelector.class.getName());
        ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);

        // copy template
        reader.accept(writer, 0);

        Type this_type = Type.getType(Type.getInternalName(template) + "Checked");
        // public class $template.getName()+"Checked" extends $template.getName()
        writer.visit(
                Opcodes.V1_6,
                Opcodes.ACC_PUBLIC,
                this_type.getInternalName(),
                null,
                Type.getInternalName(template),
                new String[0]
        );

        // protected final SASLTokenChecker checker = checker
        writer.visitField(
                Opcodes.ACC_FINAL | Opcodes.ACC_PROTECTED,
                "checker",
                Type.getDescriptor(SASLTokenChecker.class),
                null,
                checker
        );

        // protected final TokenSelector delegate = template;
        writer.visitField(
                Opcodes.ACC_FINAL | Opcodes.ACC_PROTECTED,
                "delegate",
                Type.getDescriptor(TokenSelector.class),
                null,
                template.newInstance()
        );
        /*
        // public  Token<T> selectToken(Text service,Collection<Token<? extends TokenIdentifier>> tokens);
        MethodVisitor method_writer = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "selectToken",
                MethodType.methodType(Token.class, //
                        Text.class, //
                        Collection.class //
                ).toMethodDescriptorString(),
                null,
                new String[0]
        );

        // branch
        Label check_ok = new Label();
        Label check_fail = new Label();
        Label end = new Label();

        // push checker to stack
        method_writer.visitFieldInsn(
                Opcodes.GETFIELD,
                this_type.getInternalName(),
                "checker",
                Type.getDescriptor(SASLTokenChecker.class)
        );

        // push parameter
        Class<?>[] parameters = new Class[]{
                Text.class,
                Collection.class
        };
        IntStream.range(0, parameters.length).forEach((index) -> {
            Class<?> parameter_class = parameters[index];
            Type parameter_type = Type.getType(parameter_class);

            method_writer.visitVarInsn( //
                    parameter_type.getOpcode(org.objectweb.asm.Opcodes.ILOAD), //
                    index + 1 //
            );
        });

        // checker.check
        method_writer.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(SASLTokenChecker.class),
                "check",
                MethodType.methodType(boolean.class, //
                        Text.class, //
                        Collection.class //
                ).toMethodDescriptorString()
        );

        // push true
        method_writer.visitJumpInsn(Opcodes.IFEQ, check_fail);

        // or jump to cehck_ok
        method_writer.visitJumpInsn(Opcodes.GOTO, check_ok);

        // check ok
        method_writer.visitLabel(check_ok);
        // delegate.selectToken
        // push delegate to stack
        method_writer.visitFieldInsn(
                Opcodes.GETFIELD,
                this_type.getInternalName(),
                "delegate",
                Type.getDescriptor(TokenSelector.class)
        );

        // push parameter
        IntStream.range(0, parameters.length).forEach((index) -> {
            Class<?> parameter_class = parameters[index];
            Type parameter_type = Type.getType(parameter_class);

            method_writer.visitVarInsn( //
                    parameter_type.getOpcode(org.objectweb.asm.Opcodes.ILOAD), //
                    index + 1 //
            );
        });

        // delegate.selectToken
        method_writer.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(template),
                "selectToken",
                MethodType.methodType(Token.class, //
                        Text.class, //
                        Collection.class //
                ).toMethodDescriptorString()
        );

        // check ok end
        method_writer.visitJumpInsn(Opcodes.GOTO, end);

        // check fail
        method_writer.visitLabel(check_fail);

        // new exceptoin
        method_writer.visitTypeInsn(Opcodes.NEW, Type.getInternalName(AccessControlException.class));
        method_writer.visitInsn(Opcodes.DUP);
        method_writer.visitMethodInsn(
                Opcodes.INVOKESPECIAL,
                Type.getInternalName(AccessControlException.class),
                "<init>",
                MethodType.methodType(void.class).toMethodDescriptorString()
        );
        method_writer.visitInsn(Opcodes.ATHROW);

        // check fail end
        method_writer.visitJumpInsn(Opcodes.GOTO, end);


        // end of method
        method_writer.visitLabel(end);
        method_writer.visitMaxs(0, 0);
        method_writer.visitInsn(Type.getType(Token.class).getOpcode(Opcodes.IRETURN));
        method_writer.visitEnd();

        */
        // end class
        writer.visitEnd();

        return (Class<? extends TokenSelector>) GENEARTOR.genearte(this_type.getInternalName(), writer.toByteArray());
    }
}
