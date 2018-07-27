package com.sf.misc.bytecode;

import com.sf.misc.yarn.rpc.YarnRMProtocol;
import io.airlift.log.Logger;
import org.junit.Test;

public class TestAnyCast {

    public static final Logger LOGGER = Logger.get(TestAnyCast.class);

    @Test
    public void test() {

        AnyCast anycast = new AnyCast();


        AnyCast.CodeGenContext context = anycast.codegen(YarnRMProtocol.class);

        YarnRMProtocol instance = (YarnRMProtocol) context.printClass().newInstance();
        instance.doAS(() -> {
            LOGGER.info("hello kitty");
            return 1;
        });
    }
}
