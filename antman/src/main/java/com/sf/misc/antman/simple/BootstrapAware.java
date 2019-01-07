package com.sf.misc.antman.simple;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;

public interface BootstrapAware {

    static final NioEventLoopGroup SHARE_GROUP = new NioEventLoopGroup();

    default public <T extends AbstractBootstrap> T bootstrap(T bootstrap) {
        return (T) bootstrap.group(SHARE_GROUP)
                .option(ChannelOption.SO_REUSEADDR, true);
    }
}
