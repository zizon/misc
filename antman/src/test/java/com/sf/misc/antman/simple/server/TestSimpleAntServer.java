package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.Packet;
import com.sf.misc.antman.simple.client.SimpleAntClient;
import com.sf.misc.antman.simple.packets.CrcAckPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class TestSimpleAntServer {

    public static final Log LOGGER = LogFactory.getLog(TestSimpleAntServer.class);

    @Test
    public void test() {
        File input_file = new File("large_exec_file.exe");
        SocketAddress address = new InetSocketAddress(10010);
        Promise<Void> serer_ready = Promise.wrap(new SimpleAntServer(address) {
            @Override
            protected ChannelPipeline pipeline(ChannelPipeline pipeline) {
                return pipeline.addFirst(new ChannelInboundHandlerAdapter() {
                    long read_aggregate = 0;
                    long read = 0;
                    long unit = 1024 * 1024 * 10; // 10m

                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        read += ((ByteBuf) msg).readableBytes();
                        if (read / unit != read_aggregate) {
                            read_aggregate = read / unit;
                        }
                        super.channelRead(ctx, msg);
                    }

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        this.read_aggregate = 0;
                        this.read = 0;
                        super.channelActive(ctx);
                    }
                });
            }
        }.bind()).logException();
        serer_ready.join();


        new SimpleAntClient(address) {
            @Override
            public Packet.Registry postInitializeRegistry(Packet.Registry registry) {
                LOGGER.info("client replace");
                return super.postInitializeRegistry(registry)
                        .repalce(new CrcAckPacket() {
                            @Override
                            public Packet decodePacket(ByteBuf from) {
                                return super.decodePacket(from);
                            }

                            @Override
                            public void decodeComplete(ChannelHandlerContext ctx) {
                                LOGGER.info("recive ack:" + this);
                                ctx.channel().close();
                            }
                        });
            }
        }.uploadFile(input_file)
                .logException()
                .join().closeFuture().syncUninterruptibly();


    }
}
