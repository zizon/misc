package com.sf.misc.classloaders;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedStream;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

@ChannelHandler.Sharable
public class HttpClassLoaderServer extends SimpleChannelInboundHandler<HttpRequest> {
    private static final Log LOGGER = LogFactory.getLog(HttpClassLoaderServer.class);

    protected static ListeningExecutorService WORKER_POOL = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    protected ServerBootstrap bootstrap;
    protected InetSocketAddress bind_address;
    protected HttpResponse response;

    public HttpClassLoaderServer(InetSocketAddress bind_addres) {
        this.bind_address = bind_addres;
        this.bootstrap = new ServerBootstrap() //
                .group(new NioEventLoopGroup()) //
                .channel(NioServerSocketChannel.class)//
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        initializePipeline(ch.pipeline());
                    }
                }) //
        ;

        this.response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        this.response.headers() //
                .add(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED) //
                .add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE) //
        ;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, HttpRequest request) throws Exception {
        // load class
        ListenableFuture<InputStream> class_loaded = this.workpool().submit(() -> this.openClassStream(request.getUri()));

        // prefly
        context.writeAndFlush(response).addListener((ChannelFuture wrote) -> {
            Futures.transform(class_loaded, (InputStream class_stream) -> {
                return wrote.channel().writeAndFlush(new ChunkedStream(class_stream)) //
                        .channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT) //
                        ;
            });
        });
    }

    public ChannelFuture bind() {
        return this.tune(bootstrap).bind(this.bind_address);
    }

    protected ListeningExecutorService workpool() {
        return WORKER_POOL;
    }

    protected ChannelPipeline initializePipeline(ChannelPipeline pipeline) {
        return pipeline //
                // inbound
                .addLast(new HttpRequestDecoder()) //

                // outbound
                .addLast(new HttpContentCompressor()) //
                .addLast(new HttpResponseEncoder()) //
                .addLast(new ChunkedWriteHandler()) //

                // duplex
                .addLast(new HttpObjectAggregator(Integer.MAX_VALUE)) //

                // this
                .addLast(this)
                ;
    }

    protected ServerBootstrap tune(ServerBootstrap bootstrap) {
        return bootstrap.option(ChannelOption.SO_REUSEADDR, true);
    }

    protected InputStream openClassStream(String uri) {
        String qualified_class_name = uri.substring(1, uri.length() - ".class".length()).replace("/", ".");
        Class<?> target = null;
        try {
            target = Class.forName(qualified_class_name);
        } catch (ClassNotFoundException e) {
            LOGGER.error("found no class:" + qualified_class_name, e);
            return null;
        }

        URL dir = target.getResource("");
        StringBuilder buffer = new StringBuilder();
        new BiConsumer<Class<?>, StringBuilder>() {
            @Override
            public void accept(Class<?> clazz, StringBuilder buffer) {
                if (clazz.getEnclosingClass() != null) {
                    this.accept(clazz.getEnclosingClass(), buffer);
                    buffer.append('$');
                }

                buffer.append(clazz.getSimpleName());
            }
        }.accept(target, buffer);
        buffer.append(".class");

        try {
            return new URL(dir, buffer.toString()).openConnection().getInputStream();
        } catch (IOException e) {
            LOGGER.error("fail to open class stream:" + buffer, e);
            return null;
        }
    }
}
