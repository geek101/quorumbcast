package com.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class Client {
    private static final Logger LOG =
            LoggerFactory.getLogger(Client.class);
    private final Long sid;
    private final EventLoopGroup eventLoopGroup;
    private final InetSocketAddress serverAddr;

    private Bootstrap clientBootstrap = null;
    private SocketChannel channel = null;

    private class ConnectInitializer
            extends ChannelInitializer<SocketChannel> {
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new TxHandler(sid));
        }
    }

    public Client(Long sid, InetSocketAddress serverAddr,
                  EventLoopGroup eventLoopGroup) {
        this.sid = sid;
        this.serverAddr = serverAddr;
        this.eventLoopGroup = eventLoopGroup;
        clientBootstrap = new Bootstrap();
        clientBootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ConnectInitializer());
        final Channel channel = clientBootstrap.connect(serverAddr).addListener
                (ConnectDone).channel();
        channel.closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
               LOG.info("Closed channel: " + channel.localAddress() +
               "-" + channel.remoteAddress());
            }
        });
    }

    public void shutdown() {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        eventLoopGroup.shutdownGracefully();
    }

    private final ChannelFutureListener ConnectDone
            = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future)
                throws ExampleException{
            if (future.isSuccess()) {
                channel = (SocketChannel)future.channel();
                LOG.info("Connection open: " + channel.localAddress()
                        + "-" + channel.remoteAddress());

            } else {
                future.cause().printStackTrace();
                future.channel().close();
                throw new ExampleException("Connect failed");
            }
        }
    };

    public boolean send(Integer msg) throws InterruptedException {
        if (channel == null) {
            return false;
        }

        channel.write(msg);
        return true;
    }
}
