package com.example;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by powell on 11/19/15.
 */
public class Server {
    private static final Logger LOG =
            LoggerFactory.getLogger(Client.class);
    private final EventLoopGroup eventLoopGroup;
    private final InetSocketAddress addr;
    private final Server self;
    private ChannelFuture acceptChannelFuture = null;
    private ServerBootstrap serverBootstrap = null;

    private ConcurrentLinkedQueue<Message> inboundMsgQueue
            = new ConcurrentLinkedQueue<>();

    private class MsgRxCb implements Callback {
        @Override
        public void done(Object o) {
            self.msgRx((Message)o);
        }
    }

    private class AcceptInitializer
            extends ChannelInitializer<SocketChannel> {
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast("RxHandler", new RxHandler(new MsgRxCb()));
        }
    }

    public Server(InetSocketAddress addr,
                  EventLoopGroup eventLoopGroup) {
        this.addr = addr;
        this.eventLoopGroup = eventLoopGroup;
        this.self = this;

        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_LINGER, 0)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new AcceptInitializer())
                .childOption(ChannelOption.TCP_NODELAY, true);

        acceptChannelFuture = serverBootstrap.bind(addr);
    }

    public void shutdown() throws InterruptedException {
        if (acceptChannelFuture.isSuccess()) {
            acceptChannelFuture.channel().close();
        }
        eventLoopGroup.shutdownGracefully().sync();
    }

    public Collection<Message> getMessages() {
        Collection<Message> msgs = new ArrayList<>();
        while(!inboundMsgQueue.isEmpty()) {
            msgs.add(inboundMsgQueue.poll());
        }
        return Collections.unmodifiableCollection(msgs);
    }

    private void msgRx(Message o) {
        LOG.info("Rx sid-" + o.sid + " value-" + o.value);
        inboundMsgQueue.add(o);
    }
}
