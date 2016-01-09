package com.quorum.netty;

import com.common.X509Exception;
import com.common.X509Util;
import com.quorum.AbstractServer;
import com.quorum.util.ChannelException;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import static com.common.X509Exception.KeyManagerException;
import static com.common.X509Exception.TrustManagerException;

/**
 * Hides the details of netty server side and client side initialization.
 * Abstracts Netty bootstrap and TLS support initialization.
 * Provides the following abstract API to help channel lifecycle
 * management.
 *
 * connectHandler() - for outgoing connection success/failure status
 * acceptHandler()  - for incoming new connection.
 * closedHandler()  - for any connection on closed handling.
 *
 * Currently it does not maintain a channel map and close all connections
 * on shutdown. That is left for the outer layer to handle.
 * Created by powell on 11/25/15.
 */
public abstract class NettyChannelMgr {
    private static final Logger LOG =
            LoggerFactory.getLogger(NettyChannelMgr.class.getName());
    private final EventLoopGroup group;
    private final boolean sslEnabled;
    private final X509Util x509Util;

    // protected for tester
    protected ChannelFuture acceptChannelFuture = null;

    public NettyChannelMgr(final EventLoopGroup group, boolean sslEnabled,
                           final String sslVersion,
                           final String keyStoreLocation,
                           final String keyStorePassword,
                           final String trustStoreLocation,
                           final String trustStorePassword,
                           final String trustStoreCAAlias)
            throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        this.group = group;
        this.sslEnabled = sslEnabled;
        if (this.sslEnabled) {
            this.x509Util = new X509Util(sslVersion, keyStoreLocation,
                    keyStorePassword, trustStoreLocation, trustStorePassword,
                    trustStoreCAAlias);
        } else {
            this.x509Util = null;
        }
    }

    public NettyChannelMgr(final EventLoopGroup group, boolean sslEnabled,
                           final String sslVersion)
            throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        this.group = group;
        this.sslEnabled = sslEnabled;
        if (this.sslEnabled) {
            this.x509Util = new X509Util(sslVersion);
        } else {
            this.x509Util = null;
        }
    }

    public NettyChannelMgr(final EventLoopGroup group, boolean sslEnabled)
            throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        this.group = group;
        this.sslEnabled = sslEnabled;
        if (this.sslEnabled) {
            this.x509Util = new X509Util();
        } else {
            this.x509Util = null;
        }
    }

    public void startListener(final InetSocketAddress listenerAddr)
            throws ChannelException, SSLException, CertificateException {
        if (acceptChannelFuture != null) {
            LOG.warn("Already listening: "
                    + acceptChannelFuture.channel().localAddress());
            return;
        }

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(group)
                .channel(NioServerSocketChannel.class)
                //.handler(new LoggingHandler(LogLevel.DEBUG))
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new AcceptInitializer())
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_LINGER, 0);

        acceptChannelFuture = serverBootstrap.bind(listenerAddr);
    }

    /**
     * Used for testing.
     */
    public void waitForListener() throws InterruptedException {
        acceptChannelFuture.sync();
    }

    public void shutdown() {
        if (acceptChannelFuture != null) {
            acceptChannelFuture.channel().close().syncUninterruptibly();
        }
    }

    protected void startConnection(final AbstractServer server) {
        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ClientInitializer(server))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_LINGER, 0);

        // Create an new Channel.
        clientBootstrap.connect(server.getElectionAddr())
                .addListener(new ConnectListener(server));
    }

    /**
     * Implement this to return a handler for managing incoming connections.
     * @return a new concrete handler for server server side
     */
    protected abstract NettyChannel newAcceptHandler();

    /**
     * Implement this to return a handler for managing outgoing connections.
     * @return a new concrete handler for client side
     */
    protected abstract NettyChannel newClientHandler(
            final AbstractServer server);

    /**
     * If caller wants to do something about connect success/failures.
     * @param server
     * @param handler
     * @param success
     */
    protected abstract void connectHandler(AbstractServer server,
                                           NettyChannel handler,
                                           boolean success);

    /**
     * If caller wants to do something for an incoming connection.
     * @param handler
     */
    protected abstract void acceptHandler(NettyChannel handler);

    /**
     * If caller wants to do something when channel is closed
     * @param handler
     */
    protected abstract void closedHandler(NettyChannel handler);

    private NettyChannel getHandler(Channel sc) {
        return (NettyChannel)sc.pipeline().context(NettyChannel.class)
                .handler();
    }

    private class AcceptInitializer
            extends ChannelInitializer<SocketChannel> {
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            NettyChannel handler = newAcceptHandler();
            handler.setChannel(ch);
            acceptHandler(handler);
            ch.closeFuture().addListener(new ClosedListener());

            if (sslEnabled) {
                initServerSSL(p);
            }

            p.addLast("serverhandler", handler);
        }
    }

    private class ClientInitializer
            extends ChannelInitializer<SocketChannel> {
        private final AbstractServer server;
        public ClientInitializer(final AbstractServer server) {
            this.server = server;
        }

        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            NettyChannel handler = newClientHandler(this.server);
            handler.setChannel(ch);
            if(sslEnabled) {
                initClientSSL(p, server.getElectionAddr().getHostString(),
                        server.getElectionAddr().getPort());
            }

            p.addLast("clienthandler", handler);
        }
    }

    private void initClientSSL(ChannelPipeline p, final String host, int port)
            throws X509Exception, KeyManagementException,
            NoSuchAlgorithmException {
        SSLContext sslContext = x509Util.createSSLContext();
        SSLEngine sslEngine = sslContext.createSSLEngine();
        initSSL(p, true, sslEngine);
    }

    private void initServerSSL(ChannelPipeline p)
            throws X509Exception, KeyManagementException,
            NoSuchAlgorithmException {
        SSLContext sslContext = x509Util.createSSLContext();
        SSLEngine sslEngine = sslContext.createSSLEngine();
        initSSL(p, false, sslEngine);
        sslEngine.setNeedClientAuth(true);
    }

    private void initSSL(ChannelPipeline p, boolean clientMode,
                         SSLEngine sslEngine)
            throws X509Exception, KeyManagementException,
            NoSuchAlgorithmException {
        sslEngine.setUseClientMode(clientMode);

        p.addLast("ssl", new SslHandler(sslEngine));
    }

    private class ConnectListener implements ChannelFutureListener {
        private final AbstractServer server;
        public ConnectListener(AbstractServer server) {
            this.server = server;
        }

        @Override
        public void operationComplete(ChannelFuture future)
                throws Exception {
            if (future.isSuccess()) {
                SocketChannel sc = (SocketChannel)future.channel();
                ChannelHandlerContext ctx
                        = sc.pipeline().context(NettyChannel.class);

                NettyChannel handler = (NettyChannel)ctx.handler();
                sc.closeFuture().addListener(new ClosedListener());
                connectHandler(server, handler, true);
            } else {
                LOG.info("Connect failed to server: " + server.getElectionAddr()
                        + " reason: " + future.cause());
                connectHandler(server, null, false);
            }
        }
    }

    private class ClosedListener implements ChannelFutureListener {
        @Override
        public void operationComplete(ChannelFuture future)
                throws Exception {
            Channel sc = future.channel();
            closedHandler(getHandler(sc));
        }
    }
}
