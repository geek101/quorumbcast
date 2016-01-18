/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quorum.netty;

import com.common.X509Exception;
import com.quorum.AbstractServer;
import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.util.AsyncClientSocket;
import com.quorum.util.AsyncServerSocket;
import com.quorum.util.ChannelException;
import com.quorum.util.QuorumSocketFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import static com.quorum.util.InitMessageCtx.PROTOCOL_VERSION;
import static com.quorum.util.InitMessageCtx.getAddrString;
import static io.netty.buffer.Unpooled.buffer;
import static org.junit.Assert.assertTrue;

public class BaseTest {
    protected static final Logger LOG = LoggerFactory.getLogger(
            BaseTest.class.getName());
    protected static ArrayList<String> keyStore
            = new ArrayList<>(Arrays.asList("x509ca/java/node1.ks",
                    "x509ca/java/node2.ks",
                    "x509ca/java/node3.ks"));
    protected static ArrayList<String> keyPassword
            = new ArrayList<>(
            Arrays.asList("CertPassword1", "CertPassword1", "CertPassword1"));
    protected static ArrayList<String> trustStore = new ArrayList<>(
            Arrays.asList("x509ca/java/truststore.jks"));
    protected static ArrayList<String> trustPassword
            = new ArrayList<>(Arrays.asList("StorePass"));
    protected static String trustStoreCAAlias = "ca";

    protected static ArrayList<String> badKeyStore
            = new ArrayList<>(Arrays.asList("x509ca2/java/node1.ks",
            "x509ca2/java/node2.ks",
            "x509ca2/java/node3.ks"));
    protected static ArrayList<String> badKeyPassword
            = new ArrayList<>(
            Arrays.asList("CertPassword1", "CertPassword1", "CertPassword1"));
    protected static ArrayList<String> badTrustStore = new ArrayList<>(
            Arrays.asList("x509ca2/java/truststore.jks"));
    protected static ArrayList<String> badTrustPassword
            = new ArrayList<>(Arrays.asList("StorePass"));
    protected static String badTrustStoreCAAlias = "ca";

    protected final ExecutorService executor = Executors
            .newSingleThreadExecutor();
    protected EventLoopGroup eventLoopGroup = null;

    protected boolean sslEnabled = true;

    protected ServerSocket serverSocket = null;
    protected Socket clientSocket1 = null;
    protected Socket clientSocket2 = null;

    protected ChannelFuture startListener(final InetSocketAddress listenerAddr,
                                          final EventLoopGroup group,
                                          final NettyChannel handler)
            throws ChannelException {

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(group)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new AcceptInitializer(handler))
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_LINGER, 0);

        return (serverBootstrap.bind(listenerAddr));
    }

    protected class AcceptInitializer
            extends ChannelInitializer<SocketChannel> {
        private final NettyChannel handler;
        public AcceptInitializer(NettyChannel handler) {
            this.handler = handler;
        }
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            handler.setChannel(ch);
            p.addLast("serverhandler", handler);
        }
    }

    /**
     * Synchronous connector.
     * @param server
     * @param group
     * @param handler
     * @throws InterruptedException
     */
    protected void startConnectionSync(
            final AbstractServer server, final EventLoopGroup group,
            final ChannelHandler handler, long timeoutMsec)
            throws InterruptedException {
        // Create an new Channel.
        startConnectionHelper(group, handler)
                .connect(server.getElectionAddr()).sync().await(timeoutMsec);
    }

    protected Future startConnectionAsync(final AbstractServer server,
                                          final EventLoopGroup group,
                                          final ChannelHandler handler)
            throws InterruptedException {
        // Create an new Channel and return the future.
        return startConnectionHelper(group, handler)
                .connect(server.getElectionAddr());
    }

    protected Bootstrap startConnectionHelper(
            final EventLoopGroup group, final ChannelHandler handler) {
        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(handler)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_LINGER, 0);
        return clientBootstrap;
    }

    protected Collection<Socket> connectOneClientToServerTest(
            final QuorumServer from, final QuorumServer to,
            int clientIndex, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException {

        return connectOneClientToServerTest(newClient(from, clientIndex),
                to, serverIndex);
    }

    protected Collection<Socket> connectOneBadClientToServerTest(
            final QuorumServer from, final QuorumServer to,
            int clientIndex, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException {

        return connectOneClientToServerTest(newBadClient(from, clientIndex),
                to, serverIndex);
    }

    protected Collection<Socket> connectOneClientToServerTest(
            final Socket client, final QuorumServer to, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException {
        serverSocket = newServerAndBindTest(to, serverIndex);

        clientSocket1 = client;
        FutureTask<Socket> clientSocketFuture
                = new AsyncClientSocket(clientSocket1).connect(to);
        FutureTask<Socket> serverSocketFuture
                = new AsyncServerSocket(serverSocket).accept();

        while (!clientSocketFuture.isDone()
                || !serverSocketFuture.isDone()) {
            Thread.sleep(2);
        }

        assertTrue("connected", clientSocketFuture.get().isConnected());
        assertTrue("accepted", serverSocketFuture.get().isConnected());
        return Collections.unmodifiableList(
                Arrays.asList(clientSocketFuture.get(),
                        serverSocketFuture.get()));
    }

    protected Socket newClient(final QuorumServer from, int index)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        return startClient(from, keyStore.get(index), keyPassword.get(index),
                trustStore.get(0), trustPassword.get(0), trustStoreCAAlias);
    }

    protected Socket newBadClient(final QuorumServer from, int index)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        return startClient(from, badKeyStore.get(index),
                badKeyPassword.get(index),
                badTrustStore.get(0), badTrustPassword.get(0),
                badTrustStoreCAAlias);
    }

    protected Socket startClient(final QuorumServer from,
                               final String keyStoreLocation,
                               final String keyStorePassword,
                               final String trustStoreLocation,
                               final String trustStorePassword,
                               final String trustStoreCAAlias)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        ClassLoader cl = getClass().getClassLoader();
        if (sslEnabled) {
            return QuorumSocketFactory.createForSSL(
                    cl.getResource(keyStoreLocation).getFile(),
                    keyStorePassword,
                    cl.getResource(trustStoreLocation).getFile(),
                    trustStorePassword,
                    trustStoreCAAlias).buildForClient();
        } else {
            return QuorumSocketFactory.createWithoutSSL().buildForClient();
        }
    }

    protected ServerSocket newServerAndBindTest(final QuorumServer server,
                                              int index)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        ServerSocket s = startListener(server, index);
        s.setReuseAddress(true);
        assertTrue("bind worked", s.isBound());
        return s;
    }

    protected ServerSocket startListener(final QuorumServer server, int index)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        ClassLoader cl = getClass().getClassLoader();
        if (sslEnabled) {
            return QuorumSocketFactory.createForSSL(
                    cl.getResource(keyStore.get(index)).getFile(),
                    keyPassword.get(index),
                    cl.getResource(trustStore.get(0)).getFile(),
                    trustPassword.get(0),
                    trustStoreCAAlias)
                    .buildForServer(server.getElectionAddr().getPort(),
                            server.getElectionAddr().getAddress());
        } else {
            return QuorumSocketFactory.createWithoutSSL()
                    .buildForServer(server.getElectionAddr().getPort(),
                            server.getElectionAddr().getAddress());
        }
    }

    protected Socket connectAndAssert(final QuorumServer from,
                                    final QuorumServer to)
            throws IOException, X509Exception, NoSuchAlgorithmException {
        // Connect and write a hdr, check the return value
        Socket socket = newClient(from, 1);
        socket.setTcpNoDelay(true);
        socket.setSoLinger(true, 0);
        socket.setKeepAlive(true);
        socket.connect(to.getElectionAddr());

        assertTrue("connected", socket.isConnected());
        return socket;
    }

    protected void sendVerifyVote(long leader, long zxid,
                                  long sid, Socket socket,
                                  ConcurrentLinkedQueue<Vote> inboundVoteQueue)
            throws IOException, InterruptedException {
        Vote v = new Vote(leader, zxid, sid);

        ByteBuf voteBuf = v.buildMsg();
        writeBufToSocket(voteBuf, socket);

        Vote vRx = null;
        while ((vRx = inboundVoteQueue.poll()) == null) {
            Thread.sleep(1);
        }
        LOG.debug("Got vote: " + vRx);
        assertTrue(vRx.match(v));
    }

    protected void writeBufToSocket(ByteBuf byteBuf, Socket socket)
            throws IOException {
        int size = byteBuf.readableBytes();
        byte b[] = new byte[size];
        byteBuf.readBytes(b);
        socket.getOutputStream().write(b);
    }

    protected ByteBuf buildHdr(long sid, InetSocketAddress myElectionAddr) {
        ByteBuf buf = buffer(2046);
        final String electionAddrStr
                = getAddrString(myElectionAddr);
        buf.writeLong(PROTOCOL_VERSION)
                .writeLong(sid)
                .writeInt(electionAddrStr.getBytes().length)
                .writeBytes(electionAddrStr.getBytes());
        return buf;
    }

    protected boolean hdrEquals(long sid, InetSocketAddress myElectionAddr,
                                ByteBuf hdrRx) {
        final String electionAddrStr
                = getAddrString(myElectionAddr);
        boolean val1 = hdrRx.readLong() == PROTOCOL_VERSION &&
                hdrRx.readLong() == sid &&
                hdrRx.readInt() == electionAddrStr.getBytes().length;

        byte[] b = new byte[electionAddrStr.getBytes().length];
        hdrRx.readBytes(b);
        String readAddr = new String(b);
        return val1 && readAddr.compareTo(electionAddrStr) == 0;
    }

    protected ByteBuf readHelper(final Socket socket, int len)
            throws IOException {
        byte[] b = new byte[len];
        BufferedInputStream bis =
                new BufferedInputStream(socket.getInputStream());
        int readLen = 0;
        while(readLen < len) {
            readLen += bis.read(b, readLen, len-readLen);
        }

        ByteBuf buf = Unpooled.buffer(len);
        buf.writeBytes(b);
        return buf;
    }

    protected void socketIsClosed(final Socket socket, final ByteBuf buf)
            throws InterruptedException, IOException {
        while(!socket.isClosed()) {
            Thread.sleep(100);
            try {
                socket.getInputStream().read();
                if (buf != null) {
                    writeBufToSocket(buf, socket);
                } else {
                    socket.getOutputStream().write(new byte[1]);
                }
            } catch (IOException exp) {
                LOG.info("client socket read exp: {}", exp);
                throw exp;
            }
        }
    }

    protected void setSockOptions(final Socket socket) throws SocketException {
        socket.setSoLinger(true, 0);
        socket.setReuseAddress(true);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
    }

    protected void close(ServerSocket s) {
        try {
            s.close();
        } catch (IOException e) {
        }
    }

    protected void close(Socket s) {
        try {
            s.close();
        } catch (IOException e) {}
    }
}
