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

import com.common.X509Util;
import com.quorum.AbstractServer;
import com.quorum.helpers.PortAssignment;
import com.quorum.helpers.netty.MockChannel;
import com.quorum.util.ChannelException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.common.X509Exception.KeyManagerException;
import static com.common.X509Exception.TrustManagerException;
import static org.junit.Assert.*;

public class NettyChannelMgrTest extends BaseTest {
    private static final Logger LOG = LoggerFactory.getLogger
            (QuorumBroadcastTest.class);
    private final ExecutorService executor
            = Executors.newSingleThreadExecutor();
    private EventLoopGroup eventLoopGroup = null;
    private final boolean sslEnabled = false;

    @Before
    public void setup() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(1, executor);
    }

    @After
    public void tearDown() throws Exception {
        eventLoopGroup.shutdownGracefully().sync();
    }

    @Test(timeout = 1000)
    public void testServerSide()
            throws ChannelException, IOException,
            CertificateException, InterruptedException,
            NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException  {
        final InetSocketAddress listenAddr
                = new InetSocketAddress("localhost", PortAssignment.unique());
        final ClassLoader cl = getClass().getClassLoader();

        final MockChannel testHandler = new MockChannel() {
            @Override
            public Long key() {
                return 13312L;
            }
        };

        class TestMgr extends NettyChannelMgr {
            public boolean newAcceptHandlerCalled = false;
            public NettyChannel acceptedHandler = null;
            public NettyChannel closedHandler = null;
            public TestMgr() throws NoSuchAlgorithmException,
                    KeyManagerException, TrustManagerException {
                super(eventLoopGroup, sslEnabled,
                        X509Util.SSL_VERSION,
                        cl.getResource(keyStore.get(0)).getFile(),
                        keyPassword.get(0),
                        cl.getResource(trustStore.get(0)).getFile(),
                        trustPassword.get(0), trustStoreCAAlias);
            }
            public boolean isListening() {
                return acceptChannelFuture.isDone();
            }
            @Override
            protected NettyChannel newAcceptHandler() {
                newAcceptHandlerCalled = true;
                return testHandler;
            }

            @Override
            protected NettyChannel newClientHandler(
                    AbstractServer server) {
                assertFalse("not implemented", true);
                return null;
            }

            @Override
            protected void connectHandler(
                    AbstractServer server, NettyChannel handler,
                    boolean success) { assertFalse("not implemented", true); }

            @Override
            protected void acceptHandler(NettyChannel handler) {
                acceptedHandler = handler;
            }
            @Override
            protected void closedHandler(NettyChannel handler) {
                closedHandler = handler;
            }
        };

        TestMgr mgr = new TestMgr();
        mgr.startListener(listenAddr);

        while(!mgr.isListening()) {
            Thread.sleep(2);
        }

        // Start a connection this should trigger accept.
        Socket s = new Socket(listenAddr.getHostName(), listenAddr.getPort());

        while(!mgr.newAcceptHandlerCalled) {
            Thread.sleep(2);
        }

        assertTrue("call invoked", mgr.newAcceptHandlerCalled);
        assertSame("same handler", mgr.acceptedHandler, testHandler);
        assertTrue("handler key match", ((MockChannel)mgr.acceptedHandler).key()
                        .equals(testHandler.key()));

        s.close();

        while(mgr.closedHandler == null) {
            Thread.sleep(2);
        }

        assertSame("closed invoked", mgr.closedHandler, testHandler);
        mgr.shutdown();
    }

    @Test(timeout = 1000)
    public void testClientSide()
            throws ChannelException, IOException,
            CertificateException, InterruptedException,
            NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        final InetSocketAddress listenAddr
                = new InetSocketAddress("localhost", PortAssignment.unique());
        final ClassLoader cl = getClass().getClassLoader();

        final MockChannel testHandler = new MockChannel() {
            @Override
            public Long key() {
                return 23322L;
            }
        };

        class TestMgr extends NettyChannelMgr {
            public boolean newConnectHandlerCalled = false;
            public NettyChannel connectedHandler = null;
            public Boolean connectedSuccess = null;
            public NettyChannel closedHandler = null;
            public TestMgr() throws NoSuchAlgorithmException,
            KeyManagerException, TrustManagerException {
                super(eventLoopGroup, sslEnabled,
                        X509Util.SSL_VERSION,
                        cl.getResource(keyStore.get(0)).getFile(),
                        keyPassword.get(0),
                        cl.getResource(trustStore.get(0)).getFile(),
                        trustPassword.get(0), trustStoreCAAlias);
            }

            @Override
            protected NettyChannel newAcceptHandler() {
                assertFalse("Not implemented", true);
                return null;
            }

            @Override
            protected NettyChannel newClientHandler(
                    AbstractServer server) {
                newConnectHandlerCalled = true;
                return testHandler;
            }

            @Override
            protected void connectHandler(
                    AbstractServer server, NettyChannel handler,
                    boolean success) {
                connectedHandler = handler;
                connectedSuccess = success;
            }

            @Override
            protected void acceptHandler(NettyChannel handler) {
                assertFalse("Not implemented", true);
            }

            @Override
            protected void closedHandler(NettyChannel handler) {
                closedHandler = handler;
            }
        };

        // Start the listening socket.
        ServerSocket ss = new ServerSocket(listenAddr.getPort(),
                10, listenAddr.getAddress());

        TestMgr mgr = new TestMgr();
        mgr.startConnection(new AbstractServer() {
            @Override
            public InetSocketAddress getElectionAddr() {
                return listenAddr;
            }
        });

        // This should return
        Socket s = ss.accept();

        assertTrue("server connected", s.isConnected());

        while (mgr.connectedHandler == null) {
            Thread.sleep(2);
        }

        assertTrue("call invoked", mgr.newConnectHandlerCalled);
        assertSame("same handler", mgr.connectedHandler, testHandler);
        assertTrue("handler key match",
                ((MockChannel)mgr.connectedHandler).key()
                        .equals(testHandler.key()));

        // close from server side.
        s.close();

        while(mgr.closedHandler == null) {
            Thread.sleep(2);
        }

        assertSame("closed invoked", mgr.closedHandler, testHandler);

        ss.close();
        mgr.shutdown();
    }
}
