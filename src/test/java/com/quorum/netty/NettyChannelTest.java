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

import com.quorum.QuorumServer;
import com.quorum.helpers.PortAssignment;
import com.quorum.helpers.netty.*;
import com.quorum.util.ChannelException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test all aspects of NettyChannel
 */
public class NettyChannelTest extends BaseTest {
    @Before
    public void setup() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(1, executor);
    }

    @After
    public void tearDown() throws Exception {
        eventLoopGroup.shutdownGracefully().sync();
    }

    /**
     * Test that connect timeout works when enabled.
     * @throws InterruptedException
     */
    @Test(timeout = 1000)
    public void testConnectTimeout() throws InterruptedException {
        final QuorumServer q = new QuorumServer(1,
                new InetSocketAddress("localhost", PortAssignment.unique()));

        class TimeoutHandler extends MockChannel {
            public boolean connectTimeoutTriggered = false;
            public TimeoutHandler(long connectTimeoutMsec) {
                super(0, connectTimeoutMsec);
            }

            @Override
            public Long key() {
                return 0L;
            }

            @Override
            protected void connectTimeOut(ChannelHandlerContext ctx)
                    throws ChannelException {
                connectTimeoutTriggered = true;
                super.connectTimeOut(ctx);
            }
        }

        // We will timeout in 1 milliseconds.
        TimeoutHandler th = new TimeoutHandler(1);
        try {
            startConnectionAsync(q, eventLoopGroup, th);
        } catch (InterruptedException exp) {
            LOG.error("connect failed: " + exp);
            throw exp;
        }

        while(!th.connectTimeoutTriggered) {
            Thread.sleep(1);
        }

        assertTrue("connect timeout", th.connectTimeoutTriggered);
    }

    /**
     * Test read timeout works when enabled.
     * @throws InterruptedException
     * @throws ChannelException
     */
    @Test(timeout = 1000)
    public void testReadTimeout()
            throws InterruptedException, ChannelException {
        final QuorumServer q = new QuorumServer(1,
                new InetSocketAddress("localhost", PortAssignment.unique()));

        class TimeoutHandler extends MockChannel {
            public boolean readTimeoutTriggered = false;
            public TimeoutHandler(long readTimeoutMsec) {
                super(readTimeoutMsec, 0);
            }

            @Override
            public Long key() {
                return 1L;
            }

            @Override
            protected void connected(ChannelHandlerContext ctx)
                    throws ChannelException {
                super.connected(ctx);
                resetReadTimer();
            }

            @Override
            protected void readTimeOut(ChannelHandlerContext ctx)
                    throws ChannelException {
                readTimeoutTriggered = true;
                stopReadTimer();
                super.readTimeOut(ctx);
            }
        }

        class NullHandler extends MockChannel {
            @Override
            public Long key() {
                return 2L;
            }
        }

        // Start the listener.
        TimeoutHandler th = new TimeoutHandler(1);
        ChannelFuture listenFuture = null;
        try {
            listenFuture = startListener(q.getElectionAddr(), eventLoopGroup, th);
            listenFuture.sync();
        } catch(ChannelException exp) {
            LOG.error("unable to listen, exp:" + exp);
            throw exp;
        }

        // Try to connect.
        try {
            startConnectionSync(q, eventLoopGroup, new NullHandler(), 10);
        } catch(InterruptedException exp) {
            LOG.error("unable to connect to server: " + q
                    + " exp:" + exp);
            listenFuture.channel().close().syncUninterruptibly();
            throw exp;
        }

        while(!th.readTimeoutTriggered) {
            Thread.sleep(1);
        }

        assertTrue("read timeout", th.readTimeoutTriggered);
    }

    /**
     * Send hdr transmit and receive works for netty-channel.
     */
    @Test (timeout = 1000)
    public void testHdrTxRx() throws Exception {
        final String hdrToSend = "HelloHdr!";
        int port = PortAssignment.unique();
        final InetSocketAddress listenAddr
                = new InetSocketAddress("localhost", port);
        QuorumServer q = new QuorumServer(1, listenAddr);;

        // Start the listener.
        MockReadHdrChannel hdrRcvHandler
                = new MockReadHdrChannel(1, hdrToSend.length(), 10);
        ChannelFuture listenFuture = null;
        try {
            listenFuture = startListener(q.getElectionAddr(),
                    eventLoopGroup, hdrRcvHandler);
            listenFuture.sync();
        } catch(ChannelException exp) {
            LOG.error("unable to listen, exp:" + exp);
            throw exp;
        }

        // Try to connect.
        try {
            startConnectionSync(q, eventLoopGroup, newHdrSenderHandler(2,
                    ByteBuffer.wrap(hdrToSend.getBytes()), 10), 10);
        } catch(InterruptedException exp) {
            LOG.error("unable to connect to server: " + q
                    + " exp:" + exp);
            listenFuture.channel().close().syncUninterruptibly();
            throw exp;
        }

        while(hdrRcvHandler.getHdrReceived() == null) {
            Thread.sleep(5);
        }

        assertTrue("HelloHdr rcvd",
                hdrRcvHandler.getHdrReceived().compareTo(ByteBuffer.wrap
                        (hdrToSend.getBytes())) == 0);
    }

    /**
     * Test data transmit and receive after header exchange with read timeout
     * enabled.
     * @throws Exception
     */
    @Test (timeout = 1000)
    public void testDataTxRx() throws Exception {
        final String hdrToSend = "HelloHdr!";
        final String dataToSend = "FooBar";
        int port = PortAssignment.unique();
        final InetSocketAddress listenAddr
                = new InetSocketAddress("localhost", port);
        QuorumServer q = new QuorumServer(1, listenAddr);;

        // Start the listener.
        MockReadDataChannel dataRcvHandler
                = new MockReadDataChannel(1, hdrToSend.length(), 10) {
            @Override
            protected void readMsg(ChannelHandlerContext ctx, ByteBuf in) {
                if (in.readableBytes() < dataToSend.length()) {
                    return;
                }

                readMsg = ByteBuffer.allocate(dataToSend.length());
                in.readBytes(readMsg);
                readMsg.flip();
            }
        };

        ChannelFuture listenFuture;
        try {
            listenFuture = startListener(q.getElectionAddr(), eventLoopGroup,
                    dataRcvHandler);
            listenFuture.sync();
        } catch(ChannelException exp) {
            LOG.error("unable to listen, exp:" + exp);
            throw exp;
        }

        // Try to connect.
        final MockSendDataChannel dataSendingChannel
                = new MockSendDataChannel(
                2, ByteBuffer.wrap(hdrToSend.getBytes()), 10);
        try {
            startConnectionSync(q, eventLoopGroup,
                    new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch)
                                throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            dataSendingChannel.setChannel(ch);
                            p.addLast("dataRx", dataSendingChannel);
                        }
                    }, 10);
        } catch(InterruptedException exp) {
            LOG.error("unable to connect to server: " + q
                    + " exp:" + exp);
            listenFuture.channel().close().syncUninterruptibly();
            throw exp;
        }

        while(dataRcvHandler.getHdrReceived() == null) {
            Thread.sleep(5);
        }

        assertTrue("HelloHdr rcvd",
                dataRcvHandler.getHdrReceived().compareTo(ByteBuffer.wrap
                        (hdrToSend.getBytes())) == 0);

        // send the data
        dataSendingChannel.sendMsg(ByteBuffer.wrap(dataToSend.getBytes()));

        while(dataRcvHandler.readMsg == null) {
            Thread.sleep(5);
        }

        assertTrue("FooBar rcvd",
                dataRcvHandler.readMsg.compareTo(ByteBuffer.wrap
                        (dataToSend.getBytes())) == 0);
    }

    @Test (timeout = 2000)
    public void testKeepAliveTimeout() throws Exception {
        final long keepAliveTimeoutMsec = 100;
        final int keepAliveCount = 3;
        final String hdrToSend = "HelloHdr!";
        final String dataToSend = "FooBar";
        int port = PortAssignment.unique();
        final InetSocketAddress listenAddr
                = new InetSocketAddress("localhost", port);
        QuorumServer q = new QuorumServer(1, listenAddr);;

        // Start the listener.
        MockReadDataChannel dataRcvHandler
                = new MockReadDataChannel(1, hdrToSend.length(), 10) {
            @Override
            protected void readMsg(final ChannelHandlerContext ctx,
                                   final ByteBuf in) {
                if (in.readableBytes() < dataToSend.length()) {
                    return;
                }

                readMsg = ByteBuffer.allocate(dataToSend.length());
                in.readBytes(readMsg);
                readMsg.flip();
                readMsgCount++;
            }
        };

        ChannelFuture listenFuture;
        try {
            listenFuture = startListener(q.getElectionAddr(), eventLoopGroup,
                    dataRcvHandler);
            listenFuture.sync();
        } catch(ChannelException exp) {
            LOG.error("unable to listen, exp:" + exp);
            throw exp;
        }

        // Try to connect.
        final MockSendDataKeepAliveChannel dataSendingKeepAliveChannel
                = new MockSendDataKeepAliveChannel(
                2, ByteBuffer.wrap(hdrToSend.getBytes()),
                ByteBuffer.wrap(dataToSend.getBytes()),
                keepAliveTimeoutMsec, keepAliveCount);
        try {
            startConnectionSync(q, eventLoopGroup,
                    new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch)
                                throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            dataSendingKeepAliveChannel.setChannel(ch);
                            p.addLast("dataRx", dataSendingKeepAliveChannel);
                        }
                    }, 5);
        } catch(InterruptedException exp) {
            LOG.error("unable to connect to server: " + q
                    + " exp:" + exp);
            listenFuture.channel().close().syncUninterruptibly();
            throw exp;
        }

        while(dataRcvHandler.getHdrReceived() == null) {
            Thread.sleep(1);
        }

        assertTrue("HelloHdr rcvd",
                dataRcvHandler.getHdrReceived().compareTo(ByteBuffer.wrap
                        (hdrToSend.getBytes())) == 0);

        // send the data
        dataSendingKeepAliveChannel.sendMsg(
                ByteBuffer.wrap(dataToSend.getBytes()));

        while(dataRcvHandler.readMsg == null) {
            Thread.sleep(1);
        }

        assertTrue("FooBar rcvd",
                dataRcvHandler.readMsg.compareTo(ByteBuffer.wrap
                        (dataToSend.getBytes())) == 0);


        // verify three keep alive messages and a disconnect
        dataRcvHandler.resetReadBuffer();
        dataSendingKeepAliveChannel.startKeepAliveTimer();
        LOG.info("started keep alive timer");
        while(dataRcvHandler.readMsgCount < keepAliveCount) {
            Thread.sleep(1);
        }

        assertTrue("FooBar rcvd",
                dataRcvHandler.readMsg.compareTo(ByteBuffer.wrap
                        (dataToSend.getBytes())) == 0);

        assertEquals(keepAliveCount, dataRcvHandler.readMsgCount);

        while(!dataSendingKeepAliveChannel.keepAliveDisconnect) {
            Thread.sleep(10);
        }

        assertTrue("keep alive disconnect",
                dataSendingKeepAliveChannel.keepAliveDisconnect);
    }

    @Test //(timeout = 2000)
    public void testKeepAliveNotTimeout() throws Exception {
        final long keepAliveTimeoutMsec = 200;
        final int keepAliveCount = 3;
        final String hdrToSend = "HelloHdr!";
        final String dataToSend = "FooBar";
        int port = PortAssignment.unique();
        final InetSocketAddress listenAddr
                = new InetSocketAddress("localhost", port);
        QuorumServer q = new QuorumServer(1, listenAddr);;

        // Start the listener.
        MockReadDataChannel dataRcvHandler
                = new MockReadDataChannel(1, hdrToSend.length(), 10) {
            @Override
            protected void readMsg(final ChannelHandlerContext ctx,
                                   final ByteBuf in) {
                synchronized (this) {
                    if (in.readableBytes() < dataToSend.length()) {
                        return;
                    }

                    readMsg = ByteBuffer.allocate(dataToSend.length());
                    in.readBytes(readMsg);
                    readMsg.flip();
                    readMsgCount++;
                }
            }
        };

        ChannelFuture listenFuture;
        try {
            listenFuture = startListener(q.getElectionAddr(), eventLoopGroup,
                    dataRcvHandler);
            listenFuture.sync();
        } catch(ChannelException exp) {
            LOG.error("unable to listen, exp:" + exp);
            throw exp;
        }

        // Try to connect.
        final MockSendDataKeepAliveChannel dataSendingKeepAliveChannel
                = new MockSendDataKeepAliveChannel(
                2, ByteBuffer.wrap(hdrToSend.getBytes()),
                ByteBuffer.wrap(dataToSend.getBytes()),
                keepAliveTimeoutMsec, keepAliveCount);
        try {
            startConnectionSync(q, eventLoopGroup,
                    new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch)
                                throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            dataSendingKeepAliveChannel.setChannel(ch);
                            p.addLast("dataRx", dataSendingKeepAliveChannel);
                        }
                    }, 5);
        } catch(InterruptedException exp) {
            LOG.error("unable to connect to server: " + q
                    + " exp:" + exp);
            listenFuture.channel().close().syncUninterruptibly();
            throw exp;
        }

        while(dataRcvHandler.getHdrReceived() == null) {
            Thread.sleep(1);
        }

        assertTrue("HelloHdr rcvd",
                dataRcvHandler.getHdrReceived().compareTo(ByteBuffer.wrap
                        (hdrToSend.getBytes())) == 0);

        // send the data
        dataSendingKeepAliveChannel.sendMsg(
                ByteBuffer.wrap(dataToSend.getBytes()));

        while(dataRcvHandler.readMsg == null) {
            Thread.sleep(1);
        }

        assertTrue("FooBar rcvd",
                dataRcvHandler.readMsg.compareTo(ByteBuffer.wrap
                        (dataToSend.getBytes())) == 0);


        // verify three keep alive messages and a disconnect
        dataRcvHandler.resetReadBuffer();
        dataSendingKeepAliveChannel.startKeepAliveTimer();
        LOG.info("started keep alive timer");
        while(dataRcvHandler.readMsgCount < keepAliveCount-2) {
            Thread.sleep(1);
        }

        assertTrue("FooBar rcvd",
                dataRcvHandler.readMsg.compareTo(ByteBuffer.wrap
                        (dataToSend.getBytes())) == 0);

        // Sending this will make keep alive channel not reset
        dataRcvHandler.sendMsg(ByteBuffer.wrap(dataToSend.getBytes()));
        LOG.info("msg sent to keep alive channel");

        while(dataRcvHandler.readMsgCount < keepAliveCount) {
            Thread.sleep(1);
        }

        assertTrue("keep alive did not disconnect",
                !dataSendingKeepAliveChannel.keepAliveDisconnect);
    }

    /**
     * Returns a handler with sndHdr implemented with sending the
     * given ByteBuffer.
     * @param hdr
     * @return
     */
    private NettyChannel newHdrSenderHandler(
            final long key, final ByteBuffer hdr,
            final long connectTimeoutMsec) {
        return new MockSndHdrChannel(key, hdr, connectTimeoutMsec) {
            @Override
            protected ByteBuf buildHdr(ChannelHandlerContext ctx) {
                return this.getHdr();
            }
        };
    }
}
