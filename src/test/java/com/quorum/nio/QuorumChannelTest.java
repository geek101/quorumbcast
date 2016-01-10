/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>http://www.apache.org/licenses/LICENSE-2.0</p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quorum.nio;

import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.helpers.MockMsgChannel;
import com.quorum.helpers.PortAssignment;
import com.quorum.helpers.QuorumChannelWrapper;
import com.quorum.nio.msghelper.ReadRemainer;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import com.quorum.util.InitMessageCtx;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertTrue;

/**
 * Test 1: CONNECTING state test for VotingChannel upon creation with no
 *         server to connect to.
 * Test 2: With server on the other side test that upon connect
 * Created by powell on 11/15/15.
 */
public class QuorumChannelTest {
    private static final Logger LOG = LoggerFactory.getLogger
            (QuorumChannelTest.class);
    private static final long select_timeout_ms = 1;  /// Timeout in ms.
    private final Long myId = 2L;
    private final Long acceptId = 1L;  // Lower so does not close.
    private SelectorDispatch selectorDispatch = null;
    private final String listenHost = "localhost";
    private int listenPortStart = PortAssignment.unique();
    private int listenPort;
    private InetSocketAddress listenAddr = null;
    private List<MsgChannel> acceptedChannels = new LinkedList<>();
    final long rgenseed = System.currentTimeMillis();
    Random random = new Random(rgenseed);

    /**
     * Accept ready socket com.quorum.util.Callback
     */
    private class AcceptQuorumCb implements Callback<SocketChannel> {
        private final QuorumChannelTest qb;
        public AcceptQuorumCb(QuorumChannelTest qb) {
            this.qb = qb;
        }

        public void call(final SocketChannel o)
                throws ChannelException, IOException {
            this.qb.acceptQuorumHandler(o);
        }
    }

    private void acceptQuorumHandler(final SocketChannel channel)
            throws ChannelException, IOException {
        acceptedChannels.add(
                new QuorumChannelWrapper(
                        acceptId, listenAddr, channel,
                        selectorDispatch));
    }

    /**
     * Accept ready socket com.quorum.util.Callback
     */
    private class AcceptMockCb implements Callback<SocketChannel> {
        private final QuorumChannelTest qb;
        public AcceptMockCb(QuorumChannelTest qb) {
            this.qb = qb;
        }

        public void call(final SocketChannel o) throws IOException {
            this.qb.acceptMockHandler(o);
        }
    }
    private void acceptMockHandler(SocketChannel channel) throws IOException {
        acceptedChannels.add(
                new MockMsgChannel(acceptId, channel));
    }

    @Before
    public void setUp() throws IOException {
        try {
            listenPort = listenPortStart +
                    random.nextInt(PortAssignment.unique());
            this.listenAddr = new InetSocketAddress(
                    InetAddress.getByName(listenHost),
                    listenPort);
            selectorDispatch = new SelectorDispatch(listenAddr,
                    select_timeout_ms);
        } catch (IOException exp) {
            LOG.error("Error : %s", exp);
            throw new IOException(exp);
        }
    }

    @After
    public void tearDown() throws IOException {
        try {
            for (MsgChannel qw : acceptedChannels) {
                qw.close();
            }

            if (selectorDispatch != null) {
                selectorDispatch.close();
            }
        } catch (Exception exp) {
            LOG.error("Teardown error: %s", exp);
            throw exp;
        }
    }

    /**
     * Test that channel is in connecting state when opened.
     * There is no server on the other side.
     * @throws ChannelException
     * @throws IOException
     */
    @Test
    public void testOutgoingConnectingState()
            throws ChannelException, IOException {
        testIsConnectingHelper(3920, "88.88.88.88:2888:3888").close();
    }

    /**
     * Lets initiate an outbound channel and use the mock rx to
     * check if Hdr is received and it is in proper order.
     * @throws ChannelException
     * @throws IOException
     */
    @Test(timeout = 200)
    public void testOutgoingHeaderSentState()
            throws Exception {
        testQuorumToMockHdrHelper().close();
    }

    /**
     * Test the ensure we can send a message after hdr
     * is sent properly.
     * @throws Exception
     */
    @Test(timeout = 500)
    public void testOutgoingMsg() throws Exception {
        final QuorumChannelWrapper votingChannel
                = testQuorumToMockHdrHelper();
        final MockMsgChannel mockChannel
                = (MockMsgChannel) acceptedChannels.iterator().next();
        String str = "FooBarIsTheGame";

        Vote v = Vote.createRemoveVote(1111L);
        votingChannel.sendMsg(v);

        // Run selector till votingChannel sends all the data.
        runSelectorForPred(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return votingChannel.writeMsgCount() == 0;
            }
        });

        ByteBuffer readBuf = mockChannel.readMsgBlocking(2);
        Vote voteRx = Vote.buildVote(Unpooled.buffer(readBuf.remaining())
                .writeBytes(readBuf));
        assertTrue("vote match", voteRx.match(voteRx));

        votingChannel.close();
    }

    /**
     * Test that connecting to a VotingChannel works and it moves
     * to connected state after receiving the connection from Mock channel.
     * @throws Exception
     */
    @Test(timeout = 200)
    public void testIncomingConnectingState()
            throws Exception {
        testMockToQuorumConnectedHelper(acceptedChannels.size()+1).close();
    }

    /**
     * Test an inbound connection talking to an outgoing mock. Mock
     * will send the header hence verify the inbound header parsing.
     * @throws Exception
     */
    @Test (timeout = 200)
    public void testIncomingHdrRead() throws Exception {
        testMockToQuorumHdrRead().close();
    }

    /**
     * For an inbound VotingChannel verify that msg rx works after
     * hdr rx is done.
     * @throws Exception
     */
    @Test(timeout = 500)
    public void testIncomingMsgRead() throws Exception {
        final MockMsgChannel mockChannel = testMockToQuorumHdrRead();
        final QuorumChannelWrapper votingChannel
                = (QuorumChannelWrapper) acceptedChannels.iterator().next();

        // This is the message we should get.
        final Vote msg = new Vote(1L, 2L, 3L);

        ByteBuf b = msg.buildMsg();
        ByteBuffer buf = ByteBuffer.allocate(b.readableBytes());
        b.readBytes(buf);
        mockChannel.send(buf);

        // Run selector till votingChannel gets all the data.
        runSelectorForPred(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                Vote v = votingChannel.getMessage();
                if (v != null) {
                    return !msg.match(v);
                } else {
                    return false;
                }
            }
        });

        mockChannel.close();
    }

    private MockMsgChannel testMockToQuorumHdrRead() throws Exception {
        MockMsgChannel mockMsgChannel
                = testMockToQuorumConnectedHelper(acceptedChannels.size()+1);

        // Now send the hdr via the mock
        ByteBuffer hdr = prepHdr(this.myId, listenAddr);
        mockMsgChannel.send(hdr.duplicate());

        final QuorumChannelWrapper votingChannel
                = (QuorumChannelWrapper) acceptedChannels.iterator().next();
        // Run selector till votingChannel sends all the data.
        runSelectorForPred(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return votingChannel.getConnectedQuorumServer() == null;
            }
        });

        final QuorumServer quorumServer
                = votingChannel.getConnectedQuorumServer();
        assertTrue(quorumServer.id() == this.myId);
        assertTrue(InitMessageCtx.getAddrString(listenAddr).compareTo(
                InitMessageCtx.getAddrString(quorumServer.getElectionAddr())) == 0);

        return mockMsgChannel;
    }

    private QuorumChannelWrapper testQuorumToMockHdrHelper()
        throws Exception {
        // Lets get a channel in Connecting state.
        final QuorumChannelWrapper votingChannel
                = quorumConnectToMock(acceptedChannels.size()+1);

        // Run selector till votingChannel sends all the data.
        runSelectorForPred(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return !votingChannel.stateConnected();
            }
        });

        assertTrue(acceptedChannels.size() == 1);

        // Verify that VotingChannel reached READWRITE state.
        assertTrue(votingChannel.stateConnected());

        // Read the hdr from the mock channel.
        MockMsgChannel mockChannel
                = (MockMsgChannel)acceptedChannels.iterator().next();
        assertTrue(verifyMockChannelReadHdr(mockChannel, myId, listenAddr));
        return votingChannel;
    }

    private MockMsgChannel testMockToQuorumConnectedHelper(
            int acceptedChannelsSize) throws  Exception {
        MockMsgChannel mockMsgChannel
                = mockToQuorumConnected(acceptedChannels.size()+1);

        // Check if incoming connection is in connecting
        // state.
        QuorumChannelWrapper qc
                = (QuorumChannelWrapper) acceptedChannels.iterator().next();
        assertTrue(qc.stateConnecting());
        return mockMsgChannel;
    }

    private MockMsgChannel mockToQuorumConnected(
            int acceptedChannelsSize) throws Exception {
        selectorDispatch.registerAccept(new AcceptQuorumCb(this));
        MockMsgChannel mockMsgChannel
                = mockChannel(this.listenAddr);

        mockChannelConnect(mockMsgChannel);

        assertTrue(acceptedChannels.size() == acceptedChannelsSize);

        // This tests that selector does help mock channel
        // connect.
        assertTrue(mockMsgChannel.getState()
                == MockMsgChannel.State.CONNECTED);

        // un-register selector interest.
        mockMsgChannel.getChannel().
                keyFor(selectorDispatch.getSelector()).cancel();
        return mockMsgChannel;
    }

    /**
     * Helper for reuse of isConnecting code.
     * @param sid
     * @param hostAndPort
     * @return
     * @throws ChannelException
     * @throws IOException
     */
    private QuorumChannelWrapper testIsConnectingHelper(
            long sid, String hostAndPort) throws ChannelException, IOException {
        QuorumChannelWrapper votingChannel =
                quorumChannelCreate(sid, hostAndPort);

        assertTrue(votingChannel.stateConnecting());
        return votingChannel;
    }

    /**
     * Create a new VotingChannel that is outbound and return it.
     * @param sid
     * @param hostAndPort
     * @return
     * @throws ChannelException
     * @throws IOException
     */
    private QuorumChannelWrapper quorumChannelCreate(long sid, String
            hostAndPort)
            throws ChannelException, IOException {
        QuorumServer quorumServer = null;
        try {
            quorumServer = new QuorumServer(sid, hostAndPort);
        } catch (ChannelException exp) {
            LOG.error("Config exp: %s", exp);
            throw exp;
        }

        QuorumChannelWrapper votingChannel = null;
        try {
            votingChannel = new QuorumChannelWrapper(this.myId,
                    this.listenAddr, quorumServer, selectorDispatch);
        } catch (IOException exp) {
            LOG.error("Channel error: %s", exp);
            throw exp;
        }

        return votingChannel;
    }

    /**
     * Create a Mock message channel and return it.
     * @return
     * @throws Exception
     */
    private MockMsgChannel mockChannel(InetSocketAddress serverAddr)
            throws Exception {
        MockMsgChannel mockMsgChannel = null;
        try {
            mockMsgChannel = new MockMsgChannel(1L, serverAddr);
        } catch (IOException exp) {
            LOG.error("Mock channel error: %s", exp);
            throw exp;
        }

        return mockMsgChannel;
    }

    private void runSelectorForPred(Callable<Boolean> pred) throws Exception {
        // Run the selector till something happens
        try {
            while(pred.call()) {
                selectorDispatch.run();
            }
        } catch (Exception exp) {
            LOG.error("Error: %s", exp);
            throw exp;
        }
    }

    /**
     * Connect using the mock channel.
     * @param channel
     * @throws Exception
     */
    private void mockChannelConnect(MockMsgChannel channel)
            throws Exception {
        channel.connect();

        // Register this channel.
        selectorDispatch.registerConnect(channel);

        runSelectorForPred(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return acceptedChannels.size() == 0;
            }
        });
    }

    private ByteBuffer prepHdr(final long sid,
                                     final InetSocketAddress addr) {
        ByteBuffer buf = ByteBuffer.allocate(ReadRemainer.maxBuffer);
        final String electionAddrStr
                = InitMessageCtx.getAddrString(addr);
        buf.putLong(InitMessageCtx.PROTOCOL_VERSION)
                .putLong(sid)
                .putInt(electionAddrStr.getBytes().length)
                .put(electionAddrStr.getBytes());
        buf.flip();
        return buf;
    }

    private boolean verifyHdr(ByteBuffer buf, Long sid,
                              InetSocketAddress serverAddr) {
        long protocolVersion = buf.getLong();
        if (protocolVersion != InitMessageCtx.PROTOCOL_VERSION) {
            LOG.error("Invalid protocol ver: " + protocolVersion);
            return false;
        }

        long id = buf.getLong();
        if (id != sid) {
            LOG.error("Expected sid: " + sid + " got sid: " + id);
            return false;
        }

        final String serverAddrStr
                = InitMessageCtx.getAddrString(serverAddr);
        int addrLen = buf.getInt();
        if (addrLen != serverAddrStr.getBytes().length) {
            LOG.error("Expected addrlen: " + serverAddrStr.getBytes().length
                    + "got: " + addrLen);
            return false;
        }

        byte[] b = new byte[addrLen];
        buf.get(b);
        String addrStr = new String(b);
        if (addrStr.compareTo(serverAddrStr) != 0) {
            LOG.error("Expected electionAddr: " + serverAddrStr
                    + " got addr: " + addrStr);
            return false;
        }

        return true;
    }

    private boolean verifyMockChannelReadHdr(MockMsgChannel mockChannel ,
                                             long sid, InetSocketAddress addr)
            throws IOException {
        ByteBuffer buf = mockChannel.readHdrBlocking(2, addr);
        return verifyHdr(buf, sid, addr);
    }

    /**
     * Creates a Outbound quorom channel to a mock rx channel and
     * assures that hdr is sent on the tx side.
     * @return QuorumChannelWrapper
     * @throws Exception
     */
    private final QuorumChannelWrapper quorumConnectToMock(
            int acceptChannelsSize) throws Exception {
        selectorDispatch.registerAccept(new AcceptMockCb(this));

        // Lets get a channel in Connecting state.
        final QuorumChannelWrapper votingChannel
                = testIsConnectingHelper(myId, listenHost + ":2888:"
                + String.valueOf(listenPort));

        // Run selector till votingChannel sends all the data.
        runSelectorForPred(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return !votingChannel.stateConnected();
            }
        });

        assertTrue(acceptedChannels.size() == acceptChannelsSize);

        // Verify that VotingChannel reached READWRITE state.
        assertTrue(votingChannel.stateConnected());

        return votingChannel;
    }
}
