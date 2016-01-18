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
import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.helpers.PortAssignment;
import com.quorum.util.Callback;
import com.quorum.util.Callback2;
import com.quorum.util.ChannelException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test all aspects of VotingChannel, with plain sockets.
 */
public class VotingChannelTest extends BaseTest {
    private static final Long READ_TIMEOUT_MS = 100L;
    private static final Long CONNECT_TIMEOUT_MS = 5L;

    QuorumServer listenServer;
    private final QuorumServer client1
            = new QuorumServer(2, new InetSocketAddress("localhost",
            PortAssignment.unique()));

    private final ConcurrentLinkedQueue<Long> inboundSidQueue
            = new ConcurrentLinkedQueue<>();

    final class SidLearnedCb implements Callback2 {
        @Override
        public void done(Object o, Object p) throws ChannelException {
            inboundSidQueue.add((Long)o);
        }
    }

    private final SidLearnedCb sidLearnedCb = new SidLearnedCb();

    private final ConcurrentLinkedQueue<Vote> inboundVoteQueue
            = new ConcurrentLinkedQueue<>();

    private final class MsgRxCb implements Callback<Vote> {
        @Override
        public void call(final Vote o) throws ChannelException, IOException {
            inboundVoteQueue.add(o);
        }
    }

    private final MsgRxCb msgRxCb = new MsgRxCb();

    @Before
    public void setup() throws Exception {
        sslEnabled = false;
        eventLoopGroup = new NioEventLoopGroup(1, executor);
    }

    @After
    public void tearDown() throws Exception {
        eventLoopGroup.shutdownGracefully().sync();
    }

    @Test (timeout = 1000)
    public void testClientChannelHdrSend() throws IOException, X509Exception,
            NoSuchAlgorithmException, InterruptedException {
        ImmutablePair<VotingClientChannel, Socket> pair
                = clientChannelConnectHdrAssert(
                PortAssignment.unique(), client1);

        pair.getRight().close();
        pair.getLeft().close();
        serverSocket.close();
        serverSocket = null;
    }

    @Test (timeout = 1000)
    public void testClientChannelVoteRead() throws IOException, X509Exception,
            NoSuchAlgorithmException, InterruptedException {
        ImmutablePair<VotingClientChannel, Socket> pair
                = clientChannelConnectHdrAssert(
                PortAssignment.unique(), client1);

        Socket socket = pair.getRight();
        // Send a Vote and check if we receive that.
        assertEquals(inboundVoteQueue.size(), 0);
        sendVerifyVote(123L, 456L, 789L, socket, inboundVoteQueue);

        // Send another Vote
        assertEquals(inboundVoteQueue.size(), 0);
        sendVerifyVote(111L, 222L, 333L, socket, inboundVoteQueue);
    }

    @Test (timeout = 1000)
    public void testServerChannelHdrRead() throws IOException, X509Exception,
            NoSuchAlgorithmException, InterruptedException, ChannelException {
        listenServer = new QuorumServer(1,
                new InetSocketAddress("localhost", PortAssignment.unique()));

        ChannelFuture listenFuture = startListenAndConnectClient(listenServer);

        // Connect and write a hdr, check the return value
        Socket socket = connectAndAssert(client1, listenServer);

        assertEquals(inboundSidQueue.size(), 0);

        ByteBuf hdrBuf = buildHdr(client1.id(), client1.getElectionAddr());
        writeBufToSocket(hdrBuf, socket);

        Long sidLearned;
        while((sidLearned = inboundSidQueue.poll()) == null) {
            Thread.sleep(1);
        }

        assertEquals(sidLearned.longValue(), client1.id());
        socket.close();
        listenFuture.sync().channel().close().sync();
    }

    /**
     * Test that connecting to higher SID will cause this channel to die.
     * @throws IOException
     * @throws X509Exception
     * @throws NoSuchAlgorithmException
     * @throws InterruptedException
     * @throws ChannelException
     */
    @Test (timeout = 1000, expected = SocketException.class)
    public void testServerChannelResolveSidClose() throws IOException,
            X509Exception, NoSuchAlgorithmException, InterruptedException,
            ChannelException {
        listenServer = new QuorumServer(4,
                new InetSocketAddress("localhost", PortAssignment.unique()));
        ChannelFuture listenFuture;
        Socket socket;
        ByteBuf hdrBuf = buildHdr(client1.id(), client1.getElectionAddr());
        try {
            listenFuture = startListenAndConnectClient(listenServer);

            // Connect and write a hdr, check the return value
            socket = connectAndAssert(client1, listenServer);
            writeBufToSocket(hdrBuf, socket);
        } catch (IOException exp) {
            LOG.error("Invalid io exception: " + exp);
            throw new RuntimeException(exp);
        }

        // Verify that the above triggers a disconnect
        socketIsClosed(socket, null);

        listenFuture.sync().channel().close().sync();
    }

    /**
     * Test same vote does not get sent twice.
     * @throws IOException
     * @throws X509Exception
     * @throws NoSuchAlgorithmException
     * @throws InterruptedException
     */
    @Test (timeout = 1000)
    public void testVoteSendTwoTimes() throws IOException, X509Exception,
            NoSuchAlgorithmException, InterruptedException, ChannelException {
        ImmutableTriple<VotingClientChannel, Socket, Vote> triple
                = clientChannelConnectHdrAndVoteAssert(
                PortAssignment.unique(), client1);

        final VotingClientChannel votingClientChannel = triple.getLeft();
        final Socket socket = triple.getMiddle();
        final Vote v = triple.getRight();

        // Send it again.
        while(!votingClientChannel.sendVote(v)) {
            Thread.sleep(1);
        }

        socket.setSoTimeout(100);
        ByteBuf voteReadBuf = null;
        try {
            voteReadBuf = readHelper(socket, v.buildMsg().readableBytes());
        } catch (SocketTimeoutException exp) {}

        assertTrue("vote not received", voteReadBuf == null);
    }

    /**
     * Test that if Vote if entire Vote is not sent by other end the
     * channel will shutdown.
     * @throws IOException
     * @throws X509Exception
     * @throws NoSuchAlgorithmException
     * @throws InterruptedException
     */
    @Test (timeout = 1000, expected = SocketException.class)
    public void testVotePartialTimeout()
            throws IOException, X509Exception, NoSuchAlgorithmException,
            InterruptedException {
        ImmutablePair<VotingClientChannel, Socket> pair
                = clientChannelConnectHdrAssert(
                PortAssignment.unique(), client1);

        VotingClientChannel votingClientChannel = pair.getLeft();
        Socket socket = pair.getRight();

        // Build a vote buf and send hdr len only.
        Vote v = new Vote(2222L, 3333L, votingClientChannel.mySid);
        ByteBuf votebuf = v.buildMsg();
        ByteBuf voteLenBuf = Unpooled.buffer(Integer.BYTES);
        voteLenBuf.writeInt(votebuf.readableBytes() - Integer.BYTES);
        writeBufToSocket(voteLenBuf, socket);

        // assert that votingClientChannel does timeout and disconnects
        socketIsClosed(socket, null);
    }

    /**
     * Test that server channel will also shutdown when sent partial hdr.
     * @throws IOException
     * @throws X509Exception
     * @throws NoSuchAlgorithmException
     * @throws InterruptedException
     * @throws ChannelException
     */
    @Test (timeout = 1000, expected = SocketException.class)
    public void testServerChannelHdrTimeout() throws IOException,
            X509Exception, NoSuchAlgorithmException, InterruptedException,
            ChannelException {
        listenServer = new QuorumServer(1,
                new InetSocketAddress("localhost", PortAssignment.unique()));
        ChannelFuture listenFuture;
        Socket socket;
        ByteBuf hdrBuf = buildHdr(client1.id(), client1.getElectionAddr());
        try {
            listenFuture = startListenAndConnectClient(listenServer);

            // Connect and write a hdr, check the return value
            socket = connectAndAssert(client1, listenServer);
            ByteBuf hdrPartial = Unpooled.buffer(hdrBuf.readableBytes() - 10);
            writeBufToSocket(hdrPartial, socket);
        } catch (IOException exp) {
            LOG.error("Invalid io exception: " + exp);
            throw new RuntimeException(exp);
        }

        socketIsClosed(socket, null);
        listenFuture.sync().channel().close().sync();
    }


    @Test (timeout = 1000, expected = SocketException.class)
    public void testChannelKeepAliveTimeout() throws IOException, X509Exception,
            NoSuchAlgorithmException, InterruptedException, ChannelException {
        ImmutableTriple<VotingClientChannel, Socket, Vote> triple
                = clientChannelConnectHdrAndVoteAssert(
                PortAssignment.unique(), client1, 50L, 3);

        final VotingClientChannel votingClientChannel = triple.getLeft();
        final Socket socket = triple.getMiddle();
        final Vote v = triple.getRight();
        final ByteBuf voteBuf = v.buildMsg();
        // Verify that the same vote is sent 3 times again.
        readSameVoteVerify(3, socket, v);

        // assert that votingClientChannel does timeout and disconnects
        // assert that votingClientChannel does timeout and disconnects
        while(votingClientChannel.getChannel().isActive()) {
            Thread.sleep(10);
        }

        socketIsClosed(socket, voteBuf);

        assertTrue("client channel closed",
                !votingClientChannel.getChannel().isActive());
    }

    @Test (timeout = 1000, expected = SocketException.class)
    public void testChannelKeepAliveDelayTimeout()
            throws IOException, X509Exception, NoSuchAlgorithmException,
            InterruptedException, ChannelException {
        ImmutableTriple<VotingClientChannel, Socket, Vote> triple
                = clientChannelConnectHdrAndVoteAssert(
                PortAssignment.unique(), client1, 50L, 3);

        final VotingClientChannel votingClientChannel = triple.getLeft();
        final Socket socket = triple.getMiddle();
        final Vote v = triple.getRight();
        final ByteBuf voteBuf = v.buildMsg();
        // Verify that the same vote is sent 2 times before sending reply.
        readSameVoteVerify(2, socket, v);

        // Build a vote buf and send it..
        final Vote replyVote = new Vote(2222L, 3333L,
                votingClientChannel.mySid);
        writeBufToSocket(replyVote.buildMsg(), socket);

        readSameVoteVerify(3, socket, v);

        // assert that votingClientChannel does timeout and disconnects
        while(votingClientChannel.getChannel().isActive()) {
            Thread.sleep(10);
        }

        socketIsClosed(socket, replyVote.buildMsg());

        assertTrue("client channel closed",
                !votingClientChannel.getChannel().isActive());
        votingClientChannel.close();
    }

    private void readSameVoteVerify(final int count, final Socket socket,
                              final Vote vote) throws IOException {
        // Verify that the same vote is sent 3 times before closing.
        for (int i = 0; i < count; i++) {
            // Verify that we read it.
            ByteBuf voteReadBuf = readHelper(socket,
                    vote.buildMsg().readableBytes());

            final Vote vRx = Vote.buildVote(voteReadBuf, vote.getSid());
            assertTrue("vote received - " + count, vRx.match(vote));
            LOG.info("verified vote count[max-" + count + "]: " + i);
        }
    }

    private ImmutableTriple<VotingClientChannel, Socket, Vote>
    clientChannelConnectHdrAndVoteAssert(final int listenPort,
                                         final QuorumServer from,
                                         final long keepAliveTimeoutMsec,
                                         final int keepAliveCount)
            throws IOException, X509Exception, NoSuchAlgorithmException,
            InterruptedException, ChannelException {
        ImmutablePair<VotingClientChannel, Socket> pair
                = clientChannelConnectHdrAssert(listenPort, from,
                keepAliveTimeoutMsec, keepAliveCount);

        VotingClientChannel votingClientChannel = pair.getLeft();
        Socket socket = pair.getRight();

        // Send the vote via the client channel
        Vote v = new Vote(1111L, 2222L, from.id());

        while(!votingClientChannel.sendVote(v)) {
            Thread.sleep(1);
        }

        // Verify that we read it.
        ByteBuf voteBuf = v.buildMsg();
        ByteBuf voteReadBuf = readHelper(socket, voteBuf.readableBytes());

        Vote vRx = Vote.buildVote(voteReadBuf, v.getSid());
        assertTrue("vote received", vRx.match(v));

        return ImmutableTriple.of(votingClientChannel, socket, v);
    }

    private ImmutableTriple<VotingClientChannel, Socket, Vote>
    clientChannelConnectHdrAndVoteAssert(final int listenPort,
                                         final QuorumServer from)
            throws IOException, X509Exception, NoSuchAlgorithmException,
            InterruptedException, ChannelException {
        return clientChannelConnectHdrAndVoteAssert(listenPort, from, 0L, 0);
    }

    private ImmutablePair<VotingClientChannel, Socket>
    clientChannelConnectHdrAssert(final int listenPort,
            final QuorumServer from, final long keepAliveTimeoutMsec,
                                  final int keepAliveCount)
            throws IOException, X509Exception, NoSuchAlgorithmException,
            InterruptedException {
        ImmutablePair<VotingClientChannel, Socket> pair =
                clientChannelConnect(listenPort, from,
                        keepAliveTimeoutMsec, keepAliveCount);
        VotingClientChannel votingClientChannel = pair.getLeft();
        Socket socket = pair.getRight();

        // Lets check if we got the right hdr.
        ByteBuf hdrExepcted = buildHdr(from.id(), from.getElectionAddr());
        ByteBuf hdrReceived = readHelper(socket,
                hdrExepcted.readableBytes());

        assertEquals(hdrExepcted, hdrReceived);
        return ImmutablePair.of(votingClientChannel, socket);
    }

    private ImmutablePair<VotingClientChannel, Socket>
    clientChannelConnectHdrAssert(final int listenPort,
                                  final QuorumServer from)
            throws IOException, X509Exception, NoSuchAlgorithmException,
            InterruptedException {
        return clientChannelConnectHdrAssert(listenPort, from, 0L, 0);
    }

    private ImmutablePair<VotingClientChannel, Socket> clientChannelConnect(
            final int listenPort, final QuorumServer from,
            final long keepAliveTimeoutMsec,
            final int keepAliveCount)
            throws IOException, X509Exception, NoSuchAlgorithmException,
            InterruptedException {
        listenServer
                = new QuorumServer(1,
                new InetSocketAddress("localhost", listenPort));
        serverSocket = newServerAndBindTest(listenServer, 0);
        final VotingClientChannel votingClientChannel
                = new VotingClientChannel(from.id(), from.getElectionAddr(),
                listenServer, READ_TIMEOUT_MS, CONNECT_TIMEOUT_MS,
                keepAliveTimeoutMsec, keepAliveCount, msgRxCb);

        try {
            startConnectionSync(listenServer, eventLoopGroup,
                    new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch)
                                throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            votingClientChannel.setChannel(ch);
                            p.addLast("dataRx", votingClientChannel);
                        }
                    }, CONNECT_TIMEOUT_MS - 2);
        } catch(InterruptedException exp) {
            LOG.error("unable to connect to server: "
                    + listenServer + " exp:" + exp);
            serverSocket.close();
            throw exp;
        }

        Socket socket = serverSocket.accept();
        socket.setTcpNoDelay(true);
        socket.setSoLinger(true, 0);
        return ImmutablePair.of(votingClientChannel, socket);
    }

    private ImmutablePair<VotingClientChannel, Socket> clientChannelConnect(
            final int listenPort, final QuorumServer from)
            throws IOException, X509Exception, NoSuchAlgorithmException,
            InterruptedException {
        return clientChannelConnect(listenPort, from, 0L, 0);
    }

    private ChannelFuture startListenAndConnectClient(
            final QuorumServer listenServer,
            final long keepAliveTimeoutMsec,
            final int keepAliveCount)
            throws InterruptedException, ChannelException{
        VotingServerChannel votingServerChannel =
                new VotingServerChannel(
                        listenServer.id(),listenServer.getElectionAddr(),
                        READ_TIMEOUT_MS, keepAliveTimeoutMsec,
                        keepAliveCount, sidLearnedCb, msgRxCb);

        // Lets listen.
        ChannelFuture listenFuture;
        try {
            listenFuture = startListener(listenServer.getElectionAddr(),
                    eventLoopGroup, votingServerChannel);
            listenFuture.sync();
        } catch(ChannelException exp) {
            LOG.error("unable to listen, exp:" + exp);
            throw exp;
        }

        while(!listenFuture.isDone()) {
            Thread.sleep(1);
        }

        assertTrue("listening", listenFuture.isSuccess());
        return listenFuture;
    }

    private ChannelFuture startListenAndConnectClient(
            final QuorumServer listenServer)
            throws InterruptedException, ChannelException {
        return startListenAndConnectClient(listenServer, 0L, 0);
    }
}
