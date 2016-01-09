package com.quorum.netty;

import com.common.X509Exception;
import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VotingChannelMgrTest extends BaseTest {
    private static final Logger LOG = LoggerFactory.getLogger
            (VotingChannelMgrTest.class);
    private final String type;
    private final Long readTimeoutMsec;
    private final Long connectTimeoutMsec;
    private final Long keepAliveTimeoutMsec;
    private final Integer keepAliveCount;
    private final Long sidStart;
    private final Long sidEnd;
    private final QuorumServer listenServer;
    private final QuorumServer client1;
    private Long sidRunning;
    private VotingChannelMgr votingChannelMgr;
    final long rgenseed = System.currentTimeMillis();
    Random random = new Random(rgenseed);

    private final ConcurrentLinkedQueue<Vote> inboundVoteQueue
            = new ConcurrentLinkedQueue<>();

    private final class MsgRxCb implements Callback<Vote> {
        @Override
        public void call(final Vote o) throws ChannelException, IOException {
            inboundVoteQueue.add(o);
        }
    }

    private final MsgRxCb msgRxCb = new MsgRxCb();

    @Parameterized.Parameters
    public static Collection quorumServerConfigs() {
        return Arrays.asList( new Object [][] {
                // SSL, ListenAddr, ReadTimeout, ConnectTimeout,
                // KeepAliveTimeout, KeepAliveCount, StartSid,
                // EndSid
                //{ "plain", 15555, 0L, 0L, 0L, 0, 1L, 99L},
                //{ "ssl", 25555, 0L, 0L, 0L, 0, 100L, 199L},
                { "plain", 5555, 100, 10, 100L, 3, 200L, 299L},
                { "ssl", 25555, 100L, 10L, 100L, 3, 300L, 399L},
    });}

    public VotingChannelMgrTest(final String type, final int listenAddr,
                                final long readTimeoutMsec,
                                final long connectTimeoutMsec,
                                final long keepAliveTimeoutMsec,
                                final int keepAliveCount,
                                final long sidStart, final long sidEnd) {
        this.type = type;
        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
        this.sidStart = sidStart;
        this.sidRunning = this.sidStart;
        this.sidEnd = sidEnd;

        listenServer = new QuorumServer(this.sidRunning++,
                new InetSocketAddress("localhost",
                        listenAddr + random.nextInt(15000)));
        client1 = new QuorumServer(this.sidRunning++,
                new InetSocketAddress(
                        "localhost", listenAddr +random.nextInt(15000)));
    }

    @Before
    public void setup() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(1, executor);
        LOG.info("Setup type: " + type);
        ClassLoader cl = getClass().getClassLoader();
        if (type.equals("plain")) {
            sslEnabled = false;
        } else if (type.equals("ssl")) {
            sslEnabled = true;
        } else {
            throw new IllegalArgumentException("type is invalid:" + type);
        }

        votingChannelMgr = new VotingChannelMgr(listenServer.id(),
                listenServer.getElectionAddr(), readTimeoutMsec,
                connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                eventLoopGroup, sslEnabled, msgRxCb,
                cl.getResource(keyStore.get(0)).getFile(),
                keyPassword.get(0),
                cl.getResource(trustStore.get(0)).getFile(),
                trustPassword.get(0),
                trustStoreCAAlias);
    }

    @After
    public void tearDown() throws Exception {
        if (votingChannelMgr != null) {
            LOG.info("Shutting voting channel mgr");
            votingChannelMgr.shutdown();
        }
        LOG.info("Shutting down eventLoopGroup");
        eventLoopGroup.shutdownGracefully().sync();
    }

    @Test (timeout = 2000)
    public void testListener() throws IOException, NoSuchAlgorithmException,
            X509Exception, CertificateException, ChannelException,
            InterruptedException {
        mgrInit();
        close(connectAndAssert(client1, listenServer));
    }

    @Test (timeout = 1000, expected = SocketException.class)
    public void testIncomingInvalidServer() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        mgrInit();

        Socket clientSocket = connectAndAssert(client1, listenServer);
        writeBufToSocket(buildHdr(client1.id(), client1.getElectionAddr()), clientSocket);

        socketIsClosed(clientSocket);
    }

    @Test (timeout = 1000)
    public void oneServerDuplex() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        socketPairClose(oneServerDuplexHelper(client1, 1));
    }

    @Test (expected = SocketException.class)
    public void oneServerDeInit() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        ImmutablePair<ServerSocket, Socket> socketPair =
                oneServerDuplexHelper(client1, 1);

        // Remove the server
        votingChannelMgr.removeServer(client1);

        // Verify that the above triggers a disconnect
        socketIsClosed(socketPair.getRight());

        socketPairClose(socketPair);
    }

    @Test (timeout = 1000)
    public void testOneServerSidResolveAndVote() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        ImmutablePair<ServerSocket, Socket> socketPair =
                oneServerDuplexHelper(client1, 1);

        // Connect and send the hdr
        Socket clientSocket = connectAndAssert(client1, listenServer);

        // Send the hdr
        writeBufToSocket(buildHdr(client1.id(), client1.getElectionAddr()), clientSocket);

        boolean closedCheck = false;
        // Ensure the previous incoming channel is closed.
        try {
            socketIsClosed(socketPair.getRight());
        } catch (SocketException exp) {
            closedCheck = true;
        }

        assertTrue("closed check", closedCheck);

        socketPairClose(socketPair);

        // Send a Vote via this channel and verify it received.
        assertEquals(inboundVoteQueue.size(), 0);
        sendVerifyVote(123L, 456L, client1.id(), clientSocket, inboundVoteQueue);

        close(clientSocket);
    }

    @Test (timeout = 1000)
    public void testVoteRx() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        socketPairClose(oneVoteRxHelper(client1, 1,
                new Vote(123L, 456L, listenServer.id())));
    }

    @Test (timeout = 1000)
    public void testSameVoteRxFail() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        Vote vote = new Vote(123L, 456L, listenServer.id());
        ImmutablePair<ServerSocket, Socket> socketPair
                = oneVoteRxHelper(client1, 1, vote);

        // Send it again.
        votingChannelMgr.sendVote(vote);

        // Send loop runs ever millisec, 50msec is plenty of time.
        Socket socket = socketPair.getRight();
        socket.setSoTimeout(50);

        final String expectedStr = "Read timed out";
        String expStr = "";
        try {
            readHelper(socketPair.getRight(),
                    vote.buildMsg().readableBytes());
        } catch (SocketTimeoutException exp) {
            expStr = exp.getMessage();
        }

        assertEquals(expectedStr, expStr);

        socketPairClose(socketPair);
    }

    @Test (timeout = 1000)
    public void testVoteRxOnChannelReconnect() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        Vote vote = new Vote(123L, 456L, listenServer.id());
        ImmutablePair<ServerSocket, Socket> socketPair
                = oneVoteRxHelper(client1, 1, vote);

        // Close the socket on our end.
        socketPair.getRight().close();

        // Verify re-connect
        Socket socket = socketPair.getLeft().accept();
        setSockOptions(socket);

        assertTrue("re-connected", socket.isConnected());

        // Go past header Rx
        hdrRxVerify(listenServer, socket);

        // Verify vote Rx
        voteRxVerify(listenServer, vote, socket);

        socket.close();
        socketPair.getLeft().close();
    }

    private ImmutablePair<ServerSocket, Socket> oneVoteRxHelper(
            final QuorumServer server, int index,
            final Vote vote) throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        ImmutablePair<ServerSocket, Socket> socketPair =
                oneServerDuplexHelper(server, index);

        // Send Vote via Mgr and Rx it.
        votingChannelMgr.sendVote(vote);

        voteRxVerify(listenServer, vote, socketPair.getRight());
        return socketPair;
    }

    private ImmutablePair<ServerSocket, Socket> oneServerDuplexHelper(
            final QuorumServer server, int index) throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException {
        mgrInit();

        // Add the server to mgr
        votingChannelMgr.addServer(server);

        return startListenReadHdr(server, index);
    }

    private ImmutablePair<ServerSocket, Socket> startListenReadHdr(
            final QuorumServer server, int index) throws X509Exception,
            IOException, NoSuchAlgorithmException {
        // Lets start the server
        ServerSocket serverSocket = newServerAndBindTest(server, index);

        // We should get a connect
        Socket socket = serverSocket.accept();
        setSockOptions(socket);

        hdrRxVerify(listenServer, socket);

        return ImmutablePair.of(serverSocket, socket);
    }

    private void voteRxVerify(final QuorumServer from, final Vote vote,
                              final Socket socket) throws IOException {
        ByteBuf voteBufExpected = vote.buildMsg();
        ByteBuf voteBufRead = readHelper(socket,
                voteBufExpected.readableBytes());
        Vote voteRx = Vote.buildVote(voteBufRead);
        assertTrue("vote match", vote.match(voteRx));
    }

    private void hdrRxVerify(final QuorumServer from, Socket socket)
            throws IOException {
        // Go past header Rx
        ByteBuf hdrTx = buildHdr(from.id(), from.getElectionAddr());
        ByteBuf hdrRx = readHelper(socket, hdrTx.readableBytes());

        assertTrue("hdr check", hdrEquals(from.id(), from.getElectionAddr(), hdrRx));
    }

    private void mgrInit() throws IOException, NoSuchAlgorithmException,
            X509Exception, CertificateException, ChannelException,
            InterruptedException {
        votingChannelMgr.start();
        votingChannelMgr.waitForListener();
    }

    private void socketPairClose(ImmutablePair<ServerSocket, Socket>
                                 socketPair) {
        close(socketPair.getRight());
        close(socketPair.getLeft());
    }
}
