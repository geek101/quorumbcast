package com.quorum.netty;

import com.common.X509Exception;
import com.quorum.QuorumServer;
import com.quorum.Vote;

import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;

/**
 * Broadcaster of Vote used by FLE.
 */
public class QuorumBroadcast implements com.quorum.QuorumBroadcast {
    private static final Logger LOG =
            LoggerFactory.getLogger(QuorumBroadcast.class);
    private final Long myId;
    private final Map<Long, QuorumServer> serverMap = new HashMap<>();
    private final InetSocketAddress electionAddr;
    private final EventLoopGroup eventLoopGroup;
    private final Long readMsgTimeoutMs;
    private final Long connectTimeoutMs;
    private final Long keepAliveTimeoutMs;
    private final Integer keepAliveCount;
    private final boolean sslEnabled;
    private VotingChannelMgr channelMgr;
    private boolean isShutdown = true;

    public QuorumBroadcast(Long myId, List<QuorumServer> quorumServerList,
                           final InetSocketAddress electionAddr,
                           EventLoopGroup eventLoopGroup,
                           final long readMsgTimeoutMs,
                           final long connectTimeoutMs,
                           final long keepAliveTimeoutMs,
                           final int keepAliveCount,
                           final boolean sslEnabled)
            throws ChannelException, IOException {
        this.myId = myId;
        this.electionAddr = electionAddr;
        for (QuorumServer server : quorumServerList) {
            if (server.id() != myId) {
                addServerImpl(server);
            }
        }
        this.eventLoopGroup = eventLoopGroup;
        this.readMsgTimeoutMs = readMsgTimeoutMs;
        this.connectTimeoutMs = connectTimeoutMs;
        this.keepAliveTimeoutMs = keepAliveTimeoutMs;
        this.keepAliveCount = keepAliveCount;
        this.sslEnabled = sslEnabled;
    }

    public long sid() {
        return myId;
    }

    public void start(final Callback<Vote> msgRxCb) throws ChannelException,
            SSLException, CertificateException, NoSuchAlgorithmException,
            X509Exception.KeyManagerException,
            X509Exception.TrustManagerException {
        synchronized (this) {
            if (isShutdown) {
                channelMgr = new VotingChannelMgr(myId, electionAddr,
                        readMsgTimeoutMs, connectTimeoutMs, keepAliveTimeoutMs,
                        keepAliveCount, eventLoopGroup, sslEnabled, msgRxCb);
                startInternal();
                isShutdown = false;
            }
        }
    }

    public void start(final Callback<Vote> msgRxCb,
                      final String keyStoreLocation,
                      final String keyStorePassword,
                      final String trustStoreLocation,
                      final String trustStorePassword,
                      final String trustStoreCAAlias)
            throws ChannelException, SSLException, CertificateException,
            NoSuchAlgorithmException, X509Exception.KeyManagerException,
            X509Exception.TrustManagerException {
        if (!sslEnabled) {
            start(msgRxCb);
            return;
        }

        synchronized (this) {
            if (isShutdown) {
                channelMgr = new VotingChannelMgr(myId, electionAddr,
                        readMsgTimeoutMs, connectTimeoutMs,
                        keepAliveTimeoutMs, keepAliveCount, eventLoopGroup,
                        true, msgRxCb, keyStoreLocation, keyStorePassword,
                        trustStoreLocation, trustStorePassword,
                        trustStoreCAAlias);
                startInternal();
                isShutdown = false;
            }
        }
    }

    public void shutdown() {
        synchronized (this) {
            if (!isShutdown) {
                channelMgr.shutdown();
                isShutdown = true;
            }
        }
    }

    /**
     * API to broadcast the given vote.
     * This is not a queue service. If a previous outstanding
     * vote (i.e vote could not be sent etc) exists it will
     * not be sent any more and will be replaced by the new vote.
     * @param vote what to be sent
     */
    public void broadcast(Vote vote) {
        synchronized (this) {
            if (!isShutdown) {
                channelMgr.sendVote(vote);
            }
        }
    }

    /**
     * Get the list of server sids.
     * @return Immutable Collection of Sids. Null when nothing to return.
     */
    public Collection<Long> serverSidList() {
        if (serverMap.keySet().size() > 0) {
            return Collections.unmodifiableCollection(serverMap.keySet());
        }
        return null;
    }

    /**
     * Used by constructor, fails if collision is found.
     * @param server quorum server
     * @throws ChannelException
     */
    public void addServer(final QuorumServer server)
            throws ChannelException {
        addServerImpl(server);
        channelMgr.addServer(server);
    }

    public void removeServer(final QuorumServer server)
            throws ChannelException {
        channelMgr.removeServer(server);
        serverMap.remove(server.id());
    }

    private void startInternal() throws ChannelException, SSLException,
            CertificateException {
        channelMgr.start();
        initServers();
    }

    private void initServers() throws ChannelException {
        for (QuorumServer server : serverMap.values()) {
            channelMgr.addServer(server);
        }
    }

    private void addServerImpl(final QuorumServer server)
            throws ChannelException {
        if (serverMap.containsKey(server.id())) {
            throw new ChannelException("Invalid config, server key:"
                    + server.id() + "collision: server1: "
                    + server.toString() + " server2: " + serverMap.get
                    (server.id()));
        }
        serverMap.put(server.id(), server);
    }

    public void runNow() {
        // Used for nio tester to work.
    }
}
