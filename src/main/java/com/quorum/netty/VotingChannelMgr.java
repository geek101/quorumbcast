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

import com.quorum.AbstractServer;
import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.util.Callback;
import com.quorum.util.Callback2;
import com.quorum.util.ChannelException;
import com.quorum.util.ErrCallback;
import com.quorum.util.NotNull;
import com.quorum.util.ZKTimerTask;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static com.common.X509Exception.KeyManagerException;
import static com.common.X509Exception.TrustManagerException;
import static com.common.X509Util.SSL_VERSION;

public class VotingChannelMgr extends NettyChannelMgr {
    private static final Logger LOG
            = LoggerFactory.getLogger(VotingChannelMgr.class);
    private static final long TIMEOUT_MSEC = 1;
    private final Long mySid;
    private final InetSocketAddress electionAddr;
    private final Long readMsgTimeoutMs;
    private final Long connectTimeoutMs;
    private final Long keepAliveTimeoutMs;
    private final Integer keepAliveCount;
    private final EventLoopGroup group;
    private final Callback<Vote> msgRxCb;
    private final SidLearnedCb sidLearnedCb;
    private final VotingChannelMgr self;
    private ZKTimerTask<Void> timerTask;

    private boolean started = false;
    private Vote currentVote = null;

    // Object for the concurrent hashmap.
    private class ChannelHolder {
        private final QuorumServer server;
        private VotingChannel votingChannel = null;
        private boolean removed = false;

        public ChannelHolder(QuorumServer server) {
            this.server = server;
        }

        public VotingChannel putChannel(VotingChannel votingChannel) {
            VotingChannel qc = this.votingChannel;
            this.votingChannel = votingChannel;
            return qc;
        }

        public final VotingChannel getQuorumChannel() {
            return this.votingChannel;
        }

        public void markToRemove() {
            this.removed = true;
        }

        public final QuorumServer getServer() {
            return this.server;
        }

        public boolean isRemoved() {
            return this.removed;
        }
    }

    private final ConcurrentHashMap<Long, ChannelHolder> channelMap
            = new ConcurrentHashMap<>();

    private final class SidLearnedCb implements Callback2 {
        @Override
        public void done(Object o, Object p) throws ChannelException {
            self.sidLearned((Long) o, (VotingChannel) p);
        }
    }

    private final class ErrorCb implements ErrCallback {
        @Override
        public void caughtException(Exception exp) throws Exception {
            LOG.error("Cannot have exceptions here: {}", exp);
            throw exp;
        }
    }

    public VotingChannelMgr(final long mySid,
                            final InetSocketAddress electionAddr,
                            final long readMsgTimeoutMs,
                            final long connectTimeoutMs,
                            final long keepAliveTimeoutMs,
                            final int keepAliveCount,
                            final EventLoopGroup group, boolean sslEnabled,
                            final Callback<Vote> msgRxCb)
            throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        super(group, sslEnabled);
        this.mySid = mySid;
        this.electionAddr = electionAddr;
        this.readMsgTimeoutMs = readMsgTimeoutMs;
        this.connectTimeoutMs = connectTimeoutMs;
        this.keepAliveTimeoutMs = keepAliveTimeoutMs;
        this.keepAliveCount = keepAliveCount;
        this.group = group;
        this.msgRxCb = msgRxCb;
        this.sidLearnedCb = new SidLearnedCb();
        this.self = this;

    }

    public VotingChannelMgr(final long mySid,
                            final InetSocketAddress electionAddr,
                            final long readMsgTimeoutMs,
                            final long connectTimeoutMs,
                            final long keepAliveTimeoutMs,
                            final int keepAliveCount,
                            final EventLoopGroup group,
                            boolean sslEnabled,
                            final Callback<Vote> msgRxCb,
                            final String keyStoreLocation,
                            final String keyStorePassword,
                            final String trustStoreLocation,
                            final String trustStorePassword,
                            final String trustStoreCAAlias)
            throws NoSuchAlgorithmException, KeyManagerException,
            TrustManagerException {
        super(group, sslEnabled, SSL_VERSION, keyStoreLocation,
                keyStorePassword, trustStoreLocation, trustStorePassword,
                trustStoreCAAlias);
        this.mySid = mySid;
        this.electionAddr = electionAddr;
        this.readMsgTimeoutMs = readMsgTimeoutMs;
        this.connectTimeoutMs = connectTimeoutMs;
        this.keepAliveTimeoutMs = keepAliveTimeoutMs;
        this.keepAliveCount = keepAliveCount;
        this.group = group;
        this.msgRxCb = msgRxCb;
        this.sidLearnedCb = new SidLearnedCb();
        this.self = this;
    }

    /**
     * API to start the listener and runner to manage connections to added
     * servers.Also takes care of removing the deleted servers and stopping
     * connections to them.
     * @throws ChannelException
     * @throws SSLException
     * @throws CertificateException
     */
    public void start() throws ChannelException, SSLException,
            CertificateException {
        synchronized (this) {
            if (!started) {
                started = true;
                startListener();
                timerTask = new ZKTimerTask<Void>(group, TIMEOUT_MSEC,
                        new ErrorCb()) {
                    @Override
                    public Void doRun() throws Exception {
                        try {
                            self.doRun();
                        } catch (Exception exp) {
                            LOG.error("{}", exp);
                            throw exp;
                        }
                        return null;
                    }
                };
                timerTask.start();
            }
        }
    }

    /**
     * API to shutdown
     */
    public void shutdown() {
        synchronized (this) {
            if (started) {
                super.shutdown();
                timerTask.stop();
                timerTask = null;
                removeAll();
                started = false;
            }
        }
    }

    /**
     * Starts the listener, called from start. Override if one only wants
     * outbound channels.
     *
     * @throws ChannelException
     * @throws SSLException
     * @throws CertificateException
     */
    protected void startListener()
            throws ChannelException, SSLException, CertificateException {
        super.startListener(electionAddr);
    }


    /**
     * API to add a server to start the client side connection to.
     * Based on sid of this server an accepted connection might be the only
     * one used.
     *
     * @param server
     * @throws ChannelException
     */
    public void addServer(final QuorumServer server)
            throws ChannelException {
        NotNull.check(server, "Cannot accept null for QuorumServer", LOG);
        synchronized (this) {
            if (getServer(server.id()) != null) {
                throw new ChannelException("Server: " + server
                        + " already exists.");
            }

            channelMap.put(server.id(), new ChannelHolder(server));
        }
    }

    /**
     * API to remove a server which mean stop the connections to it.
     *
     * @param server
     * @throws ChannelException
     */
    public void removeServer(final QuorumServer server)
            throws ChannelException {
        NotNull.check(server, "Cannot accept null for QuorumServer", LOG);
        synchronized (this) {
            if (getServer(server.id()) == null) {
                LOG.warn("Server: " + server + " does not exists, cannot remove");
                return;
            }

            channelMap.get(server.id()).markToRemove();
        }
    }

    /**
     * Send this vote to all servers.
     * We optimistically trigger a task on the executor to send
     * the current vote to everyone.
     * @param vote
     */
    public void sendVote(final Vote vote) {
        NotNull.check(vote, "Cannot accept null for Vote", LOG);
        this.currentVote = vote;
        sendVoteAsync();
    }

    /**
     * Helper for shutdown
     */
    private void removeAll() {
        for (ChannelHolder holder : channelMap.values()) {
            deInitServerSafe(holder.getServer());
        }
    }

    private boolean serverExists(final long sid) {
        return channelMap.containsKey(sid);
    }

    private boolean serverExists(final AbstractServer server) {
        return serverExists(((QuorumServer) server).id());
    }

    @Override
    protected void startConnection(final AbstractServer server) {
        QuorumServer quorumServer = (QuorumServer) server;
        long sid = quorumServer.id();
        if (!serverExists(sid)) {
            LOG.info("Server for sid: " + sid + " no longer exists" + "" +
                    "server: " + quorumServer);
            return;
        }

        /**
         * TODO: make this configurable so unit test don't flunk!.
        if (mySid < quorumServer.id()) {
            LOG.info("our sid is lower , cannot connect - from:" + mySid
                    + "- to:" + quorumServer.id());
            return;
        }
        */
        super.startConnection(server);
    }

    @Override
    protected NettyChannel newAcceptHandler() {
        return new VotingServerChannel(mySid, electionAddr,
                readMsgTimeoutMs, keepAliveTimeoutMs,
                keepAliveCount, sidLearnedCb, msgRxCb);
    }

    @Override
    protected NettyChannel newClientHandler(final AbstractServer server) {
        return new VotingClientChannel(mySid, electionAddr,
                (QuorumServer) server, readMsgTimeoutMs,
                connectTimeoutMs, keepAliveTimeoutMs, keepAliveCount, msgRxCb);
    }

    /**
     * If caller wants to do something about connect success/failures.
     *
     * @param server
     * @param handler
     * @param success
     */
    @Override
    protected void connectHandler(AbstractServer server, NettyChannel handler,
                                  boolean success) {
        QuorumServer quorumServer = (QuorumServer) server;
        if (!serverExists(server)) {
            LOG.warn("Connect succeeded to non existing server : " +
                    quorumServer
                    + ". Closing the channel.");
            if (handler != null) {
                handler.close();
            }
            return;
        }

        if (success) {
            NotNull.check(handler, "handler cannot be null on success", LOG);
            // if there doesn't exist a connection already then store this
            // else close.
            if (getQuorumChannel(quorumServer.id()) == null) {
                LOG.debug("Channel connected handler: " + handler);
                channelMap.get(quorumServer.id()).putChannel(
                        (VotingChannel) handler);
            } else {
                LOG.warn("Channel already exists - closing current handler" +
                        handler);
                handler.close();
            }
        }
    }

    @Override
    protected void acceptHandler(NettyChannel handler) {
        // nothing to do till we learn sid
        LOG.debug("Accepted channel");
    }

    /**
     * Handles channel disconnect for various cases.
     * case 1:
     * Incoming channel before sid is learned. Do nothing
     *
     * case 2:
     * Disconnect for a removed channel. Send vote set to Remove
     *
     * case 3:
     * Channel disconnected. Send vote set to Remove
     *
     * case 4:
     * Channel replaced with an incoming channel due to sid resolution.
     * @param handler
     */
    @Override
    protected void closedHandler(NettyChannel handler) {
        // case 1
        if (handler.key() == null) {
            // Unknown channel let it go.
            return;
        }

        // case 2
        long sid = (long) handler.key();
        if (getServer(sid) == null) {
            // Probably this is the remove iteration.
            // Send remove vote.
            postRemoveVote(sid);
            return;
        }

        // case 3
        // lets check if this is the channel that won.
        if (getQuorumChannel(sid) == handler) {
            // ok this is the same channel as before,
            // it is a genuine disconnect so mark it null and
            // re-connect will kick in.
            channelMap.get(sid).putChannel(null);
            postRemoveVote(sid);
        }

        // case 4
    }

    private void postRemoveVote(final long sid) {
        Vote vote = Vote.createRemoveVote(sid);
        vote.setRemove();
        try {
            msgRxCb.call(vote);
        } catch (ChannelException | IOException exp) {}
    }

    private QuorumServer getServer(Long sid) {
        if (!channelMap.containsKey(sid)) {
            return null;
        }

        return channelMap.get(sid).getServer();
    }

    private VotingChannel getQuorumChannel(Long sid) {
        if (!channelMap.containsKey(sid)) {
            return null;
        }

        return channelMap.get(sid).getQuorumChannel();
    }

    private void initServer(final QuorumServer server)
            throws ChannelException {
        // start the connection.
        startConnection(server);
    }

    private void deInitServer(final QuorumServer server)
            throws ChannelException {
        VotingChannel votingChannel = getQuorumChannel(server.id());
        if (votingChannel != null) {
            votingChannel.close();
        }
        channelMap.remove(server.id());
    }

    /**
     * Called from server channel handler when sid is learned.
     *
     * @param sid
     * @param votingChannel
     */
    private void sidLearned(Long sid, VotingChannel votingChannel) {
        QuorumServer quorumServer = getServer(sid);
        if (quorumServer == null) {
            LOG.error("Closing invalid incoming connection sid - " + sid
                    + " is unknown from channel: "
                    + votingChannel.getChannelStr());

            votingChannel.close();
            return;
        }

        // if sid is learned which means that channel resolved the sid and
        // survived hence use this channel.
        VotingChannel quorumChannelCurrent
                = channelMap.get(quorumServer.id()).putChannel(votingChannel);
        if (quorumChannelCurrent != null) {
            // ok we need close this.
            LOG.info("Closing channel: "
                    + quorumChannelCurrent.getChannelStr());
            quorumChannelCurrent.close();
        }
    }

    private Future<Void> sendVoteAsync() {
        FutureTask<Void> futureTask = new FutureTask<>(
                new Callable<Void>() {
                    @Override
                    public Void call() throws ChannelException {
                        sendVoteToAll();
                        return null;
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    private void sendVoteToAll() {
        // If there exists a message to send then send it.
        Vote vote = currentVote;
        if (vote == null) {
            return;
        }

        for (ChannelHolder channelHolder : channelMap.values()) {
            if (!channelHolder.isRemoved() &&
                    channelHolder.getQuorumChannel() != null) {
                sendVoteToServer(channelHolder.getQuorumChannel(),
                        channelHolder.getServer(), vote);
            }
        }
    }

    private void sendVoteToServer(final VotingChannel channel, final
                                  QuorumServer server, final Vote vote) {
        try {
            sendVote(channel, vote);
        } catch (ChannelException exp) {
            LOG.warn("Unable to send msg for server: "
                    + server + " with version: "
                    + vote.getVersion() + " exp: " + exp);
            // reset the connection.
            channel.close();
        }
    }

    private boolean sendVote(VotingChannel votingChannel, Vote vote)
            throws ChannelException {
        return votingChannel.sendVote(vote);
    }

    private void deInitServerSafe(final QuorumServer server) {
        try {
            deInitServer(server);
        } catch (ChannelException exp) {
            LOG.error("Error trying to remove server: " + server
                    + " exception: " + exp);
        } finally {
            channelMap.remove(server.id());
        }
    }

    private Collection<QuorumServer> sweepRemove() {
        Collection<QuorumServer> retList = new ArrayList<>();
        for (ChannelHolder channelHolder : channelMap.values()) {
            QuorumServer server = channelHolder.getServer();
            if (channelHolder.isRemoved()) {
                retList.add(server);
            }
        }
        return Collections.unmodifiableCollection(retList);
    }

    private void removeServers() {
        for (final QuorumServer server : sweepRemove()) {
            deInitServerSafe(server);
        }
    }

    private void initServers() {
        for (ChannelHolder channelHolder : channelMap.values()) {
            if (channelHolder.getQuorumChannel() == null) {
                // Try to connect as long as the server is added.
                try {
                    initServer(channelHolder.getServer());
                } catch (ChannelException exp) {
                    LOG.error("Error trying to add server: " +
                            channelHolder.getServer() + " exception: " + exp);
                }
            }
        }
    }

    /**
     * Main loop that runs as part of EventLoopGroup's executor.
     * Either stops connection and removes the server or starts a connection.
     */
    private void doRun() throws Exception {
        // Sweep for marked servers and remove them.
        removeServers();

        // Add servers
        initServers();

        // Try sending current vote for re-connected and new channels
        sendVoteToAll();
    }

    protected void submitTask(FutureTask task) {
        group.submit(task);
    }
}
