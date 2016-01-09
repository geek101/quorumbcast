/**
 *  Licensed to the Apache Software Foundation (ASF) under one
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
import com.common.X509Exception;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import com.quorum.util.ZKTimerTask;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;

/**
 * This class abstracts the idea of broadcasting a given message
 * (Vote in this case to an existing Quorum of servers. Hence the upper
 * layers does not need to worry about which servers are in the Quorum and
 * how to replicate message to each of them. This class does provide a way to
 * dynamically add or remove new servers to/from the Quorum without
 * disrupting the existing connections to each of them. And the added new
 * servers will automatically be broad-casted with the message sent to the past
 * servers.
 * This class also provides reliability i.e it guarantees that a message
 * is sent to the server and will retry for ever to try to send if it failed
 * to do so.
 *
 * All the above done in non-blocking mode with multiplexing
 * to multiple servers and there is only one connection given a pair of the
 * servers.
 *
 * This class does not provide message queue service to the Quorum.
 * It broadcasts a given message and if this message was not sent due to
 * servers in the Quorum being disconnected etc and if caller want
 * to broadcast a new message the new message will be sent instead of the
 * previous one(s) when channels gets connected.
 *
 * QuorumBroadcast and QuorumChannels are designed to work in a serial fashion
 * hence all the code should be run as part of single thread. There is no
 * need for multi-threading for QuorumServer connections since the set is
 * quite small.
 * Created by powell on 11/8/15.
 */

public class QuorumBroadcast extends QuorumBroadcastImpl
        implements Runnable {
    private static final Logger LOG =
            LoggerFactory.getLogger(QuorumBroadcast.class);
    private static final long TIMEOUT_MSEC = 1;  /// Timeout in ms.
    private final EventLoopGroup group;
    private final QuorumBroadcast self;
    private Callback<Vote> msgRxCb;
    private SelectorDispatch selectorDispatch;
    private Map<Long, VotingChannel> channelMap = new HashMap<>();
    private List<VotingChannel> acceptedChannels = new LinkedList<>();
    private ZKTimerTask<Void> timerTask;

    /**
     * Accept ready socket com.quorum.util.Callback
     */
    private class AcceptCb implements Callback<SocketChannel> {
        private final QuorumBroadcast qb;
        public AcceptCb(QuorumBroadcast qb) {
            this.qb = qb;
        }

        public void call(final SocketChannel o)
                throws ChannelException, IOException {
            this.qb.acceptHandler(o);
        }
    }

    /**
     * Initialize QuorumBcast with a given list of servers.
     * @param quorumServerList
     */
    public QuorumBroadcast(Long myId, List<QuorumServer> quorumServerList,
                           InetSocketAddress electionAddr,
                           EventLoopGroup eventLoopGroup)
            throws ChannelException, IOException {
        super(myId, quorumServerList, electionAddr);
        this.group = eventLoopGroup;
        this.self = this;
    }

    public void start(final Callback<Vote> msgRxCb,
                      String arg1, String arg2, String arg3,
                      String arg4, String arg5)
            throws IOException, ChannelException,
            CertificateException, NoSuchAlgorithmException,
            X509Exception.KeyManagerException,
            X509Exception.TrustManagerException{
        this.msgRxCb = msgRxCb;
        this.selectorDispatch = new SelectorDispatch(
                getElectionAddr(), TIMEOUT_MSEC);
        this.selectorDispatch.registerAccept(new AcceptCb(this));
        initServers();
        timerTask = new ZKTimerTask<Void>(group, TIMEOUT_MSEC, null) {
            @Override
            public Void doRun() throws ChannelException, IOException {
                self.runNow();
                return null;
            }
        };
        timerTask.start();
    }

    public void shutdown() {
        closeAll();
    }

    public boolean isClosed() {
        return selectorDispatch == null;
    }

    /**
     * Main run method for this class when used this class with a thread.
     */
    @Override
    public void run() {
        try {
            selectorRun();
            loop();
        } catch (ChannelException | IOException exp) {
            LOG.error("Run exp: " + exp);
            shutdown();
        }
    }

    /**
     * API to run in poll mode, remember selector blocks for a given timeout.
     * @throws Exception
     */
    public void runNow() {
        try {
            selectorRunNow();
            loop();
        } catch (ChannelException | IOException exp) {
            LOG.error("Run exp: " + exp);
            shutdown();
        }
    }

    /**
     * Used by constructor so fails if collision is found.
     * @param server
     * @throws IOException
     * @throws ChannelException
     */
    public void addServer(final QuorumServer server)
            throws ChannelException {
        addServerImpl(server);
        initServer(server);
    }

    public void removeServer(final QuorumServer server)
            throws ChannelException {
    }

    private void initServers() throws ChannelException {
        for (QuorumServer server : serverMap.values()) {
            initServer(server);
        }
    }
    private void initServer(final QuorumServer server)
            throws ChannelException {
        if (channelMap.get(server.id()) != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Invalid input server collision: ")
                    .append(server);
            LOG.error(sb.toString());
            throw new ChannelException(sb.toString());
        }

        // Create an new VotingChannel and store it, if on error
        // store a null pointer so next attempt may work.
        VotingChannel qc = null;
        try {
            qc = new VotingChannel(sid(),
                    getElectionAddr(), server, selectorDispatch);
        } catch (IOException exp) {
            LOG.warn("Channel to : " + server + " Error: " + exp);
            qc = null;
        } finally {
            channelMap.put(server.id(), qc);
        }
    }

    /**
     * Helper to store the in-bound message from a given channel,
     * @param ch
     * @param msg
     */
    private void putInboundMsg(VotingChannel ch,
                               final Vote msg)
            throws ChannelException, IOException{
        if (msg != null) {
            msgRxCb.call(msg);
        }
    }

    /**
     * Used by selector to help us handle incoming connections.
     * @param o SocketChannel
     */
    private void acceptHandler(Object o)
            throws ChannelException, IOException {
        acceptedChannels.add(new VotingChannel(sid(), getElectionAddr(),
                (SocketChannel) o, selectorDispatch));
    }

    /**
     * step 1: Look at the accepted connections and move known connections to
     *         serverStateMap.
     * @return IOException
     */
    private void reapAcceptedChannels() throws IOException {
        for (Iterator<VotingChannel> it = acceptedChannels.iterator();
             it.hasNext();) {
            VotingChannel qc = it.next();
            if (qc.closed()) {
                it.remove();
            } else {
                if (qc.getKey() != Long.MIN_VALUE) {
                    VotingChannel oldQc = channelMap.put(qc.getKey(), qc);
                    // Nuke the previous channel
                    if (oldQc != null) {
                        oldQc.close();
                    }
                    it.remove();
                }
            }
        }
    }

    /**
     * step 2: Walk serverStateMap and remove connections that are in ERROR.
     * step 3: Walk serverStateMap and spawn new QuorumChannels for null
     *         entries.
     * @throws IOException
     */
    private void processChannels() throws IOException {
        for (Iterator<Map.Entry<Long, VotingChannel>> it
             = channelMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, VotingChannel> entry = it.next();
            final VotingChannel qc = entry.getValue();
            final long serverSid = entry.getKey();
            if (qc == null || qc.closed()) {
                if (serverMap.get(serverSid) != null) {
                    VotingChannel newQc = null;
                    try {
                        newQc = new VotingChannel(sid(),
                                getElectionAddr(), serverMap.get(serverSid),
                                selectorDispatch);
                    } catch (ChannelException | IOException exp) {
                        LOG.error("Channel for server: " +
                                serverMap.get(serverSid) + " Error: " + exp);
                        newQc = null;
                    } finally {
                        entry.setValue(newQc);
                    }
                } else {   // No server id do not reconnect.
                    it.remove();
                }
            }
        }
    }

    /**
     * step 4: Get current msg or last msg and send it to all channels and
     *         get in-bound msg from each channel if it exists.
     */
    private void sendMsg() throws ChannelException, IOException{
        for (Map.Entry<Long, VotingChannel> entry : channelMap.entrySet()) {
            Vote vote = getSendMsg();  // Msg moved to lastSendMsg
            VotingChannel qc = entry.getValue();
            if (vote != null) {
                qc.sendMsg(vote);
            }

            // If we have an outbound message from this channel put it.
            putInboundMsg(qc, qc.getMessage());
        }
    }

    private void closeAll() {
        try {
            if (selectorDispatch != null) {
                selectorDispatch.close();
            }
            for (Map.Entry<Long, VotingChannel> entry :
                    channelMap.entrySet()) {
                entry.getValue().close();
            }

            for (VotingChannel qc : acceptedChannels) {
                qc.close();
            }
            selectorDispatch = null;
        } catch (IOException exp) {
        }
    }

    /**
     * step 1: Look at the accepted connections and move known connections to
     *         serverStateMap.
     * step 2: Walk serverStateMap and remove connections that are in ERROR.
     * step 3: Walk serverStateMap and spawn new QuorumChannels for null
     *         entries.
     * step 4: Get current msg or last msg and send it to all channels and
     *         get in-bound msg from each channel if it exists.
     */
    private void loop() throws ChannelException, IOException {
        // step 1.
        reapAcceptedChannels();

        // step 2 and 3.
        processChannels();

        // step 4.
        sendMsg();
    }

    private void selectorRun() throws ChannelException, IOException {
        selectorDispatch.run();
    }

    private void selectorRunNow() throws ChannelException, IOException {
        selectorDispatch.runNow();
    }
}
