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
package com.quorum.helpers;

import com.common.X509Exception;
import com.quorum.ElectionException;
import com.quorum.QuorumPeer;
import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.flexible.QuorumVerifier;
import com.quorum.util.ChannelException;
import com.quorum.util.LogPrefix;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class EnsembleVoteView extends AbstractEnsemble {
    private static final Logger LOGS
            = LoggerFactory.getLogger(EnsembleVoteView.class.getClass());
    private final List<QuorumServer> servers;
    private final long readTimeoutMsec;
    private final long connectTimeoutMsec;
    private final long keepAliveTimeoutMsec;
    private final int keepAliveCount;
    private final boolean sslEnabled;
    final String keyStoreLocation;
    final String keyStorePassword;
    final String trustStoreLocation;
    final String trustStorePassword;
    final String trustStoreCAAlias;


    private final Map<Long, QuorumServer> serverMap;

    private MockQuorumBcast mockQuorumBcast;


    private Map<Long,
                ImmutablePair<QuorumBcastWithCnxMesh, VoteViewWrapper>>
            quorumBcastAndVoteViewMap = new HashMap<>();

    private FLEV2Wrapper fleThatRan;

    public EnsembleVoteView(final long id, final int quorumSize,
                            final QuorumVerifier quorumVerifier,
                            final EnsembleState pastState,
                            final Ensemble parent,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit,
                            final List<QuorumServer> servers,
                            final long readTimeoutMsec,
                            final long connectTimeoutMsec,
                            final long keepAliveTimeoutMsec,
                            final int keepAliveCount,
                            boolean sslEnabled,
                            final String keyStoreLocation,
                            final String keyStorePassword,
                            final String trustStoreLocation,
                            final String trustStorePassword,
                            final String trustStoreCAAlias) {
        super(id, quorumSize, quorumVerifier, pastState, parent, stableTimeout,
                stableTimeoutUnit);
        this.servers = servers;
        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
        this.sslEnabled = sslEnabled;
        this.keyStoreLocation = keyStoreLocation;
        this.keyStorePassword = keyStorePassword;
        this.trustStoreLocation = trustStoreLocation;
        this.trustStorePassword = trustStorePassword;
        this.trustStoreCAAlias = trustStoreCAAlias;

        this.serverMap = new HashMap<>();
        for (final QuorumServer quorumServer : servers) {
            if (serverMap.containsKey(quorumServer.id())) {
                throw new IllegalArgumentException("sid collision for " +
                        "server:" + quorumServer.id());
            }
            serverMap.put(quorumServer.id(), quorumServer);
        }

        mockQuorumBcast = new MockQuorumBcast(getId(), getQuorumSize(),
                parent == null ? null : parent.getQuorumCnxMesh());
    }

    public EnsembleVoteView(final long id, final int quorumSize,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit,
                            final List<QuorumServer> servers,
                            final long readTimeoutMsec,
                            final long connectTimeoutMsec,
                            final long keepAliveTimeoutMsec,
                            final int keepAliveCount,
                            boolean sslEnabled,
                            final String keyStoreLocation,
                            final String keyStorePassword,
                            final String trustStoreLocation,
                            final String trustStorePassword,
                            final String trustStoreCAAlias)
            throws ElectionException {
        this(id, quorumSize, new QuorumMajWrapper(quorumSize),
                EnsembleState.INVALID, null, stableTimeout, stableTimeoutUnit,
                servers, readTimeoutMsec, connectTimeoutMsec,
                keepAliveTimeoutMsec, keepAliveCount, sslEnabled,
                keyStoreLocation, keyStorePassword, trustStoreLocation,
                trustStorePassword, trustStoreCAAlias);
        this.fles = new HashMap<>();

        for (final FLEV2Wrapper fle : getQuorumWithInitVoteSet(
                quorumSize, new QuorumMajWrapper(quorumSize))) {
            this.fles.put(fle.getId(), fle);
        }
        this.LOG = new LogPrefix(LOGS, toString());
    }

    public EnsembleVoteView(final Ensemble parentEnsemble,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit,
                            final List<QuorumServer> servers)
            throws ElectionException {
        this(((EnsembleVoteView)parentEnsemble).getId() + 1,
                (parentEnsemble).getQuorumSize(),
                ((EnsembleVoteView)parentEnsemble).getQuorumVerifier(),
                ((EnsembleVoteView)parentEnsemble).getState(),
                parentEnsemble, stableTimeout, stableTimeoutUnit,
                servers,
                ((EnsembleVoteView)parentEnsemble).readTimeoutMsec,
                ((EnsembleVoteView)parentEnsemble).connectTimeoutMsec,
                ((EnsembleVoteView)parentEnsemble).keepAliveTimeoutMsec,
                ((EnsembleVoteView)parentEnsemble).keepAliveCount,
                ((EnsembleVoteView)parentEnsemble).sslEnabled,
                ((EnsembleVoteView)parentEnsemble).keyStoreLocation,
                ((EnsembleVoteView)parentEnsemble).keyStorePassword,
                ((EnsembleVoteView)parentEnsemble).trustStoreLocation,
                ((EnsembleVoteView)parentEnsemble).trustStorePassword,
                ((EnsembleVoteView)parentEnsemble).trustStoreCAAlias);
        this.fles = ((AbstractEnsemble)parentEnsemble).fles;
        this.partitionedQuorum = ((AbstractEnsemble) parentEnsemble)
                .partitionedQuorum;
        this.LOG = new LogPrefix(LOGS, toString());
    }

    @Override
    public Ensemble createEnsemble(
            final Ensemble parentEnsemble,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                    quorumWithState) throws ElectionException {
        List<QuorumServer> serversForEnsemble = servers;
        if (quorumWithState != null) {
            serversForEnsemble = new ArrayList<>();
            for (ImmutablePair<Long, QuorumPeer.ServerState> pair
                    : quorumWithState) {
                serversForEnsemble.add(serverMap.get(pair.getLeft()));
            }
        }

        return new EnsembleVoteView(parentEnsemble,
                stableTimeout, stableTimeoutUnit, serversForEnsemble);
    }

    @Override
    protected void runLookingForSid(final long sid) throws InterruptedException,
            ExecutionException, ElectionException {
        super.runLookingForSid(sid);
        fleThatRan = fles.get(sid);

        for (final FLEV2Wrapper f : fles.values()) {
            if (f.getId() != sid &&
                    f.getState() == QuorumPeer.ServerState.LOOKING
                    && isConnected(f.getId())) {
                futuresForLookingPeers.put(f.getId(),
                        f.runLeaderElection(getVotes()));
            }
        }

        futuresForLookingPeers.put(fleThatRan.getId(),
                fleThatRan.runLeaderElection(getVotes()));
    }

    @Override
    public FLEV2Wrapper getFleThatRan() {
        return fleThatRan;
    }

    @Override
    public boolean isConnected(final long serverSid) {
        return quorumCnxMesh != null && quorumCnxMesh.isConnectedToAny
                (serverSid);
    }

    @Override
    public Future<?> shutdown() {
        super.shutdown();
        for (final ImmutablePair<QuorumBcastWithCnxMesh, VoteViewWrapper> p
                : quorumBcastAndVoteViewMap.values()) {
            try {
                p.getRight().shutdown().get();
            } catch (InterruptedException | ExecutionException exp) {
                final String errStr = "Cannot fail here, exp: " + exp;
                LOG.error(errStr);
                throw new RuntimeException(errStr);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected FLEV2Wrapper createFLEV2(
            final long sid, final QuorumVerifier quorumVerifier) {
        return createFLEV2BcastWrapper(sid, quorumVerifier);
    }

    @Override
    protected FLEV2Wrapper copyFLEV2(final FLEV2Wrapper fle,
                                     final Vote vote)
            throws InterruptedException, ExecutionException {
        if (!(fle instanceof FLEV2BcastWrapper) &&
                !(fle instanceof FLEV2BcastNoThreadWrapper)) {
            throw new IllegalArgumentException("fle is not of proper type: "
                    + fle.getClass().getName());
        }
        assert vote != null;

        FLEV2Wrapper flev2Wrapper = createFLEV2BcastWrapper(fle
                .getId(), ((AbstractFLEV2Wrapper) fle).getQuorumVerifier());
        flev2Wrapper.updateSelfVote(vote).get();
        return flev2Wrapper;
    }

    /**
     * Only FINAL state ensemble is run hence minimize threads and other
     * artifcat creation, helps with resources for test cases.
     * @param sid
     * @param quorumVerifier
     * @return
     */
    private FLEV2Wrapper createFLEV2BcastWrapper(
            final long sid, final QuorumVerifier quorumVerifier) {
        if (this.getState() != EnsembleState.FINAL) {
            final MockVoteView mockVoteView
                    = new MockVoteView(sid, mockQuorumBcast);
            return new FLEV2BcastNoThreadWrapper(sid,
                    QuorumPeer.LearnerType.PARTICIPANT, quorumVerifier,
                    mockVoteView, mockVoteView, stableTimeout,
                    stableTimeoutUnit);
        }

        // In final state allocate everything required.
        final EventLoopGroup eventLoopGroupForQBCast
                = new NioEventLoopGroup(VoteViewWrapper.MAX_THREAD_COUNT,
                Executors.newSingleThreadExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable target) {
                        final Thread thread = new Thread(target);
                        LOG.debug("Creating new worker thread");
                        thread.setUncaughtExceptionHandler(
                                new Thread.UncaughtExceptionHandler() {
                                    @Override
                                    public void uncaughtException(Thread t,
                                                                  Throwable e) {
                                        LOG.error("Uncaught Exception: " + e);
                                        System.exit(1);
                                    }
                                });
                        return thread;
                    }
                }));

        com.quorum.netty.QuorumBroadcast quorumBroadcast;
        try {
            // Create a quorumBcast per VoteView.
            quorumBroadcast = new QuorumBcastWithCnxMesh(sid,
                    servers, serverMap.get(sid).getElectionAddr(),
                    eventLoopGroupForQBCast, readTimeoutMsec, connectTimeoutMsec,
                    keepAliveTimeoutMsec, keepAliveCount,
                    sslEnabled, quorumCnxMesh);
        } catch (ChannelException | IOException exp) {
            final String errStr = "Cannot fail here, exp: " + exp;
            LOG.error(errStr);
            throw new RuntimeException(errStr);
        }

        // In final state allocate everything required.
        final EventLoopGroup eventLoopGroupForVoteView
                = new NioEventLoopGroup(VoteViewWrapper.MAX_THREAD_COUNT,
                Executors.newSingleThreadExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable target) {
                        final Thread thread = new Thread(target);
                        LOG.debug("Creating new worker thread");
                        thread.setUncaughtExceptionHandler(
                                new Thread.UncaughtExceptionHandler() {
                                    @Override
                                    public void uncaughtException(Thread t,
                                                                  Throwable e) {
                                        LOG.error("Uncaught Exception: " + e);
                                        System.exit(1);
                                    }
                                });
                        return thread;
                    }
                }));

        final VoteViewWrapper voteViewWrapper = new VoteViewWrapper(sid,
                serverMap.get(sid).getElectionAddr(), eventLoopGroupForVoteView,
                quorumBroadcast);

        quorumBcastAndVoteViewMap.put(sid,
                ImmutablePair.of((QuorumBcastWithCnxMesh)quorumBroadcast,
                        voteViewWrapper));

        try {
            voteViewWrapper.start(keyStoreLocation, keyStorePassword,
                    trustStoreLocation, trustStorePassword, trustStoreCAAlias);
        } catch (IOException | ChannelException | CertificateException |
                NoSuchAlgorithmException | X509Exception.KeyManagerException |
                X509Exception.TrustManagerException exp) {
            final String errStr = "Cannot fail here, exp: " + exp;
            LOG.error(errStr);
            throw new RuntimeException(errStr);
        }

        return new FLEV2BcastWrapper(sid, QuorumPeer.LearnerType.PARTICIPANT,
                quorumVerifier, voteViewWrapper, voteViewWrapper, stableTimeout,
                stableTimeoutUnit);
    }
}
