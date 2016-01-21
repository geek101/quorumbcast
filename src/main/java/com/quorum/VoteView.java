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
package com.quorum;

import com.common.X509Exception;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import com.quorum.util.NotNull;
import com.quorum.util.QuorumSocketFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.*;

public class VoteView extends VoteViewBase {
    private final InetSocketAddress electionAddr;
    private final QuorumBroadcast quorumBroadcast;
    private final EventLoopGroup eventLoopGroup;
    protected boolean isShutdown = true;

    private class VoteRxCb implements Callback<Vote> {
        @Override
        public void call(final Vote o) throws ChannelException, IOException {
            self.msgRx(o);
        }
    }

    /**
     * Used for testing.
     * @param mySid
     * @param electionAddr
     * @param eventLoopGroup
     * @param quorumBroadcast
     */
    protected VoteView(final Long mySid,
                       final InetSocketAddress electionAddr,
                       final EventLoopGroup eventLoopGroup,
                       final QuorumBroadcast quorumBroadcast) {
        super(mySid);

        this.electionAddr = electionAddr;
        if (!(this.group instanceof NioEventLoopGroup)) {
            throw new IllegalArgumentException("invalid event loop group");
        }
        this.eventLoopGroup = eventLoopGroup;
        if (!(quorumBroadcast instanceof  com.quorum.netty.QuorumBroadcast)){
            throw new IllegalArgumentException("invalid quorum broadcast");
        }
        this.quorumBroadcast = quorumBroadcast;
    }

    public VoteView(final String type,
                    final Long mySid,
                    final List<QuorumServer> servers,
                    final InetSocketAddress electionAddr,
                    final long readTimeoutMsec, final long connectTimeoutMsec,
                    final long keepAliveTimeoutMsec,
                    final int keepAliveCount)
            throws ChannelException, IOException {
        super(mySid);

        this.electionAddr = electionAddr;;
        this.eventLoopGroup = new NioEventLoopGroup(MAX_THREAD_COUNT,
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
                                        LOG.error("Uncaught Exception", e);
                                        System.exit(1);
                                    }
                                });
                        return thread;
                    }
                }));
        quorumBroadcast = QuorumBroadcastFactory.createQuorumBroadcast(
                type, mySid, servers, electionAddr, this.eventLoopGroup,
                readTimeoutMsec, connectTimeoutMsec, keepAliveTimeoutMsec,
                keepAliveCount);
    }

    public void start(final String keyStoreLocation,
                      final String keyStorePassword,
                      final String trustStoreLocation,
                      final String trustStorePassword,
                      final String trustStoreCAAlias)
            throws IOException, ChannelException, CertificateException,
            NoSuchAlgorithmException, X509Exception.KeyManagerException,
            X509Exception.TrustManagerException  {
        synchronized (this) {
            if (isShutdown) {
                isShutdown = false;
                quorumBroadcast.start(new VoteRxCb(),
                        keyStoreLocation, keyStorePassword,
                        trustStoreLocation, trustStorePassword,
                        trustStoreCAAlias);
            }
        }
    }

    public Future<?> shutdown() throws InterruptedException {
        synchronized (this) {
            if (!isShutdown) {
                quorumBroadcast.shutdown();
                isShutdown = true;
                return this.eventLoopGroup.shutdownGracefully();
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    /**
     * API to update the current Vote of self asynchronously,
     * will trigger a task to broadcast this to current consumers
     * asynchronously.
     * @param vote Vote to be sent
     */
    @Override
    public Future<Void> updateSelfVote(final Vote vote)
            throws InterruptedException, ExecutionException {
        Future<Void> future = super.updateSelfVote(vote);
        quorumBroadcast.broadcast(vote);
        return future;
    }

    public void addServer(final QuorumServer server) throws Exception {
        NotNull.check(server, "Cannot accept null for QuorumServer", LOG);
        synchronized (this) {
            quorumBroadcast.addServer(server);
        }
    }

    public void removeServer(final QuorumServer server) throws Exception {
        NotNull.check(server, "Cannot accept null for QuorumServer", LOG);
        synchronized (this) {
            quorumBroadcast.removeServer(server);
        }
    }

    public final QuorumServer getServer() {
        return new QuorumServer(this.getId(), this.electionAddr);
    }

    /**
     * Get current view of votes as a collection. Will return null.
     * @return collection of votes.
     */
    public Collection<Vote> getVotes() {
        if (!voteMap.isEmpty()) {
            return Collections.unmodifiableCollection(voteMap.values());
        }
        return Collections.<Vote>emptyList();
    }

    /**
     * creates a change consumer with current map, runs this on the
     * same thread where votes are processed, hence it is safe for consumption.
     * Remember only one global consumer is supported, calling multiple times
     * will throw a runtime exception.
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public VoteViewChangeConsumer createChangeConsumer()
            throws InterruptedException, ExecutionException {
        return super.createChangeConsumer();
    }

    /**
     * Create a stream consumer where incoming votes that change the previous
     * view update will be queued to consumer.
     * Remember only one global consumer is supported calling multiple times
     * will throw a runtime exception.
     * @return
     */
    @Override
    public VoteViewStreamConsumer createStreamConsumer()
            throws InterruptedException, ExecutionException  {
        return super.createStreamConsumer();
    }

    /**
     * Remove the given consumer, null or invalid argument will throw
     * runtime exception
     * @param voteViewConsumer
     */
    @Override
    public void removeConsumer(final VoteViewConsumer voteViewConsumer) {
        super.removeConsumer(voteViewConsumer);
    }
}
