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

import com.quorum.QuorumBroadcast;
import com.quorum.Vote;
import com.quorum.VoteViewBase;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class VoteViewMockBcast extends VoteViewBase {
    final QuorumBroadcast quorumBroadcast;
    protected VoteViewMockBcast(final long mySid,
                                final ExecutorService executorService,
                                final QuorumBroadcast quorumBroadcast) {
        super(mySid, executorService);
        if (!(quorumBroadcast instanceof MockQuorumBcast)) {
            throw new IllegalArgumentException("invalid quorumBroadcast type");
        }
        this.quorumBroadcast = quorumBroadcast;
        ((MockQuorumBcast)quorumBroadcast).addVoteViewChange(this);
    }

    public VoteViewMockBcast(final long mySid,
                             final QuorumBroadcast quorumBroadcast) {
        this(mySid, Executors.newSingleThreadScheduledExecutor(
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable target) {
                        final Thread thread = new Thread(target);
                        LOG.debug("Creating new worker thread");
                        thread.setUncaughtExceptionHandler(
                                new Thread.UncaughtExceptionHandler() {
                                    @Override
                                    public void uncaughtException(Thread t, Throwable e) {
                                        LOG.error("Uncaught Exception", e);
                                        System.exit(1);
                                    }
                                });
                        return thread;
                    }
                }), quorumBroadcast);
    }

    /**
     * Get current view of votes as a collection. Will return null.
     * @return collection of votes.
     */
    public Collection<Vote> getVotes() {
        if (!voteMap.isEmpty()) {
            final Collection<Vote> votes = new ArrayList<>();
            for (final Vote v : voteMap.values()) {
                if (((MockQuorumBcast)quorumBroadcast)
                        .connected(getId(), v.getSid())) {
                    votes.add(v);
                }
            }
            return Collections.unmodifiableCollection(votes);
        }
        return Collections.<Vote>emptyList();
    }

    @Override
    public Future<Void> updateSelfVote(final Vote vote)
            throws InterruptedException, ExecutionException {
        quorumBroadcast.broadcast(vote);
        return super.msgRx(vote);
    }

    @Override
    public Future<Void> msgRx(final Vote vote) {
        return super.msgRx(vote);
    }
}
