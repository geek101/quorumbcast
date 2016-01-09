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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class VoteViewMockBcast extends VoteViewBase {
    final QuorumBroadcast quorumBroadcast;
    public VoteViewMockBcast(final long mySid,
                             final QuorumBroadcast quorumBroadcast) {
        super(mySid, Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
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
                }));
        this.quorumBroadcast = quorumBroadcast;
        if (!(quorumBroadcast instanceof MockQuorumBcast)) {
            throw new IllegalArgumentException("invalid quorumBroadcast type");
        }
        ((MockQuorumBcast)quorumBroadcast).addVoteViewChange(this);
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
                        .connectionExists(getId(), v.getSid())) {
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
