package com.quorum.helpers;

import com.quorum.QuorumBroadcast;
import com.quorum.Vote;
import com.quorum.VoteViewChange;
import com.quorum.VoteViewChangeConsumer;
import com.quorum.VoteViewConsumer;
import com.quorum.VoteViewConsumerCtrl;
import com.quorum.VoteViewStreamConsumer;
import com.quorum.util.NotNull;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MockVoteView extends VoteViewChange implements
        VoteViewConsumerCtrl {
    protected static final Logger LOG = LoggerFactory.getLogger(
            MockVoteView.class.getName());
    final Map<Long, Vote> voteMap = new HashMap<>();
    private QuorumBroadcast mockQuorumBcast;

    public MockVoteView(final long mySid,
                        final QuorumBroadcast quorumBroadcast) {
        super(mySid);
        this.mockQuorumBcast = quorumBroadcast;
        if (!(quorumBroadcast instanceof MockQuorumBcast)) {
            throw new IllegalArgumentException("invalid quorumBroadcast type");
        }
        ((MockQuorumBcast)quorumBroadcast).addVoteViewChange(this);
    }

    @Override
    public Future<Void> updateSelfVote(final Vote vote)
            throws InterruptedException, ExecutionException {
        voteMap.put(getId(), vote);
        mockQuorumBcast.broadcast(vote);
        return ConcurrentUtils.constantFuture(null);
    }

    @Override
    public Vote getSelfVote() {
        return voteMap.get(getId());
    }

    /**
     * Get current view of votes as a collection. Will return null.
     * @return collection of votes.
     */
    public Collection<Vote> getVotes() {
        if (!voteMap.isEmpty()) {
            final Collection<Vote> votes = new ArrayList<>();
            for (final Vote v : voteMap.values()) {
                if (((MockQuorumBcast)mockQuorumBcast)
                        .connectionExists(getId(), v.getSid())) {
                    votes.add(v);
                }
            }
            return Collections.unmodifiableCollection(votes);
        }
        return Collections.<Vote>emptyList();
    }

    @Override
    public Future<Void> msgRx(final Vote vote) {
        NotNull.check(vote, "Received vote cannot be null", LOG);
        voteMap.put(vote.getSid(), vote);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public VoteViewChangeConsumer createChangeConsumer()
            throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public VoteViewStreamConsumer createStreamConsumer()
            throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public void removeConsumer(VoteViewConsumer voteViewConsumer) {
    }
}
