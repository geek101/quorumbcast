package com.quorum.helpers;

import com.quorum.ElectionException;
import com.quorum.FastLeaderElectionV2;
import com.quorum.Vote;
import com.quorum.VoteViewChange;
import com.quorum.VoteViewConsumerCtrl;
import com.quorum.flexible.QuorumVerifier;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.quorum.QuorumPeer.LearnerType;
import static com.quorum.QuorumPeer.ServerState;

public abstract class AbstractFLEV2Wrapper extends FastLeaderElectionV2
implements FLEV2Wrapper {
    private final VoteViewChange voteViewChange;

    public AbstractFLEV2Wrapper(final long mySid,
                                final LearnerType learnerType,
                                final QuorumVerifier quorumVerifier,
                                final VoteViewChange voteViewChange,
                                final VoteViewConsumerCtrl voteViewConsumerCtrl,
                                final int stableTimeout,
                                final TimeUnit stableTimeoutUnit) {
        super(mySid, learnerType, quorumVerifier, voteViewChange,
                voteViewConsumerCtrl, stableTimeout, stableTimeoutUnit);
        this.voteViewChange = voteViewChange;
    }

    public Vote getSelfVote() {
        return voteViewChange.getSelfVote();
    }

    public ServerState getState() {
        return voteViewChange.getSelfVote().getState();
    }

    public Future<Void> updateSelfVote(final Vote vote) {
        try {
            return voteViewChange.updateSelfVote(vote);
        } catch (InterruptedException | ExecutionException exp) {
            throw new RuntimeException(exp);
        }
    }

    public void breakFromLeader() {
        updateSelfVote(getSelfVote().breakFromLeader());
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public int hashCode() {
        return (int) this.getId();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof AbstractFLEV2Wrapper &&
                this.getId() == ((AbstractFLEV2Wrapper) other).getId();
    }

    public QuorumVerifier getQuorumVerifier() {
        return super.getQuorumVerifier();
    }

    public LearnerType getLearnerType() {
        return super.getLearnerType();
    }

    public VoteViewChange getVoteViewChange() {
        return super.getVoteViewChange();
    }

    public VoteViewConsumerCtrl getVoteViewConsumerCtrl() {
        return super.getVoteViewConsumerCtrl();
    }

    public int getStableTimeout() {
        return super.getStableTimeout();
    }

    public TimeUnit getStableTimeUnit() {
        return super.getStableTimeUnit();
    }
}

