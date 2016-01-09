package com.quorum.helpers;

import com.quorum.ElectionException;
import com.quorum.QuorumPeer;
import com.quorum.Vote;
import com.quorum.VoteViewChange;
import com.quorum.VoteViewConsumerCtrl;
import com.quorum.flexible.QuorumVerifier;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MockFLEV2Wrapper extends AbstractFLEV2Wrapper {
    final VoteViewChange voteViewChange;
    public MockFLEV2Wrapper(final long mySid,
                               final QuorumPeer.LearnerType learnerType,
                               final QuorumVerifier quorumVerifier,
                               final VoteViewChange voteViewChange,
                               final VoteViewConsumerCtrl voteViewConsumerCtrl,
                               final int stableTimeout,
                               final TimeUnit stableTimeoutUnit) {
        super(mySid, learnerType, quorumVerifier, voteViewChange,
                voteViewConsumerCtrl, stableTimeout, stableTimeoutUnit);
        this.voteViewChange = voteViewChange;
    }

    @Override
    public ImmutablePair<Vote, Collection<Vote>> lookForLeaderLoopUpdateHelper(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException {
        return super.lookForLeaderLoopUpdateHelper(votes);
    }

    @Override
    public Future<Vote> runLeaderElection(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException {
        return CompletableFuture.completedFuture(
                lookForLeaderLoopUpdateHelper(votes).getLeft());
    }
}
