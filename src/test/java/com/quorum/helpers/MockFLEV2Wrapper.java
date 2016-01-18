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

    /**
     * Run one loop of election epoch update and leader election and return
     * the elected vote along with the collection of votes which will contain
     * our self vote updated to election epoch.
     * Update self vote to follow the leader.
     * @param votes
     * @return Leader elected vote and set of votes with our vote with
     * election epoch updated.
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public ImmutablePair<Vote, Collection<Vote>> lookForLeaderLoopUpdateHelper(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException {
        final ImmutablePair<Vote, Collection<Vote>> pair
                = super.lookForLeaderLoopUpdateHelper(votes);
        // Update our vote if leader is non null
        if (pair.getLeft() != null) {
            updateSelfVote(catchUpToLeaderBeforeExitAndUpdate(pair.getLeft(),
                    getSelfVote())).get();
        }
        return ImmutablePair.of(pair.getLeft(), pair.getRight());
    }

    @Override
    public Future<Vote> runLeaderElection(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException {
        return CompletableFuture.completedFuture(
                lookForLeaderLoopUpdateHelper(votes).getLeft());
    }
}
