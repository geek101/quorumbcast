package com.quorum.helpers;

import com.quorum.ElectionException;
import com.quorum.QuorumPeer;
import com.quorum.Vote;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public interface FLEV2Wrapper {
    long getId();
    QuorumPeer.ServerState getState();

    Vote getSelfVote();
    Future<Void> updateSelfVote(final Vote vote);

    Future<Vote> runLeaderElection(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException;

    void waitForVotesRun(final Map<Long, Vote> voteMap)
            throws InterruptedException, ExecutionException;
    void verifyNonTermination();

    void shutdown();
}
