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

import com.quorum.flexible.QuorumVerifier;
import com.quorum.util.LogPrefix;
import com.quorum.util.NotNull;
import com.quorum.util.Predicate;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class FastLeaderElectionV2 implements Election {
    private static final Logger LOGS
            = LoggerFactory.getLogger(FastLeaderElectionV2.class);
    private static final int CONSUME_WAIT_MSEC = 3000;  // wait for top loop.
    private final Long mySid;
    private final QuorumPeer.LearnerType learnerType;
    private final QuorumVerifier quorumVerifier;
    private final VoteViewChange voteViewChange;
    private final VoteViewConsumerCtrl voteViewConsumerCtrl;
    private final int stableTimeout;
    private final TimeUnit stableTimeoutUnit;
    protected LogPrefix LOG = null;

    public FastLeaderElectionV2(
            final long mySid, final QuorumPeer.LearnerType learnerType,
            final QuorumVerifier quorumVerifier,
            final VoteViewChange voteViewChange,
            final VoteViewConsumerCtrl voteViewConsumerCtrl,
            final int stableTimeout,
            final TimeUnit stableTimeoutUnit) {
        this.mySid = mySid;
        this.learnerType = learnerType;
        this.quorumVerifier = quorumVerifier;
        this.voteViewChange = voteViewChange;
        this.voteViewConsumerCtrl = voteViewConsumerCtrl;
        this.stableTimeout = stableTimeout;
        this.stableTimeoutUnit = stableTimeoutUnit;
        this.LOG = new LogPrefix(LOGS, "mySid:" + this.mySid +
                "-electionEpoch:0");
    }

    /**
     * Predicate used to get changes to Vote set we last processed.
     */
    private class DefaultPredicate extends Predicate<Collection<Vote>> {
        private final Map<Long, Vote> lastVotesMap;

        public DefaultPredicate(final Collection<Vote> lastVotes) {
            lastVotesMap = new HashMap<>();
            for (final Vote v : lastVotes) {
                lastVotesMap.put(v.getSid(), v);
            }
        }

        @Override
        public Boolean call(final Collection<Vote> votes) {
            for (final Vote v : votes) {
                if (!lastVotesMap.containsKey(v.getSid()) ||
                        !lastVotesMap.get(v.getSid()).match(v)) {
                    return true;
                }
            }
            return lastVotesMap.size() < votes.size();
        }
    }

    /**
     * Leader Stability predicate helps with verifying leader stability check.
     */
    private class LeaderStabilityPredicate extends
            Predicate<Collection<Vote>> {
        private final Vote electedLeaderVote;
        private final Map<Long, Vote> lastVotesMap;

        public LeaderStabilityPredicate(final Vote electedLeaderVote,
                                        final Collection<Vote> lastVotes) {
            this.electedLeaderVote = electedLeaderVote;
            lastVotesMap = new HashMap<>();
            for (final Vote v : lastVotes) {
                lastVotesMap.put(v.getSid(), v);
            }
        }

        /**
         * Check each given vote and verify the stored elected leader is still
         * capable of being a leader.
         *
         * @param votes
         * @return
         */
        @Override
        public Boolean call(final Collection<Vote> votes) {
            boolean votesChanged = false;
            for (final Vote v : votes) {
                if (!lastVotesMap.containsKey(v.getSid()) ||
                        !lastVotesMap.get(v.getSid()).match(v)) {
                    votesChanged = true;
                    break;
                }
            }

            if (!votesChanged && lastVotesMap.size() < votes.size()) {
                return false;
            }

            final Vote stabilityCheckElectionVote
                    = lookForLeaderLoopHelper(votes).getLeft();
            if (stabilityCheckElectionVote == null ||
                    !stabilityCheckElectionVote.equals(
                            this.electedLeaderVote)) {
                if (stabilityCheckElectionVote != null) {
                    LOG.info("broke stability for: " + stabilityCheckElectionVote);
                } else {
                    LOG.info("broke stability for null vote");
                }
                return true;
            }

            LOG.info("was stable for leader: " + this.electedLeaderVote);
            return false;
        }
    }

    public long getId() {
        return mySid;
    }

    public void shutdown() {
        LOG.info("shutdown");
    }

    public Vote lookForLeader() throws ElectionException,
            InterruptedException, ExecutionException {
        throw new IllegalAccessError("not implemented");
    }

    /**
     * API to start leader election round.
     *
     * @return Vote if leader election is done, VoteView will be updated.
     * @throws ElectionException    fatal exception, something went wrong must
     *                              exit.
     * @throws InterruptedException fatal exception must exit
     * @throws ExecutionException   fatal exception must exit.
     */
    public Vote lookForLeader(final long peerEpoch,
                              final long zxid) throws ElectionException,
            InterruptedException, ExecutionException {
        // increase the logical clock and start with last good values
        final Vote selfVote = getSelfVote()
                .leaderElectionVote(peerEpoch, zxid);

        // let us broadcast this and  wait till that is done.
        updateSelfVote(selfVote).get();

        this.LOG.resetPrefix("mySid:" + this.mySid +
                "-electionEpoch:" + selfVote.getElectionEpoch());
        // Create and get the change consumer.
        final VoteViewChangeConsumer consumer
                = voteViewConsumerCtrl.createChangeConsumer();

        // Start with just our vote.
        Collection<Vote> votes = Collections.singletonList(selfVote);
        while (true) {
            // Get leader vote and self vote with election epoch updated.
            final ImmutablePair<Vote, Vote> leaderElectedAndSelfVote =
                    lookForLeaderLoop(consumer, CONSUME_WAIT_MSEC,
                            TimeUnit.MILLISECONDS, votes);
            if (leaderElectedAndSelfVote.getLeft() == null) {
                continue;
            }

            // Found a Vote, verify stability
            votes = leaderStabilityCheckLoop(consumer, stableTimeout,
                    stableTimeoutUnit, leaderElectedAndSelfVote.getLeft(),
                    leaderElectedAndSelfVote.getRight(), votes);
            if (votes == null) {
                final Vote selfFinalVote
                        = catchUpToLeaderBeforeExit(
                        leaderElectedAndSelfVote.getLeft(),
                        leaderElectedAndSelfVote.getRight());
                LOG.info("elected leader, self vote: " + selfFinalVote);
                voteViewConsumerCtrl.removeConsumer(consumer);
                return selfFinalVote;
            }

            LOG.info("leader stability failed, trying again");
        }
    }

    protected Vote catchUpToLeaderBeforeExit(final Vote leaderVote,
                                             final Vote selfVote)
            throws InterruptedException, ExecutionException {
        // We are done, catch up to leader vote and break the loop.
        QuorumPeer.ServerState targetState
                = (leaderVote.getLeader() == getId()) ?
                QuorumPeer.ServerState.LEADING : learningState();

        final Vote finalVote = selfVote.
                catchUpToLeaderVote(leaderVote, targetState);
        updateSelfVote(finalVote);
        return finalVote;
    }

    /**
     * Main loop for getting leader from a given view using the consumer
     * consume with default predicate.
     *
     * @param timeout
     * @param unit
     * @param votes   the starting view, we update it and use it.
     * @return Vote which is the elected leader if it did else null.
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private ImmutablePair<Vote, Vote> lookForLeaderLoop(
            final VoteViewChangeConsumer consumer, final int timeout,
            final TimeUnit unit, Collection<Vote> votes)
            throws ElectionException,
            InterruptedException, ExecutionException {
        do {
            final ImmutablePair<Vote, Collection<Vote>>
                    leaderAndEpochUpdatedVotes
                    = lookForLeaderLoopUpdateHelper(votes);

            final Vote selfVote
                    = getSelfVoteFromSet(leaderAndEpochUpdatedVotes.getRight());

            // If leader is found return the leader vote and updated self vote.
            if (leaderAndEpochUpdatedVotes.getLeft() != null) {
                return ImmutablePair.of(
                        leaderAndEpochUpdatedVotes.getLeft(), selfVote);
            }

            // No success try again, This set of votes contain the updated
            // ElectionEpoch for self Vote but not the modified self Vote
            // done as part of finding a leader. Hence if there is any
            // modification done to self Vote then the next consume call will
            // break immediately and election will be rerun with that updated
            // info. Which is what we desire.
            votes = leaderAndEpochUpdatedVotes.getRight();
        } while ((votes = consumer.consume(
                timeout, unit, new DefaultPredicate(votes))) != null);
        return ImmutablePair.of(null, null);
    }

    /**
     * Wraps the helper() and also performs the update of VoteView with
     * our self vote and if our vote's election epoch is updated that will
     * trigger another round if required.
     * @param votes
     * @return
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected ImmutablePair<Vote, Collection<Vote>>
    lookForLeaderLoopUpdateHelper(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException {
        ImmutableTriple<Vote, Vote, HashMap<Long, Vote>> leaderAndEpochVotes
                = lookForLeaderLoopHelper(votes);

        final Vote selfVote = leaderAndEpochVotes.getMiddle();
        updateSelfVote(selfVote);
        final HashMap<Long, Vote> voteMap = leaderAndEpochVotes.getRight();
        voteMap.put(selfVote.getSid(), selfVote);
        return ImmutablePair.of(leaderAndEpochVotes.getLeft(),
                voteMap.values());
    }

    /**
     * Non update version of the above so that stability predicate can use it.
     * @param votes
     * @return
     */
    protected ImmutableTriple<Vote, Vote, HashMap<Long, Vote>>
    lookForLeaderLoopHelper(
            final Collection<Vote> votes) {
        // Returns the elected Leader and Collection of votes.
        final HashMap<Long, Vote> voteMap = new HashMap<>();
        for (final Vote v : votes) {
            voteMap.put(v.getSid(), v);
        }

        ImmutablePair<Vote, Vote> pair = lookForLeaderLoopHelper(voteMap);
        return ImmutableTriple.of(pair.getLeft(), pair.getRight(), voteMap);
    }

    /**
     * @param votes
     * @return pair of leader vote and updated self vote with election epoch
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected ImmutablePair<Vote, Vote> lookForLeaderLoopHelper(
            final HashMap<Long, Vote> votes) {
        // Try to set our Epoch and other fields using the LOOKING vote set.
        final HashMap<Long, Vote> electionReadyVotes
                = lookingElectionEpochConverge(votes);

        // Now try to find a leader within the updated set.
        return ImmutablePair.of(leaderFromView(electionReadyVotes),
                electionReadyVotes.get(getId()));
    }

    /**
     * Go over the votes in LOOKING state and if there exists a better
     * ElectionEpoch among the votes use that and return a set of votes that
     * match the best ElectionEpoch and also catch up to the best
     * totalOrderVote among them. Do not update the VoteView here.
     *
     * @param votes
     * @return The set of LOOKING and LEADER/FOLLOWER votes which contains our
     * Vote and it could have been updated to reflect the highest
     * ElectionEpoch in the view.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected HashMap<Long, Vote> lookingElectionEpochConverge(
            final HashMap<Long, Vote> votes) {
        // Try to set our Epoch and other fields using the LOOKING vote set.
        // we will use our vote from the set.
        NotNull.check(votes, "vote set is null", LOG);
        if (votes.isEmpty()) {
            return votes;
        }

        for (final Vote v : votes.values()) {
            LOG.info("lookingElectionEpochConverge: " + v);
        }

        // Our vote after being updated for election epoch
        final Vote selfUpdatedVote
                = lookingElectionEpochUse(votes, votes.get(getId()));

        // Gather votes for leader election consumption.
        final HashMap<Long, Vote> electionReadyVotes = new HashMap<>();
        for (final Vote vote : votes.values()) {
            // If LOOKING it has to match our election epoch which is the
            // best from the given view
            if (vote.getState() == QuorumPeer.ServerState.LOOKING &&
                    vote.getElectionEpoch()
                            == selfUpdatedVote.getElectionEpoch()) {
                electionReadyVotes.put(vote.getSid(), vote);
            }

            // Add any non looking and non observing vote.
            if (vote.getState() == QuorumPeer.ServerState.FOLLOWING ||
                    vote.getState() == QuorumPeer.ServerState.LEADING) {
                electionReadyVotes.put(vote.getSid(), vote);
            }
        }

        electionReadyVotes.put(selfUpdatedVote.getSid(), selfUpdatedVote);
        return electionReadyVotes;
    }

    /**
     * Go through the LOOKING votes and get a new vote with
     * update of election epoch if given epoch equal or greater and/or total
     * order predicate is true then catch to that vote.
     *
     * @param votes give votes
     */
    protected Vote lookingElectionEpochUse(final HashMap<Long, Vote> votes,
                                           final Vote currentVote) {
        Vote selfVote = currentVote;

        // Update our vote with highest or equal Epoch when compared to us and
        // the set and also steal better numbers if we can.
        for (final Vote vote : votes.values()) {
            if (vote.getState() != QuorumPeer.ServerState.LOOKING
                    || vote.getSid() == getId()) {
                continue;
            }

            if (vote.getElectionEpoch() >= selfVote.getElectionEpoch()) {
                if (vote.getElectionEpoch() >= selfVote.getElectionEpoch()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found better or equal Election Epoch Vote: "
                                + vote);
                    }
                    selfVote = selfVote.setElectionEpoch(vote);
                }
                if (totalOrderPredicate(vote, selfVote)) {
                    LOG.debug("Found better Election epoch and Total Order " +
                            "Predicate: " + vote);
                    selfVote = updateProposal(vote, selfVote);
                }
            }
        }
        return selfVote;
    }

    /**
     * Run leader election using the given view and if no leader has been
     * elected then update self as leader.
     * The algorithm is from a given view of votes performs the following
     * steps:
     * <p>
     * Step 1:
     * Group votes by PeerEpoch and Zxid and get the highest group by
     * PeerEpoch and Zxid. Lets call this highest group H(G(PZ)).
     * <p>
     * Step 2:
     * Ref count leader from the given H(G(PZ)). We will consider leader
     * votes if and only if we see the vote and the vote considers itself a
     * leader. Sort this leader groups in descending order. Lets call this
     * set of Leader votes along with votes which refer it as SL(H(G(PZ))
     * <p>
     * Step 3:
     * Consider all leaders withs maximum votes, lets call it H(SL(H(G(PZ))))
     * <p>
     * Step 4:
     * Go through each vote in H(SL(H(G(PZ))) use totalOrderPredicate() to
     * pick the best among them. Call this TOP(H(SL(H(G(PZ)))
     * <p>
     * Step 5:
     * If the picked best Leader has Quorum then we elect it as leader and
     * exit FLEV2, if there is no Quorum then we catch up to this Leader and
     * go back to wait loop, it will reenter if our Vote was perturbed by the
     * above change and/or there was a change in the view.
     *
     * @param voteMap
     * @return
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected Vote leaderFromView(final HashMap<Long, Vote> voteMap) {
        for (final Vote v : voteMap.values()) {
            LOG.info("leaderFromView: " + v);
        }
        final Collection<Vote> highestPeerEpochAndZxidGroup =
                getHighestPeerEpochAndZxidGroup(voteMap);

        final Map<Long, Vote> highestPeerEpochAndZxidGroupMap = new HashMap<>();
        for (final Vote vote : highestPeerEpochAndZxidGroup) {
            highestPeerEpochAndZxidGroupMap.put(vote.getSid(), vote);
        }

        final ImmutablePair<Vote, HashSet<Long>> leaderElectedCountPair =
                getLeaderByCount(highestPeerEpochAndZxidGroupMap);
        final Vote leaderElectedVote = leaderElectedCountPair.getLeft();
        final HashSet<Long> quorumForLeaderElected
                = leaderElectedCountPair.getRight();
        if (leaderElectedVote == null) {
            return null;
        }

        if (!quorumVerifier.containsQuorum(quorumForLeaderElected)) {
            LOG.info("No quorum for Leader elected, Vote: "
                    + leaderElectedVote + " with count:"
                    + quorumForLeaderElected.size());
            return null;
        }

        LOG.info("Quorum for Leader elected, Vote: " + leaderElectedVote
                + " with count:" + quorumForLeaderElected.size());
        // For peace of mind!, if we picked someone else ensure
        // that that vote thinks its a leader.
        assert leaderElectedVote.getSid() == getId() ||
                leaderElectedVote.getLeader() == leaderElectedVote.getSid();

        return leaderElectedVote;
    }

    class PeerEpochAndZxid implements Comparable<PeerEpochAndZxid> {
        private final long peerEpoch;
        private final long zxid;

        public PeerEpochAndZxid(final Vote from) {
            this.peerEpoch = from.getPeerEpoch();
            this.zxid = from.getZxid();
        }

        public long getZxid() {
            return zxid;
        }

        public long getPeerEpoch() {
            return peerEpoch;
        }

        @Override
        public String toString() {
            return "PeerEpoch: " + Long.toHexString(getPeerEpoch())
                    + " zxid:" + Long.toHexString(zxid);
        }

        @Override
        public int hashCode() {
            final long h = getZxid() | getPeerEpoch();
            return (int) (h >> 32 ^ (int) h);
        }

        @Override
        public boolean equals(final Object other) {
            return (other instanceof PeerEpochAndZxid) &&
                    this.getPeerEpoch() ==
                            ((PeerEpochAndZxid) other).getPeerEpoch() &&
                    this.getZxid() == ((PeerEpochAndZxid) other).getZxid();
        }

        /**
         * 0 of other is equal to this
         * -1 if other is greater than this
         * 1 if other is less than this
         *
         * @param o
         * @return
         */
        @Override
        public int compareTo(final PeerEpochAndZxid o) {
            if (this.equals(o)) {
                return 0;
            } else if (this.less(o)) {
                return -1;
            } else {
                return 1;
            }
        }

        /**
         * This is strictly less than other.
         * Our PeerEpoch is strictly less or PeerEpoch is equal and our Zxid
         * is strictly less.
         *
         * @param other
         * @return
         */
        private boolean less(final PeerEpochAndZxid other) {
            return this.getPeerEpoch() < other.getPeerEpoch() ||
                    (this.getPeerEpoch() == other.getPeerEpoch() &&
                            this.getZxid() < other.getZxid());
        }
    }

    /**
     * .
     * TODO: ElectionEpoch < our ElectionEpoch is considered, read below.
     * One point of deviation from previous version is we also consider a Vote
     * which is in LOOKING state but its epoch is perhaps less than ours.
     * <p>
     * Protected for test case use.
     *
     * @param voteMap given Vote view
     * @return votes grouped by the highest peer epoch and zxid.
     */

    protected Collection<Vote>
    getHighestPeerEpochAndZxidGroup(final HashMap<Long, Vote> voteMap) {
        if (voteMap == null || voteMap.isEmpty()) {
            LOG.info("votes are null or empty, cannot find leader");
            return null;
        }

        /**
         * Count for each Vote how many Votes elect it as
         * a leader including itself.
         */

        final Map<PeerEpochAndZxid, HashSet<Long>> peerEpochAndZxidCountMap
                = new HashMap<>();
        for (final Vote vote : voteMap.values()) {
            if (vote.getState() != QuorumPeer.ServerState.OBSERVING) {
                // Group votes by ZXidAndPeerEpoch
                PeerEpochAndZxid peerEpochAndZxid
                        = new PeerEpochAndZxid(vote);
                if (peerEpochAndZxidCountMap.containsKey(peerEpochAndZxid)) {
                    peerEpochAndZxidCountMap.get(peerEpochAndZxid)
                            .add(vote.getSid());
                } else {
                    peerEpochAndZxidCountMap.put(new PeerEpochAndZxid(vote),
                            new HashSet<Long>(
                                    Collections.singletonList(vote.getSid())));
                }
            }
        }

        // Iterate through zxidAndPeerEpoch grouped map and get
        // the highest entry
        PeerEpochAndZxid highestPeerEpochAndZxid = null;
        for (final PeerEpochAndZxid zp : peerEpochAndZxidCountMap.keySet()) {
            if (highestPeerEpochAndZxid == null ||
                    highestPeerEpochAndZxid.compareTo(zp) < 0) {
                highestPeerEpochAndZxid = zp;
            }
        }

        // Get the quorum set for the highestPeerEpochAndZxid
        final HashSet<Long> quorumSetForHighZAndP
                = peerEpochAndZxidCountMap.get(highestPeerEpochAndZxid);
        // LOG if there is a majority with best PeerEpochAndZxid
        if (!quorumVerifier.containsQuorum(quorumSetForHighZAndP)) {
            LOG.info("Not enough quorum for highestPeerEpochAndZxid : "
                    + highestPeerEpochAndZxid + " size: " +
                    quorumSetForHighZAndP.size());
        } else {
            LOG.info("Quorum found for highestPeerEpochAndZxid : "
                    + highestPeerEpochAndZxid + " size: " +
                    quorumSetForHighZAndP.size());
        }

        final Collection<Vote> quorumVotesForHighZAndP = new ArrayList<>();
        for (final long serverSid : quorumSetForHighZAndP) {
            quorumVotesForHighZAndP.add(voteMap.get(serverSid));
        }

        return quorumVotesForHighZAndP;
    }

    // Group vote id and count together, helps us with sort
    class VoteCountSet implements Comparable<VoteCountSet> {
        private final long leaderSid;
        private HashSet<Long> voteSet;

        public VoteCountSet(final Vote leaderVote) {
            this.leaderSid = leaderVote.getSid();
            this.voteSet = new HashSet<>(
                    Collections.singletonList(leaderVote.getSid()));
        }

        public long getSid() {
            return this.leaderSid;
        }

        public int getCount() {
            return this.voteSet.size();
        }

        public HashSet<Long> getVoteSet() {
            return this.voteSet;
        }

        public void addVote(final Vote vote) {
            this.voteSet.add(vote.getSid());
        }

        @Override
        public int compareTo(final VoteCountSet o) {
            return Integer.compare(this.getCount(), o.getCount());
        }
    }

    /**
     * Step 2, 3, and 4. Refcount for each leader. Helps us pick the best set.
     *
     * @param voteMap
     * @return LeaderVote and Sid of Votes that elected it as leader. null,
     * null otherwise.
     */
    private ImmutablePair<Vote, HashSet<Long>>
    getLeaderByCount(final Map<Long, Vote> voteMap) {
        final HashMap<Long, VoteCountSet> voteToCountMap = new HashMap<>();
        // Look at each vote count reference for each valid leader.
        for (final Vote vote : voteMap.values()) {
            if (!checkLeader(voteMap, vote)) {
                continue;
            }

            // get the leader's vote for the given vote.
            final Vote leaderForVote = voteMap.get(vote.getLeader());
            if (voteToCountMap.containsKey(leaderForVote.getSid())) {
                voteToCountMap.get(leaderForVote.getSid()).addVote(vote);
            } else {
                voteToCountMap.put(leaderForVote.getSid(),
                        new VoteCountSet(leaderForVote));
                voteToCountMap.get(leaderForVote.getSid()).addVote(vote);

            }
        }

        VoteCountSet bestTotalOrder = null;
        // Among the most elected leaders pick the best.
        // There could be a case with multiple leaders have the best count,
        // in that use totalOrderPredicate() to get the best among them.
        final ArrayList<VoteCountSet> voteCounts
                = new ArrayList<>(voteToCountMap.values());
        Collections.sort(voteCounts, Collections.reverseOrder());
        int max = Integer.MIN_VALUE;
        for (final VoteCountSet voteCountSet : voteCounts) {
            if (max > voteCountSet.getCount()) {
                break;
            }
            max = voteCountSet.getCount();
            if (bestTotalOrder == null ||
                    totalOrderPredicate(voteMap.get(voteCountSet.getSid()),
                            voteMap.get(bestTotalOrder.getSid()))) {
                bestTotalOrder = voteCountSet;
            }
        }

        if (bestTotalOrder == null) {
            return ImmutablePair.of(null, null);
        }

        return ImmutablePair.of(voteMap.get(bestTotalOrder.getSid()),
                bestTotalOrder.getVoteSet());
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param voteMap    set of votes
     * @param vote vote that points to a leader
     */
    protected boolean checkLeader(
            final Map<Long, Vote> voteMap,
            final Vote vote) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if (vote.getLeader() != getId()) {
            if (voteMap.get(vote.getLeader()) == null) {
                LOG.debug("Ignore non existent leader, Vote: " + vote);
                predicate = false;
            } else if (voteMap.get(vote.getLeader()).getLeader() !=
                    voteMap.get(vote.getLeader()).getSid() ||
                    voteMap.get(vote.getLeader()).getState()
                            != QuorumPeer.ServerState.LEADING) {
                LOG.debug("Ignore leader that did not elect itself, Vote: "
                        + vote + " leader vote: "
                        + voteMap.get(vote.getLeader()));
                predicate = false;
            }
        } else if (voteMap.get(getId()).getElectionEpoch()
                != vote.getElectionEpoch()) {
            LOG.debug("we cannot be leader, election epoch mismatch, Vote: "
                    + vote);
            predicate = false;
        }

        return predicate;
    }

    /**
     * Selected leader should be visible and should consider itself
     * as leader a
     * @param leaderVote
     * @param vote
     * @return
     */
    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param voteMap set of votes
     * @param vote vote that requested this leader.
     * @return true if elected leader is sane.
     */
    @Deprecated
    protected boolean checkLeader(HashMap<Long, Vote> voteMap,
                                  final Vote vote) {
        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if(vote.getLeader() != getId()){
            if(voteMap.get(vote.getLeader()) == null) {
                LOG.info("Ignore non existent leader, Vote: " + vote);
                predicate = false;
            }
            else if(voteMap.get(vote.getLeader()).getState()
                    != QuorumPeer.ServerState.LEADING) {
                LOG.info("Ignore leader that did not elect itself, Vote: "
                        + vote + " leader vote: "
                        + voteMap.get(vote.getLeader()));
                predicate = false;
            }
        } else if(voteMap.get(vote.getLeader()) == null) {
            LOG.info("We got elected but we do not have highest zxid, Vote: "
            + vote);
            predicate = false;
        } else if (voteMap.get(vote.getLeader()).getElectionEpoch() !=
                    vote.getElectionEpoch()) {
            LOG.info("we cannot be leader, election epoch mismatch, Vote: "
                    + vote);
            predicate = false;
        }

        return predicate;
    }

    /**
     * Helper to verify stability of the give leader with a timeout
     *
     * @param timeout
     * @param unit
     * @param leaderElectedVote
     * @return true if
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private Collection<Vote> leaderStabilityCheckLoop(
            final VoteViewChangeConsumer consumer, final int timeout,
            final TimeUnit unit, final Vote leaderElectedVote,
            final Vote selfUpdatedVote, final Collection<Vote> lastVotes)
            throws ElectionException, InterruptedException, ExecutionException {
        NotNull.check(leaderElectedVote, "leader vote is null", LOG);
        final Collection<Vote> lastVotesUpdated = getUpdatedCollection(
                lastVotes, selfUpdatedVote);
        return consumer.consume(timeout, unit,
                getLeaderStabilityPredicate(leaderElectedVote,
                        lastVotesUpdated));
    }

    /**
     * Used for leader stability check, protected for testing.
     *
     * @param leaderElectedVote
     * @return
     */
    protected LeaderStabilityPredicate getLeaderStabilityPredicate(
            final Vote leaderElectedVote, final Collection<Vote> lastVotes) {
        return new LeaderStabilityPredicate(leaderElectedVote, lastVotes);
    }



    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     * @param newVote
     * @param curVote
     * @return
     */
    private boolean totalOrderPredicate(final Vote newVote,
                                        final Vote curVote) {
        return totalOrderPredicate(newVote.getSid(),
                newVote.getZxid(), newVote.getPeerEpoch(),
                curVote.getSid(), curVote.getZxid(),
                curVote.getPeerEpoch());
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     * @param newId
     * @param newZxid
     * @param newEpoch
     * @param curId
     * @param curZxid
     * @param curEpoch
     * @return
     */
    private boolean totalOrderPredicate(long newId, long newZxid,
                                        long newEpoch, long curId,
                                        long curZxid, long curEpoch) {
        LOG.debug("totalOrderPredicate: proposed leader: " + newId + ", " +
                "leader: " + curId
                + ", proposed  zxid: 0x" + Long.toHexString(newZxid)
                + ", zxid: 0x" + Long.toHexString(curZxid)
                + ", proposed peerEpoch: 0x" + Long.toHexString(newEpoch)
                + ", peerEpoch: 0x" + Long.toHexString(curEpoch));
        if (quorumVerifier.getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */

        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) &&
                        ((newZxid > curZxid) || ((newZxid == curZxid) &&
                                (newId > curId)))));
    }

    /**
     * Get a new Immutable set with the updateVote replacing its vote in the
     * given set.
     *
     * @param votes
     * @param updateVote
     * @return
     */
    private Collection<Vote> getUpdatedCollection(final Collection<Vote> votes,
                                                  final Vote updateVote) {
        final Collection<Vote> retVotes = new ArrayList<>();
        for (final Vote vote : votes) {
            if (vote.getSid() != updateVote.getSid()) {
                retVotes.add(vote);
                continue;
            }

            retVotes.add(updateVote);
        }

        return Collections.unmodifiableCollection(retVotes);
    }

    /**
     * Returned Vote is the modified vote since we wait for VoteView to finish
     * update, since we can afford to do so here.
     *
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private Vote increaseElectionEpochAndGet() throws InterruptedException,
            ExecutionException {
        voteViewChange.increaseElectionEpoch().get();
        return getSelfVote();
    }

    /**
     * Update VoteView with the vote which we think is best.
     *
     * @param vote
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private Future<Void> updateSelfVote(final Vote vote) throws
            InterruptedException,
            ExecutionException {
        return voteViewChange.updateSelfVote(vote);
    }

    public Vote getSelfVoteFromSet(final Collection<Vote> votes) {
        Vote selfVote = null;
        for (final Vote v : votes) {
            if (v.getSid() == mySid) {
                return v;
            }
        }
        NotNull.check(selfVote, "self vote must be found in the set.", LOG);
        return null;
    }

    private Vote getSelfVote() {
        return voteViewChange.getSelfVote();
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private QuorumPeer.ServerState learningState() {
        if (learnerType == QuorumPeer.LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + getId());
            return QuorumPeer.ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + getId());
            return QuorumPeer.ServerState.OBSERVING;
        }
    }

    private Vote updateProposal(final Vote src, final Vote dst) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating proposal: " + src.getLeader()
                    + " (newleader), 0x" + Long.toHexString(src.getZxid())
                    + " (newzxid), " + dst.getLeader()
                    + " (oldleader), 0x" + Long.toHexString(dst.getZxid())
                    + "(oldzxid)" + "0x" + Long.toHexString(src.getPeerEpoch())
                    + "(newpeerepoch), 0x"
                    + Long.toHexString(dst.getPeerEpoch()));
        }

        return dst.catchUpToVote(src);
    }

    private void leaveInstance(final Vote v) {
        if (LOGS.isDebugEnabled()) {
            LOG.debug("About to leave FLE instance: leader="
                    + v.getLeader() + ", zxid=0x" +
                    Long.toHexString(v.getZxid()) + ", my id=" + getId()
                    + ", my state=" + v.getState());
        }
    }

    /**
     * Used for testing
     */
    protected QuorumVerifier getQuorumVerifier() {
        return this.quorumVerifier;
    }

    protected QuorumPeer.LearnerType getLearnerType() {
        return this.learnerType;
    }

    protected VoteViewChange getVoteViewChange() {
        return this.voteViewChange;
    }

    protected VoteViewConsumerCtrl getVoteViewConsumerCtrl() {
        return this.voteViewConsumerCtrl;
    }

    protected int getStableTimeout() {
        return this.stableTimeout;
    }

    protected TimeUnit getStableTimeUnit() {
        return this.stableTimeoutUnit;
    }
}
