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
package com.quorum;

import com.quorum.flexible.QuorumVerifier;
import com.quorum.util.LogPrefix;
import com.quorum.util.NotNull;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FastLeaderElectionV2Round {
    private final Long mySid;
    private final QuorumVerifier quorumVerifier;
    private final HashMap<Long, Vote> voteMap;
    private LogPrefix LOG = null;
    private Vote leaderVote = null;
    private HashSet<Long> leaderQuorum = null;
    private Vote selfVote = null;

    public FastLeaderElectionV2Round(final long mySid,
                                     final QuorumVerifier quorumVerifier,
                                     final Collection<Vote> votes,
                                     final LogPrefix logPrefix) {
        this.mySid = mySid;
        this.quorumVerifier = quorumVerifier;
        voteMap = new HashMap<>();
        for (final Vote vote : votes) {
            voteMap.put(vote.getSid(), vote);
        }

        selfVote = voteMap.get(mySid);
        this.LOG = logPrefix;
    }

    public FastLeaderElectionV2Round(
            final FastLeaderElectionV2Round lastFleV2Round,
            final Collection<Vote> votes) {
        this(lastFleV2Round.getId(), lastFleV2Round.quorumVerifier,
                votes, lastFleV2Round.LOG);

        // Lets save the lastFleV2Round's self vote.
        selfVote = lastFleV2Round.getSelfVote();
        voteMap.put(mySid, selfVote);
    }

    public long getId() {
        return mySid;
    }

    public Vote getLeaderVote() {
        return leaderVote;
    }

    public HashSet<Long> getLeaderQuorum() {
        return leaderQuorum;
    }

    public Vote getSelfVote() {
        return selfVote;
    }

    public HashMap<Long, Vote> getVoteMap() {
        return voteMap;
    }

    public boolean foundLeaderWithQuorum() {
        return leaderQuorum != null &&
                (quorumVerifier.containsQuorum(leaderQuorum) ||
                quorumVerifier.containsQuorumFromCount(
                        leaderVote.getElectionEpoch()));
    }

    public void lookForLeader() {
        selfVote = electionEpochUpdate(voteMap);

        // put this new vote in the vote map.
        voteMap.put(getId(), selfVote);

        final ImmutablePair<Vote, HashSet<Long>> suggestedLeaderPair =
                leaderSuggestionRound(voteMap);

        if (suggestedLeaderPair.getLeft() != null &&
                suggestedLeaderPair.getLeft().getSid() == getId()) {
            voteMap.put(getId(), suggestedLeaderPair.getLeft());
        }

        final Vote suggestedLeader = suggestedLeaderPair.getLeft();
        final HashSet<Long> suggestedLeaderQuorum
                = suggestedLeaderPair.getRight();

        this.leaderVote = suggestedLeader;
        this.leaderQuorum = suggestedLeaderQuorum;
    }

    /**
     * Go over the votes in LOOKING state and if there exists a better
     * ElectionEpoch among the votes then use it.
     * @param voteMapArg set of votes from vote view in the current round.
     * @return Self vote updated if necessary. Will borrow the best election
     * epoch.
     */
    @Deprecated
    protected Vote electionEpochRound(
            final HashMap<Long, Vote> voteMapArg) {
        // Try to set our Epoch and other fields using the LOOKING vote set.
        // we will use our vote from the set.
        NotNull.check(voteMapArg, "vote set is null", LOG);
        if (voteMapArg.isEmpty()) {
            return null;
        }

        debugPrintVotes("lookingElectionEpochConverge()", voteMapArg);

        Vote bestEpochVote = voteMapArg.get(getId());

        // Look for highest election epoch among the looking votes.
        for (final Vote vote : voteMapArg.values()) {
            if (vote.getState() != QuorumPeer.ServerState.LOOKING
                    || vote.getSid() == getId()) {
                continue;
            }

            if (vote.getElectionEpoch() > bestEpochVote.getElectionEpoch()) {
                bestEpochVote = vote;
            }
        }

        Vote selfVote = voteMapArg.get(getId());
        if (bestEpochVote.getSid() != getId()) {
            selfVote = selfVote.setElectionEpoch(bestEpochVote);
        }

        return selfVote;
    }

    /**
     * Our election epoch reflects the number of peer votes we have seen.
     * @param voteMapArg
     * @return
     */
    protected Vote electionEpochUpdate(
            final HashMap<Long, Vote> voteMapArg) {
        if (voteMapArg.get(getId()).getElectionEpoch() != voteMapArg.size()) {
            return voteMapArg.get(getId()).setElectionEpoch(voteMapArg.size());
        }

        return voteMapArg.get(getId());
    }

    /**
     * Run leader suggestion round.
     * @param voteMapArg
     * @return suggested leader could be self vote.
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected ImmutablePair<Vote, HashSet<Long>> leaderSuggestionRound(
            final HashMap<Long, Vote> voteMapArg) {
        debugPrintVotes("leaderFromView()", voteMapArg);

        final Collection<Vote> highestPeerEpochGroup =
                getHighestPeerEpoch(voteMapArg);

        final Map<Long, Vote> highestPeerEpochGroupMap = new HashMap<>();
        for (final Vote vote : highestPeerEpochGroup) {
            highestPeerEpochGroupMap.put(vote.getSid(), vote);
        }

        final ImmutablePair<Vote, HashSet<Long>> leaderElectedCountPair =
                getLeaderByCount(highestPeerEpochGroupMap);
        final Vote leaderElectedVote = leaderElectedCountPair.getLeft();

        // If no available suggested leader picked then return null.
        if (leaderElectedVote == null) {
            LOG.info("suggested leader null");
            return ImmutablePair.of(null, null);
        }

        // If we see more votes then potential leader did at this point
        // then let us try to be the leader.
        if (!quorumVerifier.containsQuorum(leaderElectedCountPair.getRight()) &&
                voteMap.get(getId()).getElectionEpoch()
                        > leaderElectedVote.getElectionEpoch()) {
            LOG.info("mySid: " + getId() + " , ElectionEpoch: "
                    + voteMap.get(getId()).getElectionEpoch()
                    + " better than: " + leaderElectedVote);
            // Let us try to be leader.
            Vote selfVote = voteMap.get(getId())
                    .catchUpToVote(leaderElectedVote);
            selfVote = selfVote.setSelfAsLeader();
            final HashSet<Long> addSelfToQuorum
                    = leaderElectedCountPair.getRight();
            addSelfToQuorum.add(getId());
            LOG.info("suggested self leader: " + selfVote);
            return ImmutablePair.of(selfVote, addSelfToQuorum);
        }

        LOG.info("suggested leader: " + leaderElectedCountPair.getLeft());
        return leaderElectedCountPair;
    }

    /**
     * Group the votes by highest PeerEpoch available.
     * <p>
     * Protected for test case use.
     *
     * @param voteMap given Vote view
     * @return votes grouped by the highest peer epoch and zxid.
     */
    protected Collection<Vote>
    getHighestPeerEpoch(final HashMap<Long, Vote> voteMap) {
        if (voteMap == null || voteMap.isEmpty()) {
            LOG.debug("votes are null or empty, cannot find leader");
            return null;
        }

        /**
         * Group by highest PeerEpoch we have in the given set.
         */
        final Collection<Vote> votesForMaxPeerEpoch = new ArrayList<>();
        long maxPeerEpoch = Long.MIN_VALUE;
        for (final Vote vote : voteMap.values()) {
            if (vote.getState() != QuorumPeer.ServerState.OBSERVING) {
                if (maxPeerEpoch < vote.getPeerEpoch()) {
                    // Reset for next max peer epoch.
                    votesForMaxPeerEpoch.clear();
                    maxPeerEpoch = vote.getPeerEpoch();
                }

                if (maxPeerEpoch == vote.getPeerEpoch()) {
                    votesForMaxPeerEpoch.add(vote);
                }
            }
        }

        return votesForMaxPeerEpoch;
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
     * Step 2, 3, and 4. ref-count for each leader. Helps us pick the best set.
     *
     * @param voteMap
     * @return LeaderVote and Sid of Votes that elected it as leader. null,
     * null otherwise.
     */
    private ImmutablePair<Vote, HashSet<Long>>
    getLeaderByCount(final Map<Long, Vote> voteMap) {
        final HashMap<Long, VoteCountSet> voteToCountMap = new HashMap<>();
        // Look at each vote count reference for each valid leader.
        long maxElectionEpoch = Long.MIN_VALUE;
        final HashSet<Vote> maxElectionEpochSet = new HashSet<>();

        for (final Vote vote : voteMap.values()) {
            // Pick a leader only if it thinks its a leader or
            // I am the one picked as leader then its ok for me to be in
            // looking state.
            if (!checkLeader(voteMap, vote)) {
                continue;
            }

            if (vote.getElectionEpoch() > maxElectionEpoch) {
                maxElectionEpoch = vote.getElectionEpoch();
                maxElectionEpochSet.clear();
                maxElectionEpochSet.add(vote);
                continue;
            }

            if (vote.getElectionEpoch() == maxElectionEpoch) {
                maxElectionEpochSet.add(vote);
            }
        }

        // Among the votes with highest election epoch ref count them.
        for (final Vote vote : maxElectionEpochSet) {
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

        for (final Vote vote : voteMap.values()) {
            if (!checkLeader(voteMap, vote)) {
                continue;
            }

            if (voteToCountMap.containsKey(vote.getLeader())) {
                voteToCountMap.get(vote.getLeader()).addVote(vote);
            }
        }

        VoteCountSet bestTotalOrder = null;
        // Among the most elected leaders pick the best.
        // There could be a case with multiple leaders have the best count,
        // in that use totalOrderPredicate() to get the best among them if
        // both of them are in LEADING state.
        final ArrayList<VoteCountSet> voteCounts
                = new ArrayList<>(voteToCountMap.values());
        Collections.sort(voteCounts, Collections.reverseOrder());
        int max = Integer.MIN_VALUE;
        for (final VoteCountSet voteCountSet : voteCounts) {
            if (max > voteCountSet.getCount()) {
                break;
            }
            max = voteCountSet.getCount();
            if (bestTotalOrder == null) {
                bestTotalOrder = voteCountSet;
                continue;
            }

            final Vote leaderVote = voteMap.get(voteCountSet.getSid());
            final Vote bestLeaderVote = voteMap.get(bestTotalOrder.getSid());

            // If a leader vote is in Leading state and current best leader
            // vote is not then use the leader vote. Otherwise use
            // totalOrderPredicate() to pick the best leader.
            if ((leaderVote.getState() == QuorumPeer.ServerState.LEADING &&
                    bestLeaderVote.getState() !=
                            QuorumPeer.ServerState.LEADING) ||
                    totalOrderPredicate(leaderVote, bestLeaderVote)) {
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
     * Given a vote the following should hold to be selected as eligible leader:
     * if (vote.electedLeader().notPresent(voteMap)) return false
     * if (!vote.electedLeaderVote().electedSelfAsLeader()) return false
     * if (vote.isFollower() && !vote.electedLeaderVote().isLeaderWithState()
     * return false
     * else
     *  return true.
     * @param voteMap
     * @param vote
     * @return
     */
    protected boolean checkLeader(
            final Map<Long, Vote> voteMap,
            final Vote vote) {
        return voteMap.containsKey(vote.getLeader()) &&
                (voteMap.get(vote.getLeader()).electedSelfAsLeader() ||
                        (vote.isFollower() &&
                                voteMap.get(vote.getLeader())
                                        .isLeader()));
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
    @Deprecated
    protected boolean checkLeaderOld(
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignore non existent leader, Vote: " + vote);
                }
                predicate = false;
            } else if (voteMap.get(vote.getLeader()).getLeader() !=
                    voteMap.get(vote.getLeader()).getSid() ||
                    voteMap.get(vote.getLeader()).getState()
                            != QuorumPeer.ServerState.LEADING) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignore leader that did not elect itself, Vote: "
                            + vote + " leader vote: "
                            + voteMap.get(vote.getLeader()));
                }
                predicate = false;
            }
        } else if (voteMap.get(getId()).getElectionEpoch()
                != vote.getElectionEpoch()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("we cannot be leader, election epoch mismatch, Vote: "
                        + vote);
            }
            predicate = false;
        }

        return predicate;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("totalOrderPredicate: proposed leader: " + newId + ", " +
                    "leader: " + curId
                    + ", proposed  zxid: 0x" + Long.toHexString(newZxid)
                    + ", zxid: 0x" + Long.toHexString(curZxid)
                    + ", proposed peerEpoch: 0x" + Long.toHexString(newEpoch)
                    + ", peerEpoch: 0x" + Long.toHexString(curEpoch));
        }
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

    private void debugPrintVotes(final String str,
                            final HashMap<Long, Vote> voteMapArg) {
        if (LOG.isDebugEnabled()) {
            for (final Vote v : voteMapArg.values()) {
                LOG.info(str + ": " + v);
            }
        }
    }
}
