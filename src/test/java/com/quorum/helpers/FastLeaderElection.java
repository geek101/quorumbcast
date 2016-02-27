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


package com.quorum.helpers;

import com.quorum.Election;
import com.quorum.ElectionException;
import com.quorum.QuorumPeer;
import com.quorum.Vote;
import com.quorum.VoteViewChange;
import com.quorum.VoteViewConsumerCtrl;
import com.quorum.VoteViewStreamConsumer;
import com.quorum.flexible.QuorumVerifier;
import com.quorum.util.LogPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {
    private static final Logger LOGS
            = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    final static int maxNotificationInterval = 60000;
    
    long logicalclock; /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;
    volatile boolean stop;
    long start_fle;
    protected LogPrefix LOG = null;

    private final long mySid;
    private final QuorumPeer.LearnerType learnerType;
    private final QuorumVerifier quorumVerifier;
    private final VoteViewConsumerCtrl voteViewConsumerCtrl;
    private final VoteViewChange voteViewChange;

    // Test Helpers
    protected AtomicReference<CompletableFuture<Collection<Vote>>>
            waitForLookRunFuture
            = new AtomicReference<>();
    protected HashMap<Long, Vote> votesUptoMap = new HashMap<>();
    protected Object lastLookForLeader = null;
    protected Object suggestedForTermination = null;

    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock(){
        return logicalclock;
    }


    public FastLeaderElection(final long mySid, 
                              final QuorumPeer.LearnerType learnerType,
                              final QuorumVerifier quorumVerifier,
                              VoteViewConsumerCtrl voteViewConsumerCtrl,
                              VoteViewChange voteViewChange) {
        this.mySid = mySid;
        this.learnerType = learnerType;
        this.quorumVerifier = quorumVerifier;
        this.voteViewConsumerCtrl = voteViewConsumerCtrl;
        this.voteViewChange = voteViewChange;
        this.stop = false;
        this.logicalclock = 0;
        this.proposedLeader = -1;
        this.proposedZxid = -1;
        this.LOG = new LogPrefix(LOGS, "mySid:" + this.mySid +
                "-electionEpoch:0");
        waitForLookRunFuture.set(
                CompletableFuture.completedFuture((Collection<Vote>)null));
    }

    private void leaveInstance(Vote v) throws InterruptedException,
            ExecutionException {
        voteViewChange.updateSelfVote(v);
        if(LOG.isDebugEnabled()){
            LOG.debug("About to leave FLE instance: leader="
                + v.getLeader() + ", zxid=0x" +
                Long.toHexString(v.getZxid()) + ", my id=" + getId()
                + ", my state=" + v.getState());
        }
    }

    public long getId() {
        return mySid;
    }
    
    public QuorumPeer.LearnerType getLearnerType() {
        return learnerType;
    }

    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }
    
    public void shutdown(){
        stop = true;
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() throws InterruptedException, 
            ExecutionException{
        final Vote selfVote = new Vote(proposedLeader, proposedZxid,
                logicalclock, proposedEpoch, getId(), 
                QuorumPeer.ServerState.LOOKING);
        voteViewChange.updateSelfVote(selfVote);
        
        if(LOG.isDebugEnabled()){

            LOG.debug("Sending Notification: " + proposedLeader 
                    + " (n.leader), 0x"  + Long.toHexString(proposedZxid)
                    + " (n.zxid), 0x" + Long.toHexString(logicalclock)  +
                      " (n.round), " + getId() +
                      " (myid), 0x" + Long.toHexString(proposedEpoch) 
                    + " (n.peerEpoch)");
        }
    }


    private void printNotification(final Vote vote){
        LOG.info("Notification: " + vote);
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     * @param newId
     * @param newZxid
     * @param newEpoch
     * @param curId
     * @param curZxid
     * @param curEpoch
     * @return
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, 
                                          long newEpoch, long curId, 
                                          long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                Long.toHexString(newZxid) + ", proposed zxid: 0x" 
                + Long.toHexString(curZxid));
        if(getQuorumVerifier().getWeight(newId) == 0){
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
                ((newZxid > curZxid) || ((newZxid == curZxid) 
                        && (newId > curId)))));
    }

    /**
     * Termination predicate. Given a set of votes, determines if
     * have sufficient to declare the end of the election round.
     *
     *  @param votes    Set of votes
     *  @param vote Current vote
     */
    protected boolean termPredicate(
            HashMap<Long, Vote> votes,
            Vote vote) {

        HashSet<Long> set = new HashSet<Long>();

        /*
         * First make the views consistent. Sometimes peers will have
         * different zxids for a server depending on timing.
         */
        for (Map.Entry<Long,Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())){
                set.add(entry.getKey());
            }
        }

        return getQuorumVerifier().containsQuorum(set);
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(
            HashMap<Long, Vote> votes,
            long leader,
            long electionEpoch){

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if(leader != getId()){
            if(votes.get(leader) == null) predicate = false;
            else if(votes.get(leader).getState() 
                    != QuorumPeer.ServerState.LEADING) predicate = false;
        } else if(logicalclock != electionEpoch) {
            predicate = false;
        } 

        return predicate;
    }
    
    /**
     * This predicate checks that a leader has been elected. It doesn't
     * make a lot of sense without context (check lookForLeader) and it
     * has been separated for testing purposes.
     * 
     * @param recv  map of received votes 
     * @param ooe   map containing out of election votes (LEADING or FOLLOWING)
     * @param v    vote
     * @return          
     */
    protected boolean ooePredicate(HashMap<Long,Vote> recv, 
                                    HashMap<Long,Vote> ooe, 
                                    final Vote v) {
        
        return termPredicate(recv, v)
                && checkLeader(ooe, v.getLeader(), v.getElectionEpoch());
        
    }

    void updateProposal(long leader, long zxid, long epoch) {
        if(LOG.isDebugEnabled()){
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x"
                    + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                    + " (oldleader), 0x" + Long.toHexString(proposedZxid) 
                    + " (oldzxid)");
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
        this.LOG = new LogPrefix(LOGS, "mySid:" + this.mySid +
                "-electionEpoch: 0x" + Long.toHexString(logicalclock));
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private QuorumPeer.ServerState learningState(){
        if(getLearnerType() == QuorumPeer.LearnerType.PARTICIPANT){
            LOG.debug("I'm a participant: " + getId());
            return QuorumPeer.ServerState.FOLLOWING;
        }
        else{
            LOG.debug("I'm an observer: " + getId());
            return QuorumPeer.ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId(){
        if(getLearnerType() == QuorumPeer.LearnerType.PARTICIPANT)
            return getId();
        else return Long.MIN_VALUE;
    }
    
    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader(final long peerEpochInit, final long zxidInit)
            throws ElectionException, InterruptedException, ExecutionException {
        if (start_fle == 0) {
            start_fle = System.currentTimeMillis();
        }

        HashMap<Long, Vote> recvset = new HashMap<>();

        HashMap<Long, Vote> outofelection = new HashMap<>();

        int notTimeout = finalizeWait;

        logicalclock++;
        updateProposal(getInitId(), zxidInit, peerEpochInit);

        LOG.info("New election. My id =  " + getId() +
                ", proposed zxid=0x" + Long.toHexString(proposedZxid));
        sendNotifications();

        final VoteViewStreamConsumer consumer = voteViewConsumerCtrl
                .createStreamConsumer();
            /*
             * Loop in which we exchange notifications until we find a leader
             */

        int count = 0;
        while (!stop) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
            final Collection<Vote> votes = consumer.consume(notTimeout,
                    TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
            if (votes == null) {
                    /*
                     * Exponential backoff
                     */
                int tmpTimeOut = notTimeout * 2;
                notTimeout = (tmpTimeOut < maxNotificationInterval ?
                        tmpTimeOut : maxNotificationInterval);
                LOG.info("Notification time out: " + notTimeout);
                continue;
            }

            assert !votes.isEmpty();

            lastLookForLeader = votes;
            setVotesInRun(votes);

            debugPrintVotes("lookForLeader(): " + ++count, votes);

            for (final Vote v : votes) {
                if (v.isRemove()) {
                    continue;
                }
                switch (v.getState()) {
                    case LOOKING:
                        // If notification > current, replace and send messages out
                        if (v.getElectionEpoch() > logicalclock) {
                            logicalclock = v.getElectionEpoch();
                            recvset.clear();
                            if (totalOrderPredicate(v.getLeader(), v.getZxid(),
                                    v.getPeerEpoch(), getInitId(), zxidInit,
                                    peerEpochInit)) {
                                updateProposal(v.getLeader(), v.getZxid(),
                                        v.getPeerEpoch());
                            } else {
                                updateProposal(getInitId(), zxidInit,
                                        peerEpochInit);
                            }
                            sendNotifications();
                        } else if (v.getElectionEpoch() < logicalclock) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Notification election epoch is " +
                                        "smaller than logicalclock. " +
                                        "electionEpoch() = 0x"
                                        + Long.toHexString(v.getElectionEpoch())
                                        + ", logicalclock=0x"
                                        + Long.toHexString(logicalclock));
                            }
                            break;
                        } else if (totalOrderPredicate(v.getLeader(),
                                v.getZxid(), v.getPeerEpoch(),
                                proposedLeader, proposedZxid, proposedEpoch)) {
                            updateProposal(v.getLeader(), v.getZxid(),
                                    v.getPeerEpoch());
                            sendNotifications();
                        }

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Adding vote: from=" + v.getSid() +
                                    ", proposed leader=" + v.getLeader() +
                                    ", proposed zxid=0x"
                                    + Long.toHexString(v.getZxid()) +
                                    ", proposed election epoch=0x"
                                    + Long.toHexString(v.getElectionEpoch()));
                        }

                        recvset.put(v.getSid(), v);

                        if (termPredicate(recvset,
                                new Vote(proposedLeader, proposedZxid,
                                        logicalclock, proposedEpoch, getId()))) {

                            suggestedForTermination = recvset;

                            // Verify if there is any change in the proposed leader
                            Collection<Vote> votesStability = null;
                            while ((votesStability = consumer.consume(
                                    finalizeWait, TimeUnit.MILLISECONDS))
                                    != null) {
                                setVotesInRun(votesStability);
                                debugPrintVotes("stabilityWait",
                                        votesStability);
                                boolean brokeStability = false;

                                for (final Vote voteStability
                                        : votesStability) {
                                    if (totalOrderPredicate(
                                            voteStability.getLeader(),
                                            voteStability.getZxid(),
                                            voteStability.getPeerEpoch(),
                                            proposedLeader, proposedZxid,
                                            proposedEpoch)) {
                                        brokeStability = true;
                                    }
                                }

                                if (brokeStability) {
                                    break;
                                }
                            }

                            /*
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             */
                            if (votesStability == null) {
                                final QuorumPeer.ServerState exitState
                                        = (proposedLeader == getId()) ?
                                        QuorumPeer.ServerState.LEADING
                                        : learningState();

                                Vote endVote = new Vote(proposedLeader,
                                        proposedZxid,
                                        logicalclock,
                                        proposedEpoch,
                                        getId(),
                                        exitState);
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }  // if termPredicate()
                        break;
                    case OBSERVING:
                        LOG.debug("Notification from observer: " + v.getSid());
                        break;
                    case FOLLOWING:
                    case LEADING:
                        /*
                         * Consider all notifications from the same epoch
                         * together.
                         */
                        if (v.getElectionEpoch() == logicalclock) {
                            recvset.put(v.getSid(), v);

                            suggestedForTermination = recvset;

                            if (ooePredicate(recvset, outofelection, v)) {
                                final QuorumPeer.ServerState exitState
                                        = (v.getLeader() == getId()) ?
                                        QuorumPeer.ServerState.LEADING
                                        : learningState();

                                Vote endVote = new Vote(v.getLeader(),
                                        v.getZxid(),
                                        v.getElectionEpoch(),
                                        v.getPeerEpoch(),
                                        getId(),
                                        exitState);
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }

                        /*
                         * Before joining an established ensemble, verify
                         * a majority is following the same leader.
                         */
                        outofelection.put(v.getSid(), v);

                        suggestedForTermination = outofelection;

                        if (ooePredicate(outofelection, outofelection, v)) {
                            logicalclock = v.getElectionEpoch();
                            final QuorumPeer.ServerState exitState
                                    = (v.getLeader() == getId()) ?
                                    QuorumPeer.ServerState.LEADING
                                    : learningState();

                            Vote endVote = new Vote(v.getLeader(),
                                    v.getZxid(),
                                    v.getElectionEpoch(),
                                    v.getPeerEpoch(),
                                    getId(),
                                    exitState);
                            leaveInstance(endVote);
                            return endVote;
                        }
                        break;
                    default:
                        final String errStr = "Notification state " +
                                "unrecognized: " + v.getState() +
                                "(v.getState()), " + v.getSid() + " (n.sid)";
                        LOG.error(errStr);
                        throw new RuntimeException(errStr);
                }
            }  // for (final Vote v : votes)
        }  // while(!stop)
        return null;
    }

    // Test helpers
    protected Object couldTerminate() {
        return suggestedForTermination;
    }

    protected Object getLastLookForLeader() {
        return lastLookForLeader;
    }

    protected void setVotesInRun(final Collection<Vote> votes) {
        for (final Vote v : votes) {
            votesUptoMap.put(v.getSid(), v);
        }
        // Used for testing, helps with waiting till all Votes were
        // considered.
        // Terminate the unterminated on.
        final CompletableFuture<Collection<Vote>> lastVotesFuture =
                new CompletableFuture<>();
        lastVotesFuture.complete(Collections.unmodifiableCollection
                (votesUptoMap.values()));
        waitForLookRunFuture.set(lastVotesFuture);
    }

    /**
     * Used for testing, we need to only exit after given votes were
     * considered for a run.
     * @param voteMap given votes much match what were processed in the run.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected void waitForVotesRun(final Map<Long, Vote> voteMap)
            throws InterruptedException, ExecutionException {
        LOG.warn("entering wait for votes run count: " + voteMap.size());
        while(true) {
            final Collection<Vote> votesInRun
                    = waitForLookRunFuture.get().get();
            if (votesInRun == null || voteMap.size() != votesInRun.size()) {
                if (votesInRun != null) {
                    final String errStr = "wait for votes non success count: "
                            + votesInRun.size() + ", expected: "
                            + voteMap.size();
                    if (votesInRun.size() > voteMap.size()) {
                        throw new RuntimeException(errStr);
                    }
                }
                continue;
            }

            boolean missedAVote = false;
            for (final Vote v : votesInRun) {
                // TODO: can we match the entire Vote?, but
                // since vote is update it is hard to do
                // from outside?. May be not?.
                if (!voteMap.containsKey(v.getSid())) {
                    missedAVote = true;
                    break;
                }
            }

            if (!missedAVote) {
                return;
            }
        }
    }

    private void debugPrintVotes(final String str,
                                 final Collection<Vote> votes) {
        if (LOG.isDebugEnabled()) {
            for (final Vote v : votes) {
                LOG.info(str + ": " + v);
            }
        }
    }
}
