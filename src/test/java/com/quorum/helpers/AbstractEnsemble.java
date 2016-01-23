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

import com.quorum.ElectionException;
import com.quorum.QuorumPeer;
import com.quorum.Vote;
import com.quorum.flexible.QuorumVerifier;
import com.quorum.util.LogPrefix;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * For use with unit tests
 */
public abstract class AbstractEnsemble implements Ensemble {
    private static final Logger LOGS
            = LoggerFactory.getLogger(AbstractEnsemble.class);
    private final Long id;                /// Unique id for the Ensemble
    enum EnsembleState {
        INVALID, INITIAL, CONFIGURED, RUN, FINAL
    }

    private final Integer quorumSize;     /// Size of the ensemble.
    private final QuorumVerifier quorumVerifier;     /// Done with size.

    protected QuorumCnxMesh quorumCnxMesh;  /// Connectivity mesh based on
                                           // configure string
    protected Collection<Collection<ImmutablePair<Long, QuorumPeer
                                                   .ServerState>>>
                                                   partitionedQuorum
                                                   = new ArrayList<>();
    /**
     * Current Fles with Vote set for each.
     */
    protected HashMap<Long, FLEV2Wrapper> fles;
    private EnsembleState state;    /// State of the ensemble.

    private final Ensemble parent;
    private final Collection<Ensemble> children = new ArrayList<>();

    protected final int stableTimeout;
    protected final TimeUnit stableTimeoutUnit;

    // stage if any.
    private FLEV2Wrapper fleToRun;  /// Which node is set to run
    protected ConcurrentHashMap<Long, Future<Vote>> futuresForLookingPeers
            = new ConcurrentHashMap<>();
    protected HashMap<Long, Vote> lookingResultVotes = new HashMap<>();
    private boolean runLookingDone = false;
    private ImmutablePair<Long, Long> safteyPred;

    protected LogPrefix LOG = null;
    final long rgenseed = System.currentTimeMillis();
    final Random random = new Random(rgenseed);

    protected AbstractEnsemble(final long id, final int quorumSize,
                               final QuorumVerifier quorumVerifier,
                               final EnsembleState pastState,
                               final Ensemble parent,
                               final int stableTimeout,
                               final TimeUnit stableTimeoutUnit) {
        this.id = id;
        this.quorumSize = quorumSize;
        this.quorumVerifier =  quorumVerifier;
        if (pastState == EnsembleState.INVALID) {
            this.state = EnsembleState.INITIAL;
        } else if (pastState == EnsembleState.INITIAL) {
            this.state = EnsembleState.CONFIGURED;
        } else if (pastState == EnsembleState.CONFIGURED) {
            this.state = EnsembleState.RUN;
        } else if (pastState == EnsembleState.RUN) {
            this.state = EnsembleState.FINAL;
        }
        if (parent == null) {
            this.parent = this;
        } else {
            this.parent = parent;
        }
        this.stableTimeout = stableTimeout;
        this.stableTimeoutUnit = stableTimeoutUnit;
    }

    public AbstractEnsemble(final long id, final int quorumSize,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit)
            throws ElectionException {
        this(id, quorumSize, new QuorumMajWrapper(quorumSize),
                EnsembleState.INVALID, null, stableTimeout, stableTimeoutUnit);
        this.fles = new HashMap<>();

        for (final FLEV2Wrapper fle : getQuorumWithInitVoteSet(
                this.quorumSize, this.quorumVerifier)) {
            this.fles.put(fle.getId(), fle);
        }
        this.LOG = new LogPrefix(LOGS, toString());
    }

    protected AbstractEnsemble(final Ensemble parentEnsemble,
                               final int stableTimeout,
                               final TimeUnit stableTimeoutUnit) {
        this(((AbstractEnsemble)parentEnsemble).getId() + 1,
                parentEnsemble.getQuorumSize(),
                ((AbstractEnsemble)parentEnsemble).getQuorumVerifier(),
                ((AbstractEnsemble)parentEnsemble).getState(),
                parentEnsemble, stableTimeout, stableTimeoutUnit);
        this.fles = ((AbstractEnsemble)parentEnsemble).fles;
        this.quorumCnxMesh = parentEnsemble.getQuorumCnxMesh();
        this.partitionedQuorum = ((AbstractEnsemble) parentEnsemble)
                .partitionedQuorum;
        this.LOG = new LogPrefix(LOGS, toString());
    }

    public long getId() {
        return id;
    }

    @Override
    public int getQuorumSize() {
        return quorumSize;
    }

    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }
    public EnsembleState getState() {
        return state;
    }

    @Override
    public FLEV2Wrapper getFleToRun() {
        return this.fleToRun;
    }

    @Override
    public QuorumCnxMesh getQuorumCnxMesh() {
        return this.quorumCnxMesh;
    }

    @Override
    public String toString() {
        if (partitionedQuorum.size() == 0) {
            return "myId:" + getId() + "-" +"{"
                    + quorumSize.toString() + "}";
        }

        final List<String> stringList = new ArrayList<>();
        for (final Collection<ImmutablePair<Long, QuorumPeer.ServerState>> t
                : partitionedQuorum) {
            final TreeMap<Long, Long> sidMap = new TreeMap<>();
            for (ImmutablePair<Long, QuorumPeer.ServerState> e : t) {
                sidMap.put(e.getLeft(), e.getLeft());
            }

            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>> res =
                    new ArrayList<>();
            for (final long sid : sidMap.keySet()) {
                res.add(ImmutablePair.of(sid, fles.get(sid).getState()));
            }
            stringList.add(
                    EnsembleHelpers.getQuorumServerStateCollectionStr(res));
        }

        if (stringList.size() == 1) {
            return "myId:" + getId() + "-" + stringList.get(0);
        }

        String partitionStr = "{" + stringList.get(0);
        for (int i = 1; i < stringList.size(); i++) {
            partitionStr += ", " + stringList.get(i);
        }
        partitionStr += "}";

        return "myId:" + getId() + "-" + partitionStr;
    }

    /**
     * Override this in the implementation to spin up the right type of
     * ensemble using the parent.
     * @param parentEnsemble
     * @return
     */
    public abstract Ensemble createEnsemble(
            final Ensemble parentEnsemble,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
            quorumWithState) throws ElectionException;

    protected abstract FLEV2Wrapper createFLEV2(
            final long sid, final QuorumVerifier quorumVerifier);

    protected abstract FLEV2Wrapper copyFLEV2(
            final FLEV2Wrapper fle, final Vote vote)
            throws InterruptedException, ExecutionException;

    /**
     * If all are looking then any one can be leader but everyone
     * should agree on it regardless. Otherwise existing LEADER must
     * be elected.
     * @return
     */
    @Override
    public void verifyLeaderAfterShutDown()
            throws InterruptedException, ExecutionException {
        final HashMap<Long, Vote> resultVotes = getLeaderLoopResult();

        // Above will wait for leader elections to finish.
        shutdown().get();

        final HashMap<Long, HashSet<Long>> leaderQuorumMap = new HashMap<>();

        for (final FLEV2Wrapper fle : fles.values()) {
            if (!leaderQuorumMap.containsKey(fle.getSelfVote().getLeader())) {
                leaderQuorumMap.put(fle.getSelfVote().getLeader(),
                        new HashSet<>(
                        Collections.singletonList(fle.getId())));
            } else {
                leaderQuorumMap.get(fle.getSelfVote().getLeader()).add(fle.getId());
            }
        }

        verifyThisAsLeader(leaderQuorumMap);
    }

    public void verifyThisAsLeader(
            final HashMap<Long, HashSet<Long>> leaderQuorumMap) {
        int max = Integer.MIN_VALUE;
        long secondBestLeaderSid = Integer.MIN_VALUE;
        long bestLeaderSid = Integer.MIN_VALUE;

        for (final Map.Entry<Long, HashSet<Long>> entry
                : leaderQuorumMap.entrySet()) {
            if (entry.getValue().size() > max) {
                max = entry.getValue().size();
                secondBestLeaderSid = bestLeaderSid;
                bestLeaderSid = entry.getKey();
            }
        }

        if (!quorumVerifier.containsQuorum(leaderQuorumMap.get(bestLeaderSid))) {
            final String errStr = "Desired leader: " + bestLeaderSid
                    + " has no quorum: "
                    + leaderQuorumMap.get(bestLeaderSid).size();
            LOG.error(errStr);

            assertTrue(errStr, false);
        }

        // Verify safety of votes for the quorum that is moving forward
        for (final Long sid : leaderQuorumMap.get(bestLeaderSid)) {
            verifySafetyPredicate(fles.get(sid).getSelfVote());
        }

        /**
         * This needs to be printed after shutting down?
         */
        if (secondBestLeaderSid != Integer.MIN_VALUE) {
            final String errStr = "Got second best: " + secondBestLeaderSid
                    + " with minority quorum: " + leaderQuorumMap.get
                    (secondBestLeaderSid)
                    + " best leader: " + bestLeaderSid + " with " +
                    "majority quorum: " + leaderQuorumMap.get(bestLeaderSid);
            LOG.warn(errStr);
            printAllVotes();
        }
    }

    private void verifySafetyPredicate(final HashMap<Long, Vote> voteMap) {
        for (final Vote v : voteMap.values()) {
            verifySafetyPredicate(v);
        }
    }

    private void verifySafetyPredicate(final Vote vote) {
        assertTrue("vote not null", vote != null);
        if (vote.getPeerEpoch() != safteyPred.getLeft() ||
                vote.getZxid() != safteyPred.getRight()) {
            final String errStr = "leader vote : " + vote + " failed"
                    + " saftey check peerEpoch: 0x" + Long.toHexString
                    (safteyPred.getLeft()) + ", Zxid: 0x"
                    + Long.toHexString(safteyPred.getRight());
            assertEquals(errStr, safteyPred.getLeft().longValue(),
                    vote.getPeerEpoch());
            assertEquals(errStr, safteyPred.getRight().longValue(),
                    vote.getZxid());
        }
    }

    public void printAllVotes() {
        for (final FLEV2Wrapper fle : fles.values()) {
            LOG.error(fle.getSelfVote().toString());
        }
    }

    public static boolean verifyNullLeader(final Collection<Vote> votes) {
        for (final Vote v : votes) {
            assertEquals("leader is null", null, v);
        }
        return true;
    }

    public static boolean verifyLeader(final Vote v,
                                       final long leaderSid) {
        assertEquals("leader is " + v.getLeader() + " for " + v.getSid(),
                leaderSid, v.getLeader());
        return true;
    }

    @Override
    public HashMap<Long, Vote> getLeaderLoopResult() {
        // if already called once return stored result.
        if (!lookingResultVotes.isEmpty()) {
            return lookingResultVotes;
        }
        // wait for everyone to finish before waiting the result of
        // the peer we are interested in.
        for (final Map.Entry<Long,
                Future<Vote>> entry : futuresForLookingPeers.entrySet()) {
            try {
                lookingResultVotes.put(entry.getKey(),
                        entry.getValue().get());
            } catch (InterruptedException | ExecutionException exp) {
                LOG.error("failed to wait for result of rest of the peers");
                throw new RuntimeException(exp);
            }
        }
        return lookingResultVotes;
    }

    @Override
    public Future<?> shutdown() {
        shutdown(fles.values());
        return CompletableFuture.completedFuture(null);
    }

    public ImmutablePair<Long, QuorumPeer.ServerState>
    parseHostStateString(final String nodeStr,
                         final HashMap<Long,
                                 ImmutablePair<Long,
                                         QuorumPeer.ServerState>> result) {
        if (nodeStr.length() < 2) {
            return ImmutablePair.of(Long.MIN_VALUE, null);
        }
        final char nodeState = nodeStr.charAt(nodeStr.length()-1);
        final QuorumPeer.ServerState serverState
                = EnsembleHelpers.getServerState(nodeState);
        final long nodeId = Long.valueOf(
                nodeStr.substring(0, nodeStr.length()-1));
        result.put(nodeId, ImmutablePair.of(nodeId, serverState));
        return ImmutablePair.of(nodeId, serverState);
    }

    public int configureParser(final String quorumStr, int idx,
                               final HashSet<Long> sidMap,
                               final HashMap<Long,
                                       ImmutablePair<Long,
                                               QuorumPeer.ServerState>>
                                       flatResult,
                               final Collection<
                                       Collection<ImmutablePair<Long,
                                               QuorumPeer.ServerState>>>
                                       partitionResult) {
        if (idx == quorumStr.length()) return idx;

        Collection<ImmutablePair<Long,
                QuorumPeer.ServerState>> currentPartitionResult
                = new ArrayList<>();
        String nodeStr = "";
        for (int i = idx; i < quorumStr.length(); i++) {
            if (quorumStr.charAt(idx) == '{') {
                i = configureParser(quorumStr, i+1, new HashSet<>(),
                        flatResult, partitionResult);
            } else if (quorumStr.charAt(i) == '}') {
                final ImmutablePair<Long, QuorumPeer.ServerState> pair
                        = parseHostStateString(nodeStr, flatResult);
                if (pair.getLeft() != Long.MIN_VALUE) {
                    currentPartitionResult.add(pair);
                    sidMap.add(pair.getLeft());
                }

                for (final long sidSrc : sidMap) {
                    for (final long sidDst : sidMap) {
                        quorumCnxMesh.connect(sidSrc, sidDst);
                    }
                }
                partitionResult.add(currentPartitionResult);
                return i + 1;
            } else if (quorumStr.charAt(i) == ',') {
                final ImmutablePair<Long, QuorumPeer.ServerState> pair  =
                        parseHostStateString(nodeStr, flatResult);
                if (pair.getLeft() != Long.MIN_VALUE) {
                    currentPartitionResult.add(pair);
                    sidMap.add(pair.getLeft());
                }

                nodeStr = "";
                continue;
            } else if (quorumStr.charAt(i) == ' ') {
                continue;
            }

            if (i < quorumStr.length()) {
                nodeStr += quorumStr.charAt(i);
            }
        }

        return quorumStr.length();
    }

    /**
     * example input: {{1K, 2F}, {2F, 3L}}
     * The above example signifies network partition. When there is no
     * partition it would look like this {1L, 2F, 3L}
     * @param quorumStr
     * @return
     */
    public Ensemble configure(final String quorumStr) throws ElectionException,
            ExecutionException, InterruptedException {
        quorumCnxMesh = new QuorumCnxMeshBase(quorumSize);
        final HashMap<Long, ImmutablePair<Long, QuorumPeer.ServerState>>
                flatQuorum = new HashMap<>();

        configureParser(quorumStr, 0, null, flatQuorum, partitionedQuorum);

        // de-dup flatQuorum
        return configureAfterMesh(flatQuorum.values());
    }

    /**
     * Given an Ensemble with INIT state configure to requested quorum.
     * @param quorumWithState
     * @return Ensemble as per the requested quorum.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Ensemble configure(final Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>> quorumWithState)
            throws ElectionException, ExecutionException, InterruptedException {
        quorumCnxMesh = new QuorumCnxMeshBase(quorumSize);
        for (final ImmutablePair<Long, QuorumPeer.ServerState> p
                : quorumWithState) {
            for (final ImmutablePair<Long, QuorumPeer.ServerState> q
                    : quorumWithState) {
                quorumCnxMesh.connect(p.getLeft(), q.getLeft());
            }
        }

        return configureAfterMesh(quorumWithState);
    }

    public Ensemble configureAfterMesh(final Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>> quorumWithState)
            throws ElectionException, ExecutionException, InterruptedException {
        if (getState() != EnsembleState.INITIAL) {
            throw new IllegalAccessError("Ensemble: " + id + " not in " +
                    "init state.");
        }

        final AbstractEnsemble childEnsemble
                = (AbstractEnsemble) createEnsemble(this, quorumWithState);
        childEnsemble.configureFromParent(quorumWithState);
        childEnsemble.verifyLookingVotes();
        this.children.add(childEnsemble);
        return childEnsemble;
    }

    public Collection<Ensemble> moveToLookingForAll()
            throws ElectionException, InterruptedException, ExecutionException {
        final Collection<Ensemble> ensembles = new ArrayList<>();
        for (final FLEV2Wrapper fle : fles.values()) {
            ensembles.add(moveToLooking(fle.getId()));
        }
        return ensembles;
    }

    public Ensemble moveToLooking(final long sid)
            throws ElectionException, InterruptedException, ExecutionException {
        if (getState() != EnsembleState.CONFIGURED) {
            throw new IllegalAccessError("Ensemble: " + id + " not in " +
                    "configured state.");
        }

        if (!fles.containsKey(sid)) {
            throw new IllegalArgumentException("sid not found for: " + sid);
        }

        // Which one to set?
        final FLEV2Wrapper fle = fles.get(sid);

        // If this is a leader fle and it is going to LOOKING state then
        // Everyone must be moved to looking state.
        if (fle.getState() == QuorumPeer.ServerState.LEADING) {
            return setAllToLooking(fle.getId());
        }

        // If this is a follower then check if moving this to looking
        // breaks the learner quorum
        if (fle.getState() == QuorumPeer.ServerState.FOLLOWING) {
            final HashSet<Long> learnerSet = getLearnerQuorum();
            learnerSet.remove(fle.getId());
            if (!getQuorumVerifier().containsQuorum(learnerSet)) {
                // no quorum after so reset all to looking.
                return setAllToLooking(fle.getId());
            }
        }

        // Here means just set this to looking and be done with.
        return setOneToLooking(fle);
    }

    public Ensemble runLooking()
            throws InterruptedException, ExecutionException, ElectionException {
        if (getState() != EnsembleState.RUN) {
            throw new IllegalAccessError("Ensemble: " + id + " not in " +
                    "run state.");
        }

        if (!fles.containsKey(this.fleToRun.getId())) {
            throw new IllegalArgumentException("sid not found for: " +
                    this.fleToRun.getId());
        }

        LOG.info("running looking for sid: " + this.fleToRun.getId());
        // Which one to set?
        final FLEV2Wrapper fle = fles.get(this.fleToRun.getId());
        assert this.fleToRun == fle;
        if (fle.getState() != QuorumPeer.ServerState.LOOKING) {
            throw new IllegalArgumentException("sid not in LOOKING state: "
                    + fleToRun.getId());
        }

        final AbstractEnsemble childEnsemble = createEnsembleFromParent(this);
        childEnsemble.verifyLookingVotes();
        childEnsemble.runLookingForSid(this.fleToRun.getId());
        return childEnsemble;
    }

    protected void runLookingForSid(final long sid)
            throws InterruptedException, ExecutionException, ElectionException {
        if (runLookingDone) {
            throw new IllegalAccessError("Ensemble: " + getId()
                    + " already has leader loop result");
        }

        ImmutablePair<Long, Long> bestPeerEpochAndZxid = null;
        for (final FLEV2Wrapper fle : fles.values()) {
            if (bestPeerEpochAndZxid == null ||
                    (fle.getSelfVote().getPeerEpoch() > bestPeerEpochAndZxid
                            .getLeft() ||
                            (fle.getSelfVote().getPeerEpoch() ==
                                    bestPeerEpochAndZxid.getLeft() &&
                                    fle.getSelfVote().getZxid() >
                                            bestPeerEpochAndZxid.getRight()))) {
                bestPeerEpochAndZxid = ImmutablePair.of(fle.getSelfVote()
                        .getPeerEpoch(), fle.getSelfVote().getZxid());
            }
        }

        safteyPred = bestPeerEpochAndZxid;
        runLookingDone = true;
    }

    private AbstractEnsemble setAllToLooking(final long sid)
            throws ElectionException, InterruptedException, ExecutionException {
        final AbstractEnsemble childEnsemble = createEnsembleFromParent(this);
        childEnsemble.setFleToRun(sid);
        for (final FLEV2Wrapper fle : childEnsemble.fles.values()) {
            fle.updateSelfVote(fle.getSelfVote()
                    .setServerState(QuorumPeer.ServerState.LOOKING)).get();
        }
        childEnsemble.LOG = this.LOG = new LogPrefix(LOGS, toString());
        childEnsemble.verifyLookingVotes();
        this.children.add(childEnsemble);
        return childEnsemble;
    }

    private AbstractEnsemble setOneToLooking(final FLEV2Wrapper fle)
            throws ElectionException, InterruptedException, ExecutionException {
        final AbstractEnsemble childEnsemble = createEnsembleFromParent(this);
        childEnsemble.setFleToRun(fle.getId());
        childEnsemble.getFleToRun()
                .updateSelfVote(fle.getSelfVote().breakFromLeader()).get();
        childEnsemble.LOG = this.LOG = new LogPrefix(LOGS, toString());
        childEnsemble.verifyLookingVotes();
        this.children.add(childEnsemble);
        return childEnsemble;
    }

    private AbstractEnsemble createEnsembleFromParent(
            final AbstractEnsemble ensemble) throws ElectionException,
            InterruptedException, ExecutionException {
        final AbstractEnsemble childEnsemble
                = (AbstractEnsemble)createEnsemble(ensemble, null);
        childEnsemble.copyFromParent();
        return childEnsemble;
    }

    private void verifyLookingVotes() {
        // get the leader FLE first.
        Vote leaderVote = null;
        for (final Vote v : getVotes()) {
            if (v.getState() == QuorumPeer.ServerState.LEADING) {
                leaderVote = v;
                break;
            }
        }

        if (leaderVote == null) {
            return;
        }

        for (final Vote v : getVotes()) {
            if (v.getState() == QuorumPeer.ServerState.LOOKING ||
                    v.getState() == QuorumPeer.ServerState.FOLLOWING) {
                if ((v.getPeerEpoch() > leaderVote.getPeerEpoch()) ||
                        (v.getPeerEpoch() == leaderVote.getPeerEpoch() &&
                                v.getZxid() > leaderVote.getZxid())) {
                    LOG.error("vote :" + v + " is better than leader: "
                            + leaderVote);
                    throw new RuntimeException("error leader vote fell behind");
                }
            }
        }
    }

    public Ensemble copyFromParent() throws ExecutionException,
            InterruptedException {
        final HashMap<Long, FLEV2Wrapper> replaceMap = new HashMap<>();
        for (final FLEV2Wrapper fle : fles.values()) {
            replaceMap.put(fle.getId(), copyFLEV2(fle,
                    fle.getSelfVote().copy()));
        }
        this.fles = replaceMap;
        return this;
    }

    public Ensemble configureFromParent(
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                    quorumWithState)
            throws ExecutionException, InterruptedException {
        if (getState() != EnsembleState.CONFIGURED) {
            throw new IllegalAccessError("Ensemble: " + id + " not in " +
                    "configured state.");
        }
        final HashMap<Long, FLEV2Wrapper> copyFles = new HashMap<>();
        // get the leader FLE first.
        FLEV2Wrapper leaderFle = null;
        for (final ImmutablePair<Long, QuorumPeer.ServerState> p
                : quorumWithState) {
            if (p.getRight() == QuorumPeer.ServerState.LEADING) {
                if (leaderFle != null) {
                    throw new IllegalStateException("ensemble: " + id + " has" +
                            " two leaders.");
                }
                leaderFle = fles.get(p.getLeft());
            }
        }

        Vote leaderVote = null;
        if (leaderFle != null) {
            leaderVote = leaderFle.getSelfVote().makeMeLeader(random);
            leaderFle = copyFLEV2(leaderFle, leaderVote);
            copyFles.put(leaderFle.getId(), leaderFle);
        }

        // For every follower copy the leader's totalOrderPredicate
        for (final ImmutablePair<Long, QuorumPeer.ServerState> p
                : quorumWithState) {
            if (p.getRight() == QuorumPeer.ServerState.FOLLOWING) {
                if (leaderFle == null) {
                    throw new IllegalStateException("ensemble: " + id + " has" +
                            " followers without a leader");
                }
                copyFles.put(p.getLeft(), copyFLEV2(
                        fles.get(p.getLeft()),
                        fles.get(p.getLeft()).getSelfVote()
                                .makeMeFollower(leaderVote, random)));
            }
        }

        // now if there are any in looking state, set their vote as wanderers
        for (final ImmutablePair<Long, QuorumPeer.ServerState> p
                : quorumWithState) {
            if (p.getRight() == QuorumPeer.ServerState.LOOKING) {
                if (leaderVote != null) {
                    copyFles.put(p.getLeft(), copyFLEV2(fles.get(p.getLeft()),
                            fles.get(p.getLeft()).getSelfVote()
                                    .makeMeLooker(leaderVote, random)));
                } else {
                    copyFles.put(p.getLeft(), copyFLEV2(fles.get(p.getLeft()),
                            fles.get(p.getLeft()).getSelfVote().copy()));
                }
            }
        }

        this.fles = copyFles;
        this.LOG = new LogPrefix(LOGS, toString());
        return this;
    }

    private void setFleToRun(final long sid) {
        fleToRun = fles.get(sid);
    }

    private HashSet<Long> getLearnerQuorum() {
        final HashSet<Long> learnerSet = new HashSet<>();
        for (final FLEV2Wrapper fle : fles.values()) {
            if (fle.getState() == QuorumPeer.ServerState.LEADING ||
                    fle.getState() == QuorumPeer.ServerState.FOLLOWING) {
                learnerSet.add(fle.getId());
            }
        }
        return learnerSet;
    }

    /**
     * Initlize the ensemble votes and fle for each of them and set the fle.
     * @param size
     * @return
     * @throws ElectionException
     */
    public Collection<FLEV2Wrapper> getQuorumWithInitVoteSet(
            final long size, final QuorumVerifier quorumVerifier)
            throws ElectionException {
        final Collection<Vote> votes =
                AbstractEnsemble.initQuorumVotesWithSize(quorumSize);
        final Collection<FLEV2Wrapper> fles = new ArrayList<>();
        for(final Vote vote : votes) {
            FLEV2Wrapper fle = createFLEV2(vote.getSid(),
                    this.quorumVerifier);
            try {
                fle.updateSelfVote(vote).get();
            } catch (InterruptedException | ExecutionException exp) {
                LOG.error("failed to update vote, unhandled exp: " + exp);
                throw new RuntimeException(exp);
            }
            fles.add(fle);
        }
        return fles;
    }

    protected Collection<Vote> getVotes() {
        final Collection<Vote> votes = new ArrayList<>();
        for (final FLEV2Wrapper fle : fles.values()) {
            if (isConnected(fle.getId())) {
                votes.add(fle.getSelfVote());
            }
        }
        return votes;
    }

    private Collection<AbstractEnsemble> getQuorumCombinations()
            throws InterruptedException, ExecutionException {
        final boolean[] flevec = new boolean[quorumSize];
        return null;
    }


    public Collection<Collection<Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>>>>
    quorumMajorityWithLeaderServerStateCombinations() {
        return EnsembleHelpers.quorumMajorityWithLeaderServerStateCombinations(
                this.quorumSize);
    }

    private static void shutdown(FLEV2Wrapper flev2Wrapper) {
        flev2Wrapper.shutdown();
    }

    private static void shutdown(final Collection<FLEV2Wrapper> flev2Wrappers) {
        for (final FLEV2Wrapper fle : flev2Wrappers) {
            shutdown(fle);
        }
    }

    private static Collection<Vote> initQuorumVotesWithSize(final int size) {
        Collection<Vote> voteSet = new HashSet<>();
        for (int i = 0; i < size; i++) {
            voteSet.add(createInitVoteLooking(i+1));
        }
        return Collections.unmodifiableCollection(voteSet);
    }

    /**
     * Create a self leader vote(LOOKING) with everything else set to 0.
     *
     * @param sid
     * @return
     */
    private static Vote createInitVoteLooking(final long sid) {
        return createVoteWithState(sid, QuorumPeer.ServerState.LOOKING);
    }

    /**
     * Create a self leader vote with given state with everything else
     * set to 0.
     * @param sid
     * @param serverState
     * @return
     */
    private static Vote createVoteWithState(
            final long sid, QuorumPeer.ServerState serverState) {
        return new Vote(Vote.Notification.CURRENTVERSION, sid, 0, 0, 0,
                sid, serverState);
    }
}
