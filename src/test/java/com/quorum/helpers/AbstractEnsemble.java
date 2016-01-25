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
    protected Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                                                   flatQuorumWithState;
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
    final HashMap<Long, Future<Vote>> terminatingFuturesMap
            = new HashMap<>();
    final HashMap<Long, Future<Vote>> nonTerminatingFuturesMap
            = new HashMap<>();
    protected HashMap<Long, Vote> lookingResultVotes = null;
    private boolean runLookingDone = false;
    private Long safetyPred;

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
            this.fles = ((AbstractEnsemble)parent).fles;
        }
        this.stableTimeout = stableTimeout;
        this.stableTimeoutUnit = stableTimeoutUnit;
    }

    /**
     * Called only at create time, this is equivalent to
     * createEnsemble(id, size, "{ 1K, 2K, .... NK }"), i.e no partitions
     * everyone can connect to everyone else.
     * @param id
     * @param quorumSize
     * @param stableTimeout
     * @param stableTimeoutUnit
     * @throws ElectionException
     */
    public AbstractEnsemble(final long id, final int quorumSize,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit)
            throws ElectionException {
        this(id, quorumSize, new QuorumMajWrapper(quorumSize),
                EnsembleState.INVALID, null, stableTimeout, stableTimeoutUnit);
        this.quorumCnxMesh = new QuorumCnxMeshBase(this.quorumSize);
        this.quorumCnxMesh.connectAll();

        final Collection<ImmutablePair<Long, QuorumPeer
                .ServerState>> noPartition = new ArrayList<>();
        this.fles = new HashMap<>();
        for (final FLEV2Wrapper fle : getQuorumWithInitVoteSet(
                this.quorumSize, this.quorumVerifier)) {
            this.fles.put(fle.getId(), fle);
            noPartition.add(ImmutablePair.of(fle.getId(), fle.getState()));
        }
        this.partitionedQuorum.add(noPartition);
        this.LOG = new LogPrefix(LOGS, toString());
    }

    protected AbstractEnsemble(
            final Ensemble parentEnsemble, final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                                       flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long,
                                       QuorumPeer.ServerState>>>
                    partitionedQuorumArg,
            final int stableTimeout,
            final TimeUnit stableTimeoutUnit) {
        this(((AbstractEnsemble)parentEnsemble).getId() + 1,
                parentEnsemble.getQuorumSize(),
                ((AbstractEnsemble)parentEnsemble).getQuorumVerifier(),
                ((AbstractEnsemble)parentEnsemble).getState(),
                parentEnsemble, stableTimeout, stableTimeoutUnit);
        this.quorumCnxMesh = quorumCnxMeshArg;
        this.flatQuorumWithState = flatQuorumWithState;
        this.partitionedQuorum = partitionedQuorumArg;
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
            if (fles == null) {
                stringList.add(
                        EnsembleHelpers.getQuorumServerStateCollectionStr(t));
                continue;
            }
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
            final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<
                    ImmutablePair<Long, QuorumPeer.ServerState>>
                    flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long, QuorumPeer
                    .ServerState>>> partitionedQuorumArg) throws ElectionException;

    protected abstract FLEV2Wrapper createFLEV2(
            final long sid, final QuorumVerifier quorumVerifier);

    protected abstract FLEV2Wrapper copyFLEV2(
            final FLEV2Wrapper fle, final Vote vote)
            throws InterruptedException, ExecutionException;

    /**
     * For looking which are in partition in Quorum must elect a leader but
     * everyone should agree on it regardless. Otherwise existing LEADER must
     * be elected.
     * For looking which are in partition with no Quorum will not terminate,
     * verify that.
     * New leader and terminating followers should never violate safety, i.e
     * should have better peerEpoch what we had before leader election is run.
     */
    @Override
    public void verifyLeaderAfterShutDown()
            throws InterruptedException, ExecutionException {
        waitForEnsembleToRun();


        // This gets results from terminating set.
        final HashMap<Long, Vote> resultVotes = getLeaderLoopResult();

        // Verify before shutdown non of the non terminating results finish.
        for (final Map.Entry<Long, Future<Vote>> f : nonTerminatingFuturesMap
                .entrySet()) {
            assertTrue("non terminating sid: " + f.getKey(),
                    !f.getValue().isDone());
            fles.get(f.getKey()).verifyNonTermination();
        }

        // Above will wait for leader elections to finish.
        shutdown().get();

        // If none are terminating runs then exit here.
        if (resultVotes.isEmpty()) {
            return;
        }

        final HashMap<Long, HashSet<Long>> leaderQuorumMap = new HashMap<>();

        for (final FLEV2Wrapper fle : fles.values()) {
            if (!leaderQuorumMap.containsKey(fle.getSelfVote().getLeader())) {
                leaderQuorumMap.put(fle.getSelfVote().getLeader(),
                        new HashSet<>(
                        Collections.singletonList(fle.getId())));
            } else {
                leaderQuorumMap.get(fle.getSelfVote().getLeader())
                        .add(fle.getId());
            }
        }

        verifyLeaderInQuorum(leaderQuorumMap);
    }

    private void verifyLeaderInQuorum(
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
        if (vote.getPeerEpoch() != safetyPred) {
            final String errStr = "leader vote : " + vote + " failed"
                    + " saftey check peerEpoch: 0x"
                    + Long.toHexString(safetyPred);
            assertEquals(errStr, safetyPred.longValue(),
                    vote.getPeerEpoch());
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
        if (lookingResultVotes != null) {
            return lookingResultVotes;
        }

        lookingResultVotes = new HashMap<>();

        // Collect results to terminating and non terminating .
        collectLeaderLoopResultFutures();

        // wait for everyone to finish before waiting the result of
        // the peer we are interested in.
        for (final Map.Entry<Long,
                Future<Vote>> entry : terminatingFuturesMap.entrySet()) {
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

    /**
     * Algo: go through each partition, if partition has enough members i.e
     * (n/2 + 1) then all looking members there must terminate.
     */
    private void collectLeaderLoopResultFutures() {
        final HashMap<Long, Integer> fleVisibleCountMap
                = new HashMap<>();
        for (final Collection<ImmutablePair<Long, QuorumPeer
                .ServerState>> partition : partitionedQuorum) {
            for (final ImmutablePair<Long, QuorumPeer.ServerState> pair :
                    partition) {
                if (!fleVisibleCountMap.containsKey(pair.getKey())) {
                    fleVisibleCountMap.put(pair.getKey(), partition.size());
                } else {
                    fleVisibleCountMap.put(pair.getKey(),
                            fleVisibleCountMap.get(pair.getKey())
                                    + partition.size());
                }
            }
        }

        for (final Map.Entry<Long, Integer> e : fleVisibleCountMap.entrySet()) {
            HashMap<Long, Future<Vote>> resultMap =
                    nonTerminatingFuturesMap;
            if (e.getValue() >= ((quorumSize / 2) + 1)) {
                 resultMap = terminatingFuturesMap;
            }

            if (futuresForLookingPeers.get(e.getKey()) != null) {
                resultMap.put(e.getKey(),
                        futuresForLookingPeers.get(e.getKey()));
            }
        }
    }

    /**
     * Wait for each partition to run leader election with votes from
     * all of its peers in that partition.
     * TODO: Improve to make sure all round are run!?.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void waitForEnsembleToRun() throws InterruptedException,
            ExecutionException {
        final HashMap<Long, HashMap<Long, Vote>> fleVisibleVoteMap
                = new HashMap<>();
        for (final Collection<ImmutablePair<Long,
                QuorumPeer.ServerState>> partition : partitionedQuorum) {
            final HashMap<Long, Vote> partitionVotes = new HashMap<>();
            for (final ImmutablePair<Long, QuorumPeer.ServerState> p
                    : partition) {
                final FLEV2Wrapper fle = fles.get(p.getLeft());
                partitionVotes.put(fle.getId(), fle.getSelfVote());
            }

            for (final ImmutablePair<Long, QuorumPeer.ServerState> p
                    : partition) {
                if (!fleVisibleVoteMap.containsKey(p.getLeft())) {
                    final HashMap<Long, Vote> pv = new HashMap<>();
                    pv.putAll(partitionVotes);
                    fleVisibleVoteMap.put(p.getLeft(), pv);
                } else {
                    fleVisibleVoteMap.get(p.getLeft())
                            .putAll(partitionVotes);
                }
            }
        }

        for (final Long sid : futuresForLookingPeers.keySet()) {
            LOG.warn("waiting for vote run for fle: " + sid);
            fles.get(sid).waitForVotesRun(fleVisibleVoteMap.get(sid));
        }
    }

    @Override
    public Future<?> shutdown() {
        shutdown(fles.values());
        return CompletableFuture.completedFuture(null);
    }

    public static ImmutablePair<Long, QuorumPeer.ServerState>
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

    public static int configureParser(final String quorumStr, int idx,
                               final HashSet<Long> sidMap,
                               final QuorumCnxMesh quorumCnxMeshArg,
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
                        quorumCnxMeshArg,
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
                        quorumCnxMeshArg.connect(sidSrc, sidDst);
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
        final QuorumCnxMesh quorumCnxMeshLocal
                = new QuorumCnxMeshBase(quorumSize);
        final Collection<Collection<ImmutablePair<Long, QuorumPeer
                .ServerState>>> partitionedQuorumLocal = new ArrayList<>();
        final HashMap<Long, ImmutablePair<Long, QuorumPeer.ServerState>>
                flatQuorum = new HashMap<>();

        configureParser(quorumStr, 0, null, quorumCnxMeshLocal, flatQuorum,
                partitionedQuorumLocal);

        return configure(quorumCnxMeshLocal,
                flatQuorum.values(), partitionedQuorumLocal);
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
        return configure(EnsembleHelpers.getQuorumServerStateCollectionStr(
                (quorumWithState)));
    }

    private Ensemble configure(
            final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>> flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long, QuorumPeer
            .ServerState>>> partitionedQuorumArg)
            throws ElectionException, ExecutionException, InterruptedException {
        if (getState() != EnsembleState.INITIAL) {
            throw new IllegalAccessError("Ensemble: " + id + " not in " +
                    "init state.");
        }

        final AbstractEnsemble childEnsemble
                = (AbstractEnsemble) createEnsemble(this, quorumCnxMeshArg,
                flatQuorumWithState, partitionedQuorumArg);
        childEnsemble.prepareSelf();
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
            final HashSet<Long> learnerSet = getLearnerQuorum().getRight();
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

        Long bestPeerEpoch = null;
        for (final FLEV2Wrapper fle : fles.values()) {
            if (bestPeerEpoch == null) {
                bestPeerEpoch = fle.getSelfVote().getPeerEpoch();
                continue;
            }

            bestPeerEpoch = Math.max(fle.getSelfVote().getPeerEpoch(),
                    bestPeerEpoch);
        }

        safetyPred = bestPeerEpoch;
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
                = (AbstractEnsemble)createEnsemble(ensemble,
                this.quorumCnxMesh, this.flatQuorumWithState,
                this.partitionedQuorum);
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

    protected Ensemble prepareSelf()
            throws ExecutionException, InterruptedException {
        if (getState() != EnsembleState.CONFIGURED) {
            throw new IllegalAccessError("Ensemble: " + id + " not in " +
                    "configured state.");
        }
        final ImmutablePair<Map<Long, Vote>, Vote> learnerVotePair =
                coerceLearnerVotes();
        final Map<Long, Vote> followingVoteMap = coerceLookingVotes(
                learnerVotePair.getRight());

        // check if there is an error
        for (final long sid : learnerVotePair.getLeft().keySet()) {
            if (followingVoteMap.containsKey(sid)) {
                throw new RuntimeException("error: sid: " + sid + " clash!");
            }
        }

        // merge all the votes since it is safe to do so.
        followingVoteMap.putAll(learnerVotePair.getLeft());

        final HashMap<Long, FLEV2Wrapper> copyFles = new HashMap<>();
        for (final ImmutablePair<Long, QuorumPeer.ServerState> p
                : flatQuorumWithState) {
            copyFles.put(p.getLeft(), copyFLEV2(fles.get(p.getLeft()),
                    followingVoteMap.get(p.getLeft())));
        }

        this.fles = copyFles;
        this.LOG = new LogPrefix(LOGS, toString());
        return this;
    }

    private void setFleToRun(final long sid) {
        fleToRun = fles.get(sid);
    }

    /**
     * If there is a leader and followers then coerce them according to
     * the following rules:
     * Leader and Followers have the same peerEpoch, leader has the highest
     * Zxid and followers have zxid upto leader's but cannot exceed.
     * @return Pair of LeaderVote and MinZxidFollowerVote or pair of null, null
     */
    private ImmutablePair<Map<Long, Vote>, Vote> coerceLearnerVotes() {
        final ImmutablePair<Long, HashSet<Long>> pair
                = getLeaderQuorumFromFlatServerState();
        if (pair.getLeft() == Long.MIN_VALUE) {
            return ImmutablePair.of(new HashMap<>(), null);
        }

        final Map<Long, Vote> voteMap = new HashMap<>();
        final Vote leaderVote = createVoteWithState(pair.getLeft(),
                QuorumPeer.ServerState.LEADING).makeMeLeader(random);
        voteMap.put(leaderVote.getSid(), leaderVote);

        Vote minZxidVote = null;
        for (final long sid : pair.getRight()) {
            if (sid == leaderVote.getSid()) {
                continue;
            }

            final Vote followerVote
                    = createVoteWithState(sid, QuorumPeer.ServerState.FOLLOWING)
                    .makeMeFollower(leaderVote, random);
            voteMap.put(followerVote.getSid(), followerVote);
            if (minZxidVote == null ||
                    minZxidVote.getZxid() > followerVote.getZxid()) {
                minZxidVote = followerVote;
            }
        }

        return ImmutablePair.of(voteMap, minZxidVote);
    }

    private Map<Long, Vote> coerceLookingVotes(final Vote totalOrderVote) {
        final Map<Long, Vote> voteMap = new HashMap<>();
        for (final ImmutablePair<Long, QuorumPeer.ServerState> pair
                : flatQuorumWithState) {
            if (pair.getRight() != QuorumPeer.ServerState.LOOKING) {
                continue;
            }

            final Vote lookingVote = createVoteWithState(pair.getLeft(),
                    QuorumPeer.ServerState.FOLLOWING).makeMeLooker
                    (totalOrderVote, random);
            voteMap.put(lookingVote.getSid(), lookingVote);
        }

        return voteMap;
    }

    private ImmutablePair<Long, HashSet<Long>>
            getLeaderQuorumFromFlatServerState() {
        long leaderSid = Long.MIN_VALUE;
        final HashSet<Long> learnerSet = new HashSet<>();
        for (final ImmutablePair<Long, QuorumPeer.ServerState> pair
                : flatQuorumWithState) {
            if (pair.getRight() == QuorumPeer.ServerState.LEADING) {
                assert leaderSid == Long.MIN_VALUE;
                leaderSid = pair.getLeft();
                learnerSet.add(pair.getLeft());
                continue;
            }

            if (pair.getRight() == QuorumPeer.ServerState.FOLLOWING) {
                learnerSet.add(pair.getLeft());
            }
        }
        return ImmutablePair.of(leaderSid, learnerSet);
    }

    private ImmutablePair<Long, HashSet<Long>> getLearnerQuorum() {
        long leaderSid = Long.MIN_VALUE;
        final HashSet<Long> learnerSet = new HashSet<>();
        for (final FLEV2Wrapper fle : fles.values()) {
            if (fle.getState() == QuorumPeer.ServerState.LEADING) {
                assert leaderSid == Long.MIN_VALUE;
                leaderSid = fle.getId();
            }

            if (fle.getState() == QuorumPeer.ServerState.LEADING
                    || fle.getState() == QuorumPeer.ServerState.FOLLOWING) {
                learnerSet.add(fle.getId());
            }
        }
        return ImmutablePair.of(leaderSid, learnerSet);
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
