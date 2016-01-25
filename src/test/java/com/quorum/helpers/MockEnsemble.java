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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class MockEnsemble extends AbstractEnsemble {
    private static final Logger LOG
            = LoggerFactory.getLogger(MockEnsemble.class.getClass());
    private MockQuorumBcast mockQuorumBcast;
    private FLEV2Wrapper fleThatRan;
    public MockEnsemble(final long id, final int quorumSize,
                        final int stableTimeout,
                        final TimeUnit stableTimeoutUnit)
            throws ElectionException {
        super(id, quorumSize, stableTimeout, stableTimeoutUnit);
        mockQuorumBcast = new MockQuorumBcast(getId(), quorumSize,
                getQuorumCnxMesh());
    }

    protected MockEnsemble(
            final Ensemble parentEnsemble, final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                                   flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long,
                    QuorumPeer.ServerState>>> partitionedQuorumArg,
            final int stableTimeout,
            final TimeUnit stableTimeoutUnit) {
        super(parentEnsemble, quorumCnxMeshArg, flatQuorumWithState,
                partitionedQuorumArg, stableTimeout, stableTimeoutUnit);
        mockQuorumBcast = new MockQuorumBcast(getId(),
                parentEnsemble.getQuorumSize(),
                quorumCnxMeshArg);
    }

    /**
     * Override this in the implementation to spin up the right type of
     * ensemble using the parent.
     *
     * @param parentEnsemble
     * @param quorumCnxMeshArg
     * @param flatQuorumWithState
     * @param partitionedQuorumArg @return
     */
    @Override
    public Ensemble createEnsemble(
            final Ensemble parentEnsemble, final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                                               flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long,
                                           QuorumPeer.ServerState>>>
                                               partitionedQuorumArg)
            throws ElectionException {
        return new MockEnsemble(parentEnsemble, quorumCnxMeshArg,
                flatQuorumWithState, partitionedQuorumArg,
                stableTimeout, stableTimeoutUnit);
    }

    /**
     * Mock needs to run looking manually for non targets first.
     * @param sid
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws ElectionException
     */
    @Override
    protected void runLookingForSid(final long sid)
            throws InterruptedException, ExecutionException, ElectionException {
        super.runLookingForSid(sid);

        fleThatRan = fles.get(sid);

        // bump up election epoch for the peer for which we need to run looking

        final Vote runVote = fleThatRan.getSelfVote(); //.increaseElectionEpoch();
        fleThatRan.updateSelfVote(runVote).get();

        // bump rest of the peers election epoch to match so one run
        // will finish leader election
        for (final FLEV2Wrapper f : fles.values()) {
            if (f.getId() != sid &&
                    f.getState() == QuorumPeer.ServerState.LOOKING
                    // TODO: fix negative verification.
                    && isConnected(f.getId())) {
                f.updateSelfVote(f.getSelfVote()
                        .setElectionEpoch(runVote)).get();
            }
        }

        // copy best totalOrderPredicate now that electionEpoch is normalized
        Vote bestVote = null;
        for (final FLEV2Wrapper f : fles.values()) {
            if (f.getState() == QuorumPeer.ServerState.LOOKING
                    // TODO: fix negative verification.
                    && isConnected(f.getId())) {
                if (bestVote == null ||
                        totalOrderPredicate(f.getSelfVote(), bestVote)) {
                    bestVote = f.getSelfVote();
                }
            }
        }

        // now let everyone catch up to this.
        for (final FLEV2Wrapper f : fles.values()) {
            if (f.getId() != sid &&
                    f.getState() == QuorumPeer.ServerState.LOOKING
                    // TODO: fix negative verification.
                    && isConnected(f.getId())) {
                f.updateSelfVote(f.getSelfVote()
                        .catchUpToVote(bestVote)).get();
            }
        }

        fleThatRan.updateSelfVote(fleThatRan.getSelfVote()
                .catchUpToVote(bestVote));

        final List<ImmutablePair<Long, Future<Vote>>> runOnceVotes
                = new ArrayList<>();
        // Now assume everyone sees everyone votes and run election expect
        // for the target FLE.
        for (final FLEV2Wrapper f : fles.values()) {
            if (f.getId() != sid &&
                    f.getState() == QuorumPeer.ServerState.LOOKING
                    // TODO: fix negative verification.
                    && isConnected(f.getId())) {
                runOnceVotes.add(ImmutablePair.of(f.getId(),
                        f.runLeaderElection(getVotes())));
            }
        }

        // run ours at last.
        runOnceVotes.add(ImmutablePair.of(fleThatRan.getId(),
                fleThatRan.runLeaderElection(getVotes())));

        long electedLeaderSid = Long.MIN_VALUE;
        if (lookingResultVotes == null) {
            lookingResultVotes = new HashMap<>();
        }
        for (final ImmutablePair<Long, Future<Vote>> pair : runOnceVotes) {
            final Vote v = pair.getRight().get();
            if (v != null && v.getSid() == pair.getLeft() &&
                    v.getSid() == v.getLeader()) {
                final Vote leaderStateVote = v.setServerState(QuorumPeer
                        .ServerState.LEADING);
                fles.get(pair.getLeft()).updateSelfVote(leaderStateVote);
                lookingResultVotes.put(pair.getLeft(), leaderStateVote);
                electedLeaderSid = pair.getLeft();
                break;
            }
        }

        if (electedLeaderSid == Long.MIN_VALUE) {
            // now let everyone catch up to this.
            for (final FLEV2Wrapper f : fles.values()) {
                if (f.getState() == QuorumPeer.ServerState.LEADING
                        // TODO: fix negative verification.
                        && isConnected(f.getId())) {
                    electedLeaderSid = f.getId();
                }
            }
        }

        assert electedLeaderSid != Long.MIN_VALUE;

        // Now leader selected itself, run for rest of them.
        for (final FLEV2Wrapper f : fles.values()) {
            if (f.getId() != electedLeaderSid && f.getId() != sid &&
                    f.getState() == QuorumPeer.ServerState.LOOKING
                    // TODO: fix negative verification.
                    && isConnected(f.getId())) {
                lookingResultVotes.put(f.getId(),
                        f.runLeaderElection(getVotes()).get());
            }
        }

        // run ours at last if we are not leader already.
        if (fleThatRan.getId() != electedLeaderSid) {
            lookingResultVotes.put(fleThatRan.getId(),
                    fleThatRan.runLeaderElection(getVotes()).get());
        }
    }

    @Override
    public FLEV2Wrapper getFleThatRan() {
        return this.fleThatRan;
    }

    public boolean isConnected(final long serverSid) {
        return mockQuorumBcast.isConnectedToAny(serverSid);
    }

    @Override
    protected FLEV2Wrapper createFLEV2(
            final long sid, final QuorumVerifier quorumVerifier) {
        if (mockQuorumBcast == null) {
            mockQuorumBcast = new MockQuorumBcast(getId(), getQuorumSize(),
                    getQuorumCnxMesh());
        }
        return createFLEV2Wrapper(sid, quorumVerifier);
    }


    @Override
    protected FLEV2Wrapper copyFLEV2(final FLEV2Wrapper fle, final Vote vote)
            throws InterruptedException, ExecutionException {
        if (!(fle instanceof  MockFLEV2Wrapper)) {
            throw new IllegalArgumentException("fle is not of Mock type");
        }
        final MockFLEV2Wrapper mockFle = (MockFLEV2Wrapper)fle;
        final FLEV2Wrapper newFle = createFLEV2Wrapper(mockFle.getId(),
                mockFle.getQuorumVerifier());
        newFle.updateSelfVote(vote).get();
        return newFle;
    }

    private FLEV2Wrapper createFLEV2Wrapper(
            final long sid, final QuorumVerifier quorumVerifier) {
        final MockVoteView voteView
                = new MockVoteView(sid, mockQuorumBcast);
        return new MockFLEV2Wrapper(sid, QuorumPeer.LearnerType.PARTICIPANT,
                quorumVerifier, voteView,
                voteView, stableTimeout, stableTimeoutUnit);
    }

    private boolean totalOrderPredicate(final Vote newVote,
                                        final Vote curVote) {
        return totalOrderPredicate(newVote.getSid(),
                newVote.getZxid(), newVote.getPeerEpoch(),
                curVote.getSid(), curVote.getZxid(),
                curVote.getPeerEpoch());
    }

    private boolean totalOrderPredicate(long newId, long newZxid,
                                        long newEpoch, long curId,
                                        long curZxid, long curEpoch) {
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
}
