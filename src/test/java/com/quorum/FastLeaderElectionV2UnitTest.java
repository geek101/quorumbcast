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

import com.quorum.helpers.Ensemble;
import com.quorum.helpers.EnsembleFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class FastLeaderElectionV2UnitTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(FastLeaderElectionV2UnitTest.class);
    private String ensembleType;
    private final int quorumSize;
    private final int stableTimeout;
    private final TimeUnit stableTimeoutUnit;

    @Parameterized.Parameters
    public static Collection quorumTypeAndSize() {
        return Arrays.asList( new Object [][] {
                { "mock", 3, 1, TimeUnit.MILLISECONDS },
                { "mock", 5, 1, TimeUnit.MILLISECONDS },
                { "mock", 7, 1, TimeUnit.MILLISECONDS },
                { "mockbcast", 3, 50, TimeUnit.MILLISECONDS},
                { "mockbcast", 5, 50, TimeUnit.MILLISECONDS},
        });
    }

    public FastLeaderElectionV2UnitTest(final String ensembleType,
                                        final int quorumSize,
                                        final int stableTimeout,
                                        final TimeUnit stableTimeUnit) {
        this.ensembleType = ensembleType;
        this.quorumSize = quorumSize;
        this.stableTimeout = stableTimeout;
        this.stableTimeoutUnit = stableTimeUnit;
    }

    @Before
    public void setup() {
        LOG.info("Setup with Ensemble Type: " + ensembleType +
                ", Quorum size: " + this.quorumSize);
    }

    /**
     * Test leader election fails with votes from init set.
     * The largest Sid server will always become the leader when rest of
     * the params are same for all votes.
     *
     * @throws ElectionException
     */
    @Test
    public void testLeaderElectionFromInitLookingSet()
            throws ElectionException, InterruptedException, ExecutionException {
        Ensemble ensemble = EnsembleFactory.createEnsemble(
                this.ensembleType, 1L, this.quorumSize, this.stableTimeout,
                this.stableTimeoutUnit).configure(getInitLookingQuorumStr());
        final Collection<Ensemble> lookingSet = ensemble.moveToLookingForAll();
        for (final Ensemble e : lookingSet) {
            final Ensemble result = e.runLooking();
            result.verifyLeader();
            result.shutdown();
            e.shutdown();
        }
        ensemble.shutdown();
    }

    /**
     * Start with all combination of all majority subsets of a given
     * quorumSize with current leader and followers and for each node move it
     * to LOOKING state and run election and find the leader.
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test (timeout = 1000*600)
    public void testLeaderForCombinations()
            throws ElectionException, InterruptedException, ExecutionException {
        long count = 0;
        Ensemble ensemble = EnsembleFactory.createEnsemble(
                this.ensembleType, 1L, this.quorumSize, this.stableTimeout,
                this.stableTimeoutUnit);
        final Collection<Collection<Collection<
                ImmutablePair<Long, QuorumPeer.ServerState>>>> combs =
                ensemble.quorumMajorityWithLeaderServerStateCombinations();
        for (final Collection<Collection<
                ImmutablePair<Long, QuorumPeer.ServerState>>> c : combs) {
            for (final Collection<
                    ImmutablePair<Long, QuorumPeer.ServerState>> q : c) {
                final Ensemble configuredParent = ensemble.configure(q);
                LOG.info("config for: " + Ensemble
                        .getQuorumServerStateCollectionStr(q)
                        + " result: " + configuredParent);
                final Collection<Ensemble> movedEnsembles = configuredParent
                        .moveToLookingForAll();
                for (final Ensemble e: movedEnsembles) {
                    LOG.warn("ensemble looking: " + e);
                    leaderCombValidate(ImmutableTriple.of(configuredParent, e,
                            e.runLooking()));
                    count++;
                }
            }
        }
        LOG.warn("For QuorumSize: " + quorumSize + " validated count: " +
                count);
    }

    @Test
    public void testLeaderForFiveLeaderLooking()
        throws ElectionException, InterruptedException, ExecutionException {
        final Ensemble ensemble = EnsembleFactory.createEnsemble(
                "mockbcast", 1L, 5, 50, TimeUnit.MILLISECONDS);
        final Ensemble parentEnsemble
                = ensemble.configure("{1F,2F,3K,4F,5L}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(5);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();
        LOG.info("verified " + parentEnsemble + "->" + movedEnsemble
                + " : election[" +
                Ensemble.getSidWithServerStateStr(
                        ImmutablePair.of(
                                movedEnsemble.getFleToRun().getId(),
                                movedEnsemble.getFleToRun().getState()))
                + "] -> leader: "
                + doneEnsemble.getLeaderLoopResult().values()
                .iterator().next().getLeader());
        doneEnsemble.verifyLeader();
    }

    private void leaderCombValidate(ImmutableTriple<Ensemble, Ensemble,
            Ensemble> t) {
        final Ensemble configured = t.getLeft();
        final Ensemble moved = t.getMiddle();
        final Ensemble done = t.getRight();
        LOG.info("verify " + configured + "->" + moved + " : election[" +
                Ensemble.getSidWithServerStateStr(
                        ImmutablePair.of(
                                moved.getFleToRun().getId(),
                                moved.getFleToRun().getState()))
                + "]");

        done.verifyLeader();

        LOG.info("verified " + configured + "->" + moved + " : election[" +
                Ensemble.getSidWithServerStateStr(
                        ImmutablePair.of(
                                moved.getFleToRun().getId(),
                                moved.getFleToRun().getState()))
                + "] -> leader: "
                + done.getLeaderLoopResult().values()
                .iterator().next().getLeader());
    }

    private String getInitLookingQuorumStr() {
        String str = "{1K";
        for (long i = 2; i <= this.quorumSize; i++) {
            str += ", " + i + "K";
        }
        return str + "}";
    }
}
