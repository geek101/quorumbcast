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
import com.quorum.helpers.PortAssignment;
import com.quorum.netty.BaseTest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class FastLeaderElectionV2UnitTest extends BaseTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(FastLeaderElectionV2UnitTest.class);
    private String ensembleType;
    private final int quorumSize;
    private final int stableTimeout;
    private final TimeUnit stableTimeoutUnit;
    private final List<QuorumServer> quorumServerList = new ArrayList<>();
    private final Long readTimeoutMsec = 100L;
    private final Long connectTimeoutMsec = 500L;
    private final Long keepAliveTimeoutMsec = 50L;
    private final Integer keepAliveCount = 3;

    @Parameterized.Parameters
    public static Collection quorumTypeAndSize() {
        return Arrays.asList( new Object [][] {
                { "mock", 3, 1, TimeUnit.MILLISECONDS },
                { "mock", 5, 1, TimeUnit.MILLISECONDS },
                { "mock", 7, 1, TimeUnit.MILLISECONDS },
                { "mockbcast", 3, 50, TimeUnit.MILLISECONDS},
                { "mockbcast", 5, 50, TimeUnit.MILLISECONDS},
                { "quorumbcast", 3, 150, TimeUnit.MILLISECONDS},
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
        for (long sid = 1; sid <= quorumSize; sid++) {
            final QuorumServer quorumServer = new QuorumServer(sid,
                    new InetSocketAddress("localhost",
                            PortAssignment.unique()),
                    new InetSocketAddress("localhost",
                            PortAssignment.unique()));
            this.quorumServerList.add(quorumServer);
        }
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
        Ensemble ensemble = createEnsemble(1L)
                .configure(getInitLookingQuorumStr());
        final Collection<Ensemble> lookingSet = ensemble.moveToLookingForAll();
        for (final Ensemble e : lookingSet) {
            final Ensemble result = e.runLooking();
            result.verifyLeader();
            result.shutdown().get();
            e.shutdown().get();
        }
        ensemble.shutdown().get();
    }

    /**
     * Start with all combination of all majority subsets of a given
     * quorumSize with current leader and followers and for each node move it
     * to LOOKING state and run election and find the leader.
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test (timeout = 1000*300)
    public void testLeaderForCombinations()
            throws ElectionException, InterruptedException, ExecutionException {
        long count = 0;
        Ensemble ensemble = createEnsemble(1L);
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

    private void leaderCombValidate(ImmutableTriple<Ensemble, Ensemble,
            Ensemble> t) throws ExecutionException, InterruptedException {
        final Ensemble configured = t.getLeft();
        final Ensemble moved = t.getMiddle();
        final Ensemble done = t.getRight();
        LOG.warn("verify " + configured + "->" + moved + " : election[" +
                Ensemble.getSidWithServerStateStr(
                        ImmutablePair.of(
                                moved.getFleToRun().getId(),
                                moved.getFleToRun().getState()))
                + "]");

        done.verifyLeader();
        done.shutdown().get();

        LOG.warn("verified " + configured + "->" + moved + " : election[" +
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

    public Ensemble createEnsemble(final Long id) throws ElectionException {
        return EnsembleFactory.createEnsemble(
                this.ensembleType, id, this.quorumSize, this.stableTimeout,
                this.stableTimeoutUnit, this.quorumServerList,
                this.readTimeoutMsec,
                this.connectTimeoutMsec, this.keepAliveTimeoutMsec,
                this.keepAliveCount, this.keyStore.get(0),
                this.keyPassword.get(0), this.trustStore.get(0),
                this.trustPassword.get(0), this.trustStoreCAAlias);
    }
}
