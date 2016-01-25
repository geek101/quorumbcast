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
import com.quorum.helpers.FLEV2BaseTest;
import com.quorum.helpers.PortAssignment;
import com.quorum.helpers.EnsembleHelpers;
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
public class FLEV2CombValidLeaderElectTest extends FLEV2BaseTest {
    @Parameterized.Parameters
    public static Collection quorumTypeAndSize() {
        return Arrays.asList( new Object [][] {
                // type, quorum-size, stable-timeout, stable-timeout unit
                { "mock", 3, 1, 0, 0, 0, 0, TimeUnit.MILLISECONDS },
                { "mock", 5, 1, 0, 0, 0, 0, TimeUnit.MILLISECONDS },
                { "mock", 7, 1, 0, 0, 0, 0, TimeUnit.MILLISECONDS },
                { "mockbcast", 3, 50, 0, 0, 0, 0, TimeUnit.MILLISECONDS},
                { "mockbcast", 5, 50, 0, 0, 0, 0, TimeUnit.MILLISECONDS},
                { "quorumbcast", 3, 150, 150, 250, 50, 3,
                        TimeUnit.MILLISECONDS},
                { "quorumbcast-ssl", 3, 350, 350, 550, 50, 7,
                        TimeUnit.MILLISECONDS},
        });
    }

    public FLEV2CombValidLeaderElectTest(final String ensembleType,
                                         final int quorumSize,
                                         final int stableTimeoutMsec,
                                         final int readTimeoutMsec,
                                         final int connectTimeoutMsec,
                                         final int keepAliveTimeoutMsec,
                                         final int keepAliveCount,
                                         final TimeUnit timeoutUnit) {
        super(ensembleType, quorumSize, stableTimeoutMsec, readTimeoutMsec,
                connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                timeoutUnit);
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
            result.verifyLeaderAfterShutDown();
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
    @Test (timeout = 1000*500)
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
                LOG.info("config for: " + EnsembleHelpers
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
                EnsembleHelpers.getSidWithServerStateStr(
                        ImmutablePair.of(
                                moved.getFleToRun().getId(),
                                moved.getFleToRun().getState()))
                + "]");

        done.verifyLeaderAfterShutDown();

        verifyPrintHelper(configured, moved, done);
    }

    private String getInitLookingQuorumStr() {
        String str = "{1K";
        for (long i = 2; i <= this.quorumSize; i++) {
            str += ", " + i + "K";
        }
        return str + "}";
    }


}
