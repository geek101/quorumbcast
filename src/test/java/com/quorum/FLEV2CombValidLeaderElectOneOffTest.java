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
import com.quorum.helpers.EnsembleHelpers;
import com.quorum.netty.BaseTest;
import org.apache.commons.lang3.tuple.ImmutablePair;
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

@RunWith(Parameterized.class)
public class FLEV2CombValidLeaderElectOneOffTest extends BaseTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(
            FLEV2CombValidLeaderElectOneOffTest.class);

    private String ensembleType;
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
                { "mock", 1, TimeUnit.MILLISECONDS },
                { "mockbcast", 50, TimeUnit.MILLISECONDS},
                { "quorumbcast", 150, TimeUnit.MILLISECONDS},
        });
    }

    public FLEV2CombValidLeaderElectOneOffTest(final String ensembleType,
                                         final int stableTimeout,
                                         final TimeUnit stableTimeUnit) {
        this.ensembleType = ensembleType;
        this.stableTimeout = stableTimeout;
        this.stableTimeoutUnit = stableTimeUnit;
    }

    @Test
    public void testLeaderForFiveLeaderGoesToLooking()
            throws ElectionException, InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{1F,2F,3F,4F,5L}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(5);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        LOG.warn("verified " + parentEnsemble + "->" + movedEnsemble
                + " : election[" +
                EnsembleHelpers.getSidWithServerStateStr(
                        ImmutablePair.of(
                                movedEnsemble.getFleToRun().getId(),
                                movedEnsemble.getFleToRun().getState()))
                + "] -> " + doneEnsemble + ", leader: "
                + doneEnsemble.getLeaderLoopResult().values()
                .iterator().next().getLeader());
    }

    @Test
    public void testLookingJoinExistingEnsemble()
            throws ElectionException, InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{1k, 2L, 3F, 4F, 5K}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(1);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        LOG.warn("verified " + parentEnsemble + "->" + movedEnsemble
                + " : election[" +
                EnsembleHelpers.getSidWithServerStateStr(
                        ImmutablePair.of(
                                movedEnsemble.getFleToRun().getId(),
                                movedEnsemble.getFleToRun().getState()))
                + "] -> " + doneEnsemble + ", leader: "
                + doneEnsemble.getLeaderLoopResult().values()
                .iterator().next().getLeader());
    }

    @Test
    public void testPartitionEnsemble() throws ElectionException,
            InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{{1k, 2K, 5L}, {3F, 4F, 5L}}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(3);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        LOG.warn("verified " + parentEnsemble + "->" + movedEnsemble
                + " : election[" +
                EnsembleHelpers.getSidWithServerStateStr(
                        ImmutablePair.of(
                                movedEnsemble.getFleToRun().getId(),
                                movedEnsemble.getFleToRun().getState()))
                + "] -> " + doneEnsemble + ", leader: "
                + doneEnsemble.getLeaderLoopResult().values()
                .iterator().next().getLeader());
    }

    public Ensemble createEnsemble(final Long id, final int quorumSize) throws
            ElectionException {
        for (long sid = 1; sid <= quorumSize; sid++) {
            final QuorumServer quorumServer = new QuorumServer(sid,
                    new InetSocketAddress("localhost",
                            PortAssignment.unique()),
                    new InetSocketAddress("localhost",
                            PortAssignment.unique()));
            this.quorumServerList.add(quorumServer);
        }

        return EnsembleFactory.createEnsemble(
                this.ensembleType, id, quorumSize, this.stableTimeout,
                this.stableTimeoutUnit, this.quorumServerList,
                this.readTimeoutMsec,
                this.connectTimeoutMsec, this.keepAliveTimeoutMsec,
                this.keepAliveCount, this.keyStore.get(0),
                this.keyPassword.get(0), this.trustStore.get(0),
                this.trustPassword.get(0), this.trustStoreCAAlias);
    }
}
