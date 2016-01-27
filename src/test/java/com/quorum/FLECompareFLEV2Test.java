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

import com.quorum.helpers.Ensemble;
import com.quorum.helpers.FLEV2BaseTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FLECompareFLEV2Test extends FLEV2BaseTest {
    @Parameterized.Parameters
    public static Collection quorumTypeAndSize() {
        return Arrays.asList( new Object [][] {
                // type, stability-tout, read-tout, connect-tout,
                // keep-alive-tout, keep-alive count, time unit for all
                { "flemockbcast", 250, 0, 0, 0, 0, TimeUnit.MILLISECONDS},
                { "flequorumbcast", 150, 150, 250, 50, 3,
                        TimeUnit.MILLISECONDS},
                { "mockbcast", 250, 0, 0, 0, 0, TimeUnit.MILLISECONDS},
                { "quorumbcast", 150, 150, 250, 50, 3, TimeUnit.MILLISECONDS},
        });
    }

    public FLECompareFLEV2Test(final String ensembleType,
                               final int stableTimeoutMsec,
                               final int readTimeoutMsec,
                               final int connectTimeoutMsec,
                               final int keepAliveTimeoutMsec,
                               final int keepAliveCount,
                               final TimeUnit timeoutUnit) {
        super(ensembleType, stableTimeoutMsec, readTimeoutMsec,
                connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                timeoutUnit);
    }

    @Test (timeout = 20000)
    public void test1_InitAllToLooking()
            throws ElectionException, InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 3);
        final Ensemble parentEnsemble
                = ensemble.configure("{1K,2K,3K}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(2);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }

    @Test (timeout = 20000)
    public void test1_LeaderGoesToLooking()
            throws ElectionException, InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 3);
        final Ensemble parentEnsemble
                = ensemble.configure("{1F,2F,3L}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(1);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }

    @Test (timeout = 20000)
    public void test2_PartitionJoinLeader() throws ElectionException,
            InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{{1k, 2K, 5L}, {2K, 3F, 4F, 5L}}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(2);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }

    @Test (timeout = 20000)
    public void test3_PartitionAllLooking() throws ElectionException,
            InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{{1k, 2K}, {1K, 3K}, {1K, 4K}, {5K}}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(1);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }
}
