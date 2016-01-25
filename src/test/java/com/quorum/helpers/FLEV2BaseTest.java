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
import com.quorum.QuorumServer;
import com.quorum.netty.BaseTest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FLEV2BaseTest extends BaseTest {
    protected static final Logger LOG
            = LoggerFactory.getLogger(
            FLEV2BaseTest.class.getName());

    protected final String ensembleType;
    protected final int stableTimeoutMsec;
    protected final List<QuorumServer> quorumServerList = new ArrayList<>();
    protected final long readTimeoutMsec;
    protected final long connectTimeoutMsec;
    protected final long keepAliveTimeoutMsec;
    protected final int keepAliveCount;
    protected final TimeUnit timeoutUnit;
    protected int quorumSize;

    public FLEV2BaseTest(final String ensembleType,
                         final int stableTimeoutMsec,
                         final int readTimeoutMsec,
                         final int connectTimeoutMsec,
                         final int keepAliveTimeoutMsec,
                         final int keepAliveCount,
                         final TimeUnit timeoutUnit) {
        this.ensembleType = ensembleType;
        this.stableTimeoutMsec = stableTimeoutMsec;
        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
        this.timeoutUnit = timeoutUnit;
    }

    public FLEV2BaseTest(final String ensembleType,
                         final int quorumSize,
                         final int stableTimeout,
                         final int readTimeoutMsec,
                         final int connectTimeoutMsec,
                         final int keepAliveTimeoutMsec,
                         final int keepAliveCount,
                         final TimeUnit timeoutUnit) {
        this(ensembleType, stableTimeout, readTimeoutMsec,
                connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                timeoutUnit);
        this.quorumSize = quorumSize;
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

        ClassLoader cl = getClass().getClassLoader();
        return EnsembleFactory.createEnsemble(
                this.ensembleType, id, quorumSize, this.stableTimeoutMsec,
                this.timeoutUnit, this.quorumServerList,
                this.readTimeoutMsec,
                this.connectTimeoutMsec, this.keepAliveTimeoutMsec,
                this.keepAliveCount,
                cl.getResource(this.keyStore.get(0)).getFile(),
                this.keyPassword.get(0),
                cl.getResource(this.trustStore.get(0)).getFile(),
                this.trustPassword.get(0), this.trustStoreCAAlias);
    }

    public Ensemble createEnsemble(final Long id) throws
            ElectionException {
        return createEnsemble(id, this.quorumSize);
    }

    public void verifyPrintHelper(final Ensemble parentEnsemble,
                                  final Ensemble movedEnsemble,
                                  final Ensemble doneEnsemble) {
        LOG.warn("verified " + parentEnsemble + "->" + movedEnsemble
                + " : election[" +
                EnsembleHelpers.getSidWithServerStateStr(
                        ImmutablePair.of(
                                movedEnsemble.getFleToRun().getId(),
                                movedEnsemble.getFleToRun().getState()))
                + "] -> " + doneEnsemble + ", leader: "
                + (doneEnsemble.getLeaderLoopResult().values().isEmpty() ?
                    " NONE" : doneEnsemble.getLeaderLoopResult().values()
                .iterator().next().getLeader()));
    }
}
