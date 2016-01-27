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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class EnsembleFLEMockBcast extends EnsembleMockBcast {
    private static final Logger LOG
            = LoggerFactory.getLogger(EnsembleFLEMockBcast.class.getClass());

    public EnsembleFLEMockBcast(long id, int quorumSize, int stableTimeout,
                                TimeUnit stableTimeoutUnit)
            throws ElectionException {
        super(id, quorumSize, stableTimeout, stableTimeoutUnit);
    }

    public EnsembleFLEMockBcast(
            final Ensemble parentEnsemble, final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                    flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long,
                    QuorumPeer.ServerState>>>
                    partitionedQuorumArg,
            final int stableTimeout, final TimeUnit stableTimeoutUnit)
            throws ElectionException {
        super(parentEnsemble, quorumCnxMeshArg, flatQuorumWithState,
                partitionedQuorumArg, stableTimeout, stableTimeoutUnit);
    }

    @Override
    protected Vote setElectionEpoch(final Vote vote,
                                    final long visibleQuorumVoteCount) {
        return vote;
    }

    @Override
    public Ensemble createEnsemble(
            final Ensemble parentEnsemble,
            final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                    flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long,
                    QuorumPeer.ServerState>>>
                    partitionedQuorumArg) throws ElectionException {
        return new EnsembleFLEMockBcast(parentEnsemble, quorumCnxMeshArg,
                flatQuorumWithState, partitionedQuorumArg,
                stableTimeout, stableTimeoutUnit);
    }

    /**
     * OVerriden to return FLEV2WrapForFLE
     * @param sid
     * @param quorumVerifier
     * @return
     */
    @Override
    protected FLEV2BcastWrapper createFLEV2BcastWrapper(
            final long sid, final QuorumVerifier quorumVerifier) {
        final VoteViewMockBcast voteViewMockBcast
                = new VoteViewMockBcast(sid, getMockQuorumBcast());
        return new FLEV2WrapForFLE(sid, QuorumPeer.LearnerType.PARTICIPANT,
                quorumVerifier, voteViewMockBcast,
                voteViewMockBcast, stableTimeout, stableTimeoutUnit);
    }
}
