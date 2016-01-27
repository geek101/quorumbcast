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
import com.quorum.VoteViewChange;
import com.quorum.VoteViewConsumerCtrl;
import com.quorum.flexible.QuorumVerifier;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FLEV2WrapForFLE extends FLEV2BcastWrapper {
    private final FastLeaderElection fastLeaderElection;
    public FLEV2WrapForFLE(long mySid, QuorumPeer.LearnerType learnerType,
                           QuorumVerifier quorumVerifier,
                           VoteViewChange voteViewChange,
                           VoteViewConsumerCtrl voteViewConsumerCtrl,
                           int stableTimeout, TimeUnit stableTimeoutUnit) {
        super(mySid, learnerType, quorumVerifier, voteViewChange,
                voteViewConsumerCtrl, stableTimeout, stableTimeoutUnit);
        fastLeaderElection = new FastLeaderElection(mySid, learnerType,
                quorumVerifier, voteViewConsumerCtrl, voteViewChange);

    }

    @Override
    public Future<Vote> runLeaderElection(Collection<Vote> votes)
            throws ElectionException, InterruptedException,
            ExecutionException {
        FutureTask<Vote> futureTask = new FutureTask<>(
                new Callable<Vote>()  {
                    @Override
                    public Vote call() throws ElectionException,
                            InterruptedException,
                            ExecutionException {
                        return fastLeaderElection.lookForLeader(
                                getSelfVote().getPeerEpoch(),
                                getSelfVote().getZxid());
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    @Override
    public void waitForVotesRun(final Map<Long, Vote> voteMap)
            throws InterruptedException, ExecutionException {
        fastLeaderElection.waitForVotesRun(voteMap);
    }

    @Override
    public void verifyNonTermination() {
        assertNotEquals("non terminating did run sid: " + getId(),
                null, fastLeaderElection.getLastLookForLeader());
        assertEquals("non terminating never tried stability run sid: "
                + getId(), null, fastLeaderElection.couldTerminate());
    }
}
