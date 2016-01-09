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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class VoteViewChange implements VoteViewRead {
    private final Long mySid;   // Sid of this server

    public VoteViewChange(final long mySid) {
        this.mySid = mySid;
    }

    public long getId() {
        return this.mySid;
    }

    public void setServerState(final QuorumPeer.ServerState serverState)
            throws InterruptedException, ExecutionException {
        updateSelfVote(this.getSelfVote().setServerState(serverState));
    }

    public Future<Void> increaseElectionEpoch()
            throws InterruptedException, ExecutionException {
        return updateSelfVote(this.getSelfVote().increaseElectionEpoch());
    }

    public Future<Void> setElectionEpoch(final Vote other)
            throws InterruptedException, ExecutionException {
        return updateSelfVote(this.getSelfVote().setElectionEpoch(other));
    }

    public Future<Void> catchUpToVote(final Vote other)
            throws InterruptedException, ExecutionException {
        return updateSelfVote(this.getSelfVote().catchUpToVote(other));
    }

    public Future<Void> quorumPeerVoteSet(final Vote other, final long sid)
            throws InterruptedException, ExecutionException {
        return updateSelfVote(Vote.quorumPeerVoteSet(other, sid));
    }

    public abstract Future<Void> updateSelfVote(final Vote vote)
            throws InterruptedException, ExecutionException;

    /**
     * Here for testing purposes.
     * @param vote
     * @return
     */
    public abstract Future<Void> msgRx(final Vote vote);
}
