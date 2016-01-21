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

import com.quorum.Vote;
import com.quorum.VoteViewChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MockQuorumBcast extends MockQuorumBcastTestBase {
    private static final Logger LOG
            = LoggerFactory.getLogger(MockQuorumBcast.class);
    private QuorumCnxMesh quorumCnxMesh;
    final Map<Long, VoteViewChange> voteViewMap;

    public MockQuorumBcast(final long id, final int quorumSize,
                           final QuorumCnxMesh quorumCnxMesh) {
        super(id, quorumSize);
        this.quorumCnxMesh = quorumCnxMesh;
        this.voteViewMap = new HashMap<>();
    }

    public void resetQuorumCnxMesh(final QuorumCnxMesh quorumCnxMesh) {
        this.quorumCnxMesh = quorumCnxMesh;
    }

    public boolean connected(final long sid1, final long sid2) {
        return this.quorumCnxMesh.connected(sid1, sid2);
    }

    public boolean isConnectedToAny(final long sid) {
        return this.quorumCnxMesh.isConnectedToAny(sid);
    }

    @Override
    public void broadcast(final Vote vote) {
        synchronized (this) {
            broadcast_(vote);
        }
    }

    public void addVoteViewChange(final VoteViewChange voteView) {
        synchronized (this) {
            if (voteViewMap.containsKey(voteView.getId())) {
                final String errStr = "error registering vote view for sid: " +
                        voteView.getId() + ", already exists";
                LOG.error(errStr);
                throw new RuntimeException(errStr);
            }
            voteViewMap.put(voteView.getId(), voteView);

            for (final long sid : voteViewMap.keySet()) {
                // rx other peer votes.
                if (sid != voteView.getId() &&
                        quorumCnxMesh != null &&
                        quorumCnxMesh.connected(voteView.getId(), sid) &&
                        voteViewMap.get(sid) != null) {
                    voteView.msgRx(voteViewMap.get(sid).getSelfVote());
                }
            }

            // broadcast the vote of the new peer to everyone
            if (voteView.getSelfVote() != null) {
                broadcast_(voteView.getSelfVote());
            }
        }
    }

    /**
     * called with lock held.
     * @param vote
     */
    private void broadcast_(final Vote vote) {
        for (final Map.Entry<Long, VoteViewChange> e : voteViewMap.entrySet()) {
            if (e.getKey() != vote.getSid() &&
                    quorumCnxMesh != null &&
                    quorumCnxMesh.connected(vote.getSid(), e.getKey())) {
                try {
                    e.getValue().msgRx(vote).get();
                } catch (InterruptedException | ExecutionException exp) {
                    LOG.error("failed to update vote, unhandled exp: " + exp);
                    throw new RuntimeException(exp);
                }
            }
        }
    }
}
