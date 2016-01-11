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
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FLEV2BcastNoThreadWrapper extends AbstractFLEV2Wrapper {
    private static final Logger LOG
            = LoggerFactory.getLogger(FLEV2BcastWrapper.class.getClass());
    final QuorumVerifier quorumVerifier;
    final ExecutorService executorService;
    final FLEV2BcastNoThreadWrapper self;
    public FLEV2BcastNoThreadWrapper(final long mySid,
                             final QuorumPeer.LearnerType learnerType,
                             final QuorumVerifier quorumVerifier,
                             final VoteViewChange voteViewChange,
                             final VoteViewConsumerCtrl voteViewConsumerCtrl,
                             final int stableTimeout,
                             final TimeUnit stableTimeoutUnit) {
        super(mySid, learnerType, quorumVerifier, voteViewChange,
                voteViewConsumerCtrl, stableTimeout, stableTimeoutUnit);
        this.quorumVerifier = quorumVerifier;
        this.executorService = null;
        this.self = this;
    }

    /**
     * not implemented will throw runtime exception.
     * @param votes
     * @return
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public Future<Vote> runLeaderElection(
            final Collection<Vote> votes) throws ElectionException,
            InterruptedException, ExecutionException {
        throw new NotImplementedException("not implemented");
    }

    @Override
    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }

    @Override
    public void shutdown() {
    }
}
