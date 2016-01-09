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

import com.quorum.helpers.VoteViewConsumerTestBase;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VoteViewStreamConsumerTest extends VoteViewConsumerTestBase {
    @Override
    protected VoteViewConsumer createConsumer()
            throws InterruptedException, ExecutionException {
        return (VoteViewConsumer)voteViewWrapper.createStreamConsumer();
    }

    @Override
    protected void verifyConsumerConsume(
            VoteViewConsumer consumer)
            throws InterruptedException, ExecutionException {
        if (!(consumer instanceof VoteViewStreamConsumer)) {
            throw new IllegalAccessError("invalid consumer");
        }
        Queue<Vote> queue = new LinkedBlockingQueue<>();
        Collection<Vote> votes;
        while((votes = consumeHelper(consumer)) != null) {
            queue.addAll(votes);
        }

        assertEquals(voteUpdates.size(), queue.size());
        Map<Long, Vote> updateMap = new HashMap<>();
        for (final Vote v : voteUpdates) {
            updateMap.put(v.getSid(), v);
        }

        Map<Long, Vote> inboundMap = new HashMap<>();
        while (!queue.isEmpty()) {
            Vote v = queue.poll();
            inboundMap.put(v.getSid(), v);
        }

        assertEquals(updateMap, inboundMap);
    }

    @Override
    protected Collection<Vote> consumeHelper(
            final VoteViewConsumer consumer)
            throws InterruptedException, ExecutionException {
        if (!(consumer instanceof VoteViewStreamConsumer)) {
            throw new IllegalAccessError("invalid consumer");
        }
        return ((VoteViewStreamConsumer)consumer).consume(POLL_TIMEOUT_MSEC, TIME_UNIT);
    }
}
