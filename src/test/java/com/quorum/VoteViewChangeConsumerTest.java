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
import com.quorum.util.Predicate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;

public class VoteViewChangeConsumerTest extends VoteViewConsumerTestBase {
    private Predicate<Collection<Vote>> testPredicate = null;
    @Override
    protected VoteViewConsumer createConsumer()
            throws InterruptedException, ExecutionException {
        return (VoteViewConsumer)voteViewWrapper.createChangeConsumer();
    }

    @Override
    protected void verifyConsumerConsume(
            final VoteViewConsumer consumer)
            throws InterruptedException, ExecutionException {
        if (!(consumer instanceof VoteViewChangeConsumer)) {
            throw new IllegalAccessError("invalid consumer");
        }

        Queue<Vote> queue = new LinkedBlockingQueue<>();
        Collection<Vote> votes;
        while((votes = consumeHelper(consumer)) != null) {
                queue.addAll(votes);
        }

        Map<Long, Vote> inboundMap = new HashMap<>();
        while (!queue.isEmpty()) {
            Vote v = queue.poll();
            inboundMap.put(v.getSid(), v);
        }

        assertEquals(voteUpdateMap, inboundMap);
    }

    @Override
    protected Collection<Vote> consumeHelper(
            final VoteViewConsumer consumer)
            throws InterruptedException, ExecutionException {
        if (!(consumer instanceof VoteViewChangeConsumer)) {
            throw new IllegalAccessError("invalid consumer");
        }
        if (testPredicate != null) {
            return ((VoteViewChangeConsumer)consumer)
                    .consume(POLL_TIMEOUT_MSEC, TIME_UNIT, testPredicate);
        }
        return ((VoteViewChangeConsumer)consumer).consume(POLL_TIMEOUT_MSEC,
                TIME_UNIT);
    }
}
