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
package com.quorum.helpers;

import com.quorum.Vote;
import com.quorum.VoteViewConsumer;
import com.quorum.util.NotNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InvalidClassException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class VoteViewConsumerTestBase {
    protected static final Logger LOG
            = LoggerFactory.getLogger(VoteViewConsumerTestBase.class.getName());
    private static final Long MY_SID = 1L;
    protected static final Integer POLL_TIMEOUT_MSEC = 100;
    protected static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    final long rgenseed = System.currentTimeMillis();
    final Random random = new Random(rgenseed);

    protected VoteViewBaseWrapper voteViewWrapper;

    protected List<Vote> voteUpdates = new LinkedList<>();
    protected Map<Long, Vote> voteUpdateMap = new HashMap<>();

    @Before
    public void setup() {
        voteViewWrapper = new VoteViewBaseWrapper(MY_SID);
    }

    @After
    public void tearDown() {
        voteViewWrapper = null;
    }

    /**
     * Test without any vote, this tests VoteView rather than
     * consumer.
     * @throws Exception
     */
    @Test
    public void testNullVote() throws Exception {
        assertEquals(null, voteViewWrapper.getSelfVote());
    }

    /**
     * Set a self vote first time and ensure it is set.
     * Remember updateSelfVote is asynchronous but this function
     * waits for it to be done.
     * @throws Exception
     */
    @Test
    public void testSelfVote() throws Exception {
        Vote v = updateSelfVote();
        assertEquals(v, voteViewWrapper.getSelfVote());
    }

    /**
     * Update the self vote twice and ensure last one is verified.
     * @throws Exception
     */
    @Test
    public void testSelfVoteTwice() throws Exception {
        testSelfVote();
        testSelfVote();
    }

    /**
     * Verify consumer count limit of 1.
     * @throws Throwable
     */
    @Test(expected = RuntimeException.class)
    public void testTwoConsumerException() throws Throwable {
        VoteViewConsumer consumer =
                createConsumerAndInitTest();
        try {
            createConsumerAndInitTest();
        } catch (ExecutionException exp) {
            if (exp.getCause() == null) {
                throw new NullPointerException("cause is null: " + exp);
            }
            if (exp.getCause() instanceof RuntimeException) {
                throw exp.getCause();
            }

            throw new InvalidClassException("invalid cause: " +
                    exp.getCause());
        }
        releaseConsumer(consumer);
    }

    /**
     * Verify behaviour of consumer at init time when no vote is set.
     * Expects return of empty set rather than null.
     * @throws Exception
     */
    @Test
    public void testConsumerInitEmptyOnNoVote() throws Exception {
        testNullVote();
        releaseConsumer(createConsumerAndInitTest());
    }

    /**
     * Verify that consumer will timeout when no vote is received
     * since its start.
     * @throws Exception
     */
    @Test
    public void testConsumerTimeoutOnNoVote() throws Exception {
        testNullVote();
        VoteViewConsumer consumer =
                createConsumerAndInitTest();
        Collection<Vote> votes = consumeHelper(consumer);
        assertEquals(null, votes);
        releaseConsumer(consumer);
    }

    /**
     * Test that consumer will return the only self vote when
     * called for the first time on a view with just the self vote.
     * @throws Exception
     */
    @Test
    public void testSelfVoteConsumerInit() throws Exception {
        releaseConsumer(selfVoteConsumerInitTest());
    }

    /**
     * Test that consumer will return th new vote when it is updated
     * @throws Exception
     */
    @Test
    public void testSelfVoteUpdateOnceAfterInit() throws Exception {
        VoteViewConsumer consumer =
                selfVoteConsumerInitTest();
        Vote secondSelfVote = updateSelfVote();
        Collection<Vote> votes = consumeHelper(consumer);
        assertEquals(1, votes.size());
        assertTrue("vote verify", votes.contains(secondSelfVote));
    }

    /**
     * Send a vote from a random peer and ensure the map has both self and
     * the random peers.
     * @throws Exception
     */
    @Test
    public void testTwoVoteConsumer() throws Exception {
        VoteViewConsumer consumer
                = twoVoteConsumerTest();
        releaseConsumer(consumer);
    }

    /**
     * Test same vote (non self vote) will cause consumer to timeout after
     * verifying testVoteConsumer().
     * @throws Exception
     */
    @Test
    public void testSameVoteConsumerTimeout() throws Exception {

        ImmutablePair<Vote, VoteViewConsumer> retPair
                = voteConsumerTestAndGetOtherVote();
        Vote otherVote = retPair.getLeft();
        VoteViewConsumer consumer = retPair.getRight();

        // Send voter other than self vote again and verify consumer will
        // timeout
        postVote(otherVote);

        assertEquals(null, consumeHelper(consumer));
        releaseConsumer(consumer);
    }

    /**
     * Test two votes and change the vote for the non self sid server
     * and send and verify the update.
     * @throws Exception
     */
    @Test
    public void testDifferentVoteConsumer() throws Exception {
        ImmutablePair<Vote, VoteViewConsumer> retPair
                = voteConsumerTestAndGetOtherVoteAndGetDiffVote();
        Vote diffOtherVote = retPair.getLeft();
        VoteViewConsumer consumer = retPair.getRight();

        // Send a different vote for current vote and verify
        postVote(diffOtherVote);

        verifyConsumerConsume(consumer);
        releaseConsumer(consumer);
    }

    /**
     * Modify the non self sid vote twice and send twice and ensure
     * the both votes are received for stream case and last vote
     * for change case.
     * @throws Exception
     */
    @Test
    public void testSameDifferentVoteConsumer() throws Exception {
        ImmutablePair<Vote, VoteViewConsumer> retPair
                = voteConsumerTestAndGetOtherVoteAndGetDiffVote();
        Vote diffOtherVote = retPair.getLeft();
        VoteViewConsumer consumer = retPair.getRight();

        // ignore the past.
        voteUpdateReset();

        // Send self vote again in sync mode so for stream consumer
        // it will be stored in the queue.
        postVoteSync(diffOtherVote);

        // Get a second different vote.
        Vote sameDiffOtherVote = getRandomVote(diffOtherVote.getSid());
        while (sameDiffOtherVote.match(diffOtherVote)) {
            sameDiffOtherVote = getRandomVote(diffOtherVote.getSid());
        }

        assertTrue("diff vote sid mis match",
                sameDiffOtherVote.getSid() == diffOtherVote.getSid());

        // Send not self vote again and verify consumer will timeout
        postVote(sameDiffOtherVote);

        verifyConsumerConsume(consumer);
        releaseConsumer(consumer);
    }

    /**
     * Simulate a remove vote rx for non self sid and verify that.
     * @throws Exception
     */
    @Test
    public void testDifferentVoteRemoveConsumer() throws Exception {
        ImmutablePair<Vote, VoteViewConsumer> retPair
                = voteConsumerTestAndRemoveVoteNoVerify();
        verifyConsumerConsume(retPair.getRight());
        releaseConsumer(retPair.getRight());
    }

    /**
     * Send a vote for a removed sid.
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void testAddAfterRemoveConsumer() throws Exception {
        ImmutablePair<Vote, VoteViewConsumer> retPair
                = voteConsumerTestAndRemoveVoteNoVerify();
        Vote diffRemoveVote = retPair.getLeft();
        Vote diffNonRemoveVote = getRandomVote(diffRemoveVote.getSid());

        postVoteSync(diffNonRemoveVote);

        // We sent remove vote and add vote afterwards.
        verifyConsumerConsume(retPair.getRight());
        releaseConsumer(retPair.getRight());
    }

    private ImmutablePair<Vote, VoteViewConsumer>
    voteConsumerTestAndRemoveVoteNoVerify() throws InterruptedException,
            ExecutionException {
        ImmutablePair<Vote, VoteViewConsumer> retPair
                = voteConsumerTestAndGetOtherVoteAndGetDiffVote();
        Vote diffOtherVote = retPair.getLeft();
        VoteViewConsumer consumer = retPair.getRight();

        // ignore the past.
        voteUpdateReset();

        // Send self vote again in sync mode with Remove set so for stream
        // consumer it will be stored in the queue.
        diffOtherVote.setRemove();
        postVoteSync(diffOtherVote);

        return ImmutablePair.of(diffOtherVote, consumer);
    }

    private ImmutablePair<Vote, VoteViewConsumer>
            voteConsumerTestAndGetOtherVote() throws InterruptedException,
            ExecutionException {
        VoteViewConsumer consumer
                = twoVoteConsumerTest();
        Vote otherVote = null;
        for (final Vote v : voteUpdateMap.values()) {
            if (v.getSid() != MY_SID) {
                otherVote = v;
                break;
            }
        }

        assertTrue("othervote null", otherVote != null);
        voteUpdateReset();
        return ImmutablePair.of(otherVote, consumer);
    }

    private ImmutablePair<Vote, VoteViewConsumer>
    voteConsumerTestAndGetOtherVoteAndGetDiffVote() throws InterruptedException,
            ExecutionException {
        Vote otherVote = null;
        VoteViewConsumer consumer = null;
        ImmutablePair<Vote, VoteViewConsumer> retPair
                = voteConsumerTestAndGetOtherVote();
        otherVote = retPair.getLeft();
        consumer = retPair.getRight();

        Vote diffOtherVote = getRandomVote(otherVote.getSid());
        while (diffOtherVote.match(otherVote)) {
            diffOtherVote = getRandomVote(otherVote.getSid());
        }

        assertTrue("diff vote sid mis match",
                otherVote.getSid() == diffOtherVote.getSid());
        return ImmutablePair.of(diffOtherVote, consumer);
    }

    /**
     * Override this in the concrete test class.
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected abstract VoteViewConsumer createConsumer()
            throws InterruptedException, ExecutionException;

    protected abstract void verifyConsumerConsume(
            VoteViewConsumer consumer)
            throws InterruptedException, ExecutionException;

    protected abstract Collection<Vote> consumeHelper(
            final VoteViewConsumer consumer)
            throws InterruptedException, ExecutionException;

    /**
     * Self vote and a different vote for a different sid.
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected VoteViewConsumer twoVoteConsumerTest()
            throws InterruptedException, ExecutionException {
        VoteViewConsumer consumer
                = selfVoteConsumerInitTest();
        voteUpdateReset();
        updateSelfVote();
        postVote();
        verifyConsumerConsume(consumer);
        return consumer;
    }

    protected VoteViewConsumer selfVoteConsumerInitTest()
            throws InterruptedException, ExecutionException {
        Vote v = updateSelfVote();
        VoteViewConsumer consumer = createConsumer();
        Collection<Vote> votes = consumeHelper(consumer);
        assertEquals(1, votes.size());
        assertTrue("vote verify", votes.contains(v));
        return consumer;
    }

    protected VoteViewConsumer createConsumerAndInitTest()
            throws InterruptedException, ExecutionException {
        final VoteViewConsumer consumer = createConsumer();
        Collection<Vote> votes = consumeHelper(consumer);
        assertEquals(0, votes.size());
        return consumer;
    }

    protected void releaseConsumer(
            final VoteViewConsumer consumer) {
        voteViewWrapper.removeConsumer(consumer);
    }

    protected Vote updateSelfVote()
            throws InterruptedException, ExecutionException {
        Vote v = getSelfRandomVote();
        voteViewWrapper.updateSelfVote(v).get();
        voteUpdate(v);
        return v;
    }

    protected Vote postVote() throws InterruptedException, ExecutionException {
        return postVote(getRandomVote());
    }

    protected Vote postVote(final long sid) throws InterruptedException,
            ExecutionException {
        return postVote(getRandomVote(sid));
    }

    protected Vote postVote(final Vote v) throws InterruptedException,
            ExecutionException {
        voteViewWrapper.msgRxHelper(v);
        voteUpdate(v);
        return v;
    }

    protected Vote postVoteSync(final Vote v)
            throws InterruptedException, ExecutionException {
        voteViewWrapper.msgRxHelper(v).get();
        voteUpdate(v);
        return v;
    }

    private Vote getSelfRandomVote() {
        return new Vote(random.nextLong(), random.nextLong(),
                voteViewWrapper.getId());
    }

    private Vote getRandomVote() {
        return getRandomVote(random.nextLong());
    }

    private Vote getRandomVote(final long sid) {
        return new Vote(random.nextLong(), random.nextLong(), sid);
    }

    private void voteUpdate(final Vote v) {
        voteUpdates.add(v);
        if (v.isRemove()) {
            voteUpdateMap.remove(v.getSid());
            return;
        }
        voteUpdateMap.put(v.getSid(), v);
    }

    private void voteUpdateReset() {
        voteUpdates.clear();
    }
}
