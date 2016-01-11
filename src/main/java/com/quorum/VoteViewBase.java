package com.quorum;

import com.quorum.util.ErrCallback;
import com.quorum.util.NotNull;
import com.quorum.util.Predicate;
import com.quorum.util.ZKTimerTask;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class VoteViewBase extends VoteViewChange
        implements VoteViewConsumerCtrl {
    protected static final Logger LOG = LoggerFactory.getLogger(
            VoteViewBase.class.getName());

    /**
     * Do not increase the max thread count. Current design assumes a single
     * thread executor.
     */
    public static final int MAX_THREAD_COUNT = 1;
    protected static final int MAX_CONSUMER_COUNT = 1;
    protected static final long TIMEOUT_MSEC = 10;

    protected final ExecutorService group;
    protected final VoteViewBase self;

    protected ZKTimerTask<Void> timerTask;

    /**
     * Used by Broadcast module to store all the incoming votes.
     */
    protected final ConcurrentLinkedQueue<Vote> inboundVoteQueue
            = new ConcurrentLinkedQueue<>();

    /**
     * Contains the all known Quorum Peer votes that are received.
     */
    protected final Map<Long, Vote> voteMap = new ConcurrentHashMap<>();

    /**
     * List of all known consumers who are interested in our data.
     */
    protected final Set<VoteViewProducer> streamConsumers = new HashSet<>();
    protected final Set<VoteViewProducer> changeConsumers = new HashSet<>();

    private final class ErrorCb implements ErrCallback {
        @Override
        public void caughtException(Exception exp) throws Exception {
            LOG.error("Cannot have exceptions here: {}", exp);
            throw exp;
        }
    }

    protected VoteViewBase(final long mySid,
                           final EventLoopGroup eventLoopGroup) {
        super(mySid);
        this.group = eventLoopGroup;
        this.self = this;
    }

    public VoteViewBase(final long mySid) {
        this(mySid, new NioEventLoopGroup(MAX_THREAD_COUNT,
                Executors.newSingleThreadExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable target) {
                        final Thread thread = new Thread(target);
                        LOG.debug("Creatin new worker thread");
                        thread.setUncaughtExceptionHandler(
                                new Thread.UncaughtExceptionHandler() {
                                    @Override
                                    public void uncaughtException(Thread t,
                                                                  Throwable e) {
                                        LOG.error("Uncaught Exception", e);
                                        System.exit(1);
                                    }
                                });
                        return thread;
                    }
                })));
    }

    public VoteViewBase(final long mySid, final ExecutorService group) {
        super(mySid);
        this.group = group;
        this.self = this;
    }

    /**
     * API to get the current Vote of the system.
     * @return If Vote exists else it will be null.
     */
    public final Vote getSelfVote() {
        if (!voteMap.containsKey(getId())) {
            return null;
        }
        return voteMap.get(getId()).copy();
    }

    /**
     * API to update the current Vote of self synchronously,
     * @param vote Vote to be sent
     */
    public Future<Void> updateSelfVote(final Vote vote)
            throws InterruptedException, ExecutionException {
        return msgRx(vote);
    }

    /**
     * creates a change consumer with current map, runs this on the
     * same thread where votes are processed, hence it is safe for consumption.
     * @return consumer of VoteViewChangeConsumer type
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public VoteViewChangeConsumer createChangeConsumer()
            throws InterruptedException, ExecutionException {
        FutureTask<VoteViewChangeConsumer> futureTask = new FutureTask<>(
                new Callable<VoteViewChangeConsumer>() {
                    @Override
                    public VoteViewChangeConsumer call() {
                        return createChangeConsumerSync();
                    }
                });
        submitTask(futureTask);
        return futureTask.get();
    }


    private VoteViewChangeConsumer createChangeConsumerSync() {
        synchronized (this) {
            VoteViewChangeConsumerImpl consumer = null;
            if (changeConsumers.size() == MAX_CONSUMER_COUNT) {
                final String errStr = "Invalid usage, consumer count " +
                        "reached: " + changeConsumers.size();
                LOG.error(errStr);
                throw new RuntimeException(errStr);
            }
            consumer = new VoteViewChangeConsumerImpl();
            changeConsumers.add(consumer);

            startTimer_();
            return consumer;
        }
    }

    /**
     * Create a stream consumer, incoming votes are streamed when
     * they can change the view here.
     * @return consumer of VoteviewStreamConsumer
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public VoteViewStreamConsumer createStreamConsumer()
            throws InterruptedException, ExecutionException {
        FutureTask<VoteViewStreamConsumer> futureTask = new FutureTask<>(
                new Callable<VoteViewStreamConsumer>() {
                    @Override
                    public VoteViewStreamConsumer call() throws
                            InterruptedException {
                        return createStreamConsumerSync();
                    }
                });
        submitTask(futureTask);
        return futureTask.get();
    }

    private VoteViewStreamConsumer createStreamConsumerSync()
            throws InterruptedException {
        synchronized (this) {
            VoteViewStreamConsumerImpl consumer = null;
            if (streamConsumers.size() == MAX_CONSUMER_COUNT) {
                final String errStr = "Invalid usage, consumer count " +
                        "reached: " + streamConsumers.size();
                LOG.error(errStr);
                throw new RuntimeException(errStr);
            }
            consumer = new VoteViewStreamConsumerImpl();
            streamConsumers.add(consumer);
            doRunNow();
            return consumer;
        }
    }

    /**
     * Release the given consumer.
     * @param voteViewConsumer consumer which was created before.
     */
    public void removeConsumer(final VoteViewConsumer voteViewConsumer) {
        NotNull.check(voteViewConsumer, "arg cannot be null", LOG);
        synchronized (this) {
            if (voteViewConsumer instanceof VoteViewChangeConsumerImpl) {
                changeConsumers.remove(voteViewConsumer);
            } else if (voteViewConsumer instanceof VoteViewStreamConsumerImpl) {
                streamConsumers.remove(voteViewConsumer);
                // TODO: Since only one consumer at a time this works,
                // ref-count when consumer count > 1
                stopTimer_();
            } else {
                final String errStr = "cannot handle object type: " +
                        voteViewConsumer.getClass().getName();
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
        }
    }

    /**
     * Called from msgRxCb callback.
     * Will trigger a optimistic run of doRun() if in-bound vote
     * change change the view.
     * @param vote
     */
    @Override
    public Future<Void> msgRx(final Vote vote) {
        NotNull.check(vote, "Received vote cannot be null", LOG);
        if (canChangeView(vote)) {
            inboundVoteQueue.add(vote);

            // Lets trigger doRun optimistically to minimize latency of
            // vote reception at the consumer end.
            return doRunNow();
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Schedule doRun() now.
     * @return future , ignored here.
     */
    protected Future<Void> doRunNow() {
        FutureTask<Void> futureTask = new FutureTask<>(
                new Callable<Void>() {
                    @Override
                    public Void call() throws InterruptedException {
                        doRun();
                        return null;
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    /**
     * Main loop that runs as part of EventLoopGroup's executor.
     * This loop is serialized with few config methods above. Lot of
     * contention is not expected.
     * Things done here are:
     * 1. Process the list of incoming Votes and prune the list but
     *    do not update the view yet.
     * 2. Walk the consumer set and add this prune list of votes to them.
     * 3. Apply the prune list to the view.
     */
    protected void doRun() throws InterruptedException {
        synchronized (this) {
            doRun_();
        }
    }

    private void doRun_() throws InterruptedException {
        final Map<Long, Vote> dedupMap = new HashMap<>();
        // inboundVoteQueue is serialized per Server so use the
        // last in the queue for each Server.
        while(!inboundVoteQueue.isEmpty()) {
            Vote vote = inboundVoteQueue.poll();
            dedupMap.put(vote.getSid(), vote);
        }

        doRun_(Collections.unmodifiableCollection(dedupMap.values()));
    }

    /**
     * Must be called with locked/synchronized
     * @throws InterruptedException
     */
    protected void doRun_(final Collection<Vote> votes) throws
            InterruptedException {
        if (!votes.isEmpty()) {
            pruneInboundVoteQueue(votes);
            processConsumers(votes, streamConsumers);
            updateVoteView(votes);
        }
        processConsumers(voteMap.values(), changeConsumers);
    }

    /**
     * Remove votes that are not different that what is in the view.
     * TODO: Verify the use of match.
     * @param votes incoming votes
     */
    private void pruneInboundVoteQueue(final Collection<Vote> votes) {
        // Remove vote that cannot change the view
        for (Iterator<Vote> it = votes.iterator(); it.hasNext();) {
            Vote vote = it.next();
            Vote lastVote = voteMap.get(vote.getSid());
            if (lastVote != null && lastVote.match(vote) &&
                    !vote.isRemove()) {
                // Same vote Rx, drop it.
                LOG.debug("Rx vote: " + vote + " same as last vote: "
                        + lastVote);
                it.remove();
            }
        }
    }

    /***
     * Helper for both types of consumers.
     * @param votes incoming pruned votes
     * @throws InterruptedException
     */
    private void processConsumers(
            final Collection<Vote> votes,
            final Set<VoteViewProducer> consumers)
            throws InterruptedException {
        for (VoteViewProducer consumer : consumers) {
            consumer.produce(votes);
        }
    }

    /**
     * Replace the vote if this is not a remove action.
     * If the vote is marked remove then remove the current vote in the map
     * if one exists.
     * @param votes incoming pruned votes
     * @return boolean , if there was a change return true else false.
     */
    private boolean updateVoteView(final Collection<Vote> votes) {
        boolean viewChange = false;
        for (final Vote vote : votes) {
            if (!vote.isRemove()) {
                voteMap.put(vote.getSid(), vote);
                viewChange = true;
                continue;
            }

            if (voteMap.containsKey(vote.getSid())) {
                LOG.info("Removing vote from view: " + vote);
                voteMap.remove(vote.getSid());
                viewChange = true;
                continue;
            }
        }
        return viewChange;
    }

    private void startTimer_() {
        if (timerTask != null) {
            return;
        }

        timerTask = new ZKTimerTask<Void>(
                (ScheduledExecutorService)group, TIMEOUT_MSEC, new ErrorCb()) {
            @Override
            public Void doRun() throws Exception {
                try {
                    self.doRun();
                } catch (Exception exp) {
                    LOG.error("{}", exp);
                    throw exp;
                }
                return null;
            }
        };
        timerTask.start();
    }

    private void stopTimer_() {
        if (timerTask == null) {
            return;
        }

        timerTask.stop();
        timerTask = null;
    }

    private class VoteViewStreamConsumerImpl extends VoteViewStreamConsumer
             implements VoteViewProducer  {
        private final LinkedBlockingQueue<Collection<Vote>> queue
                = new LinkedBlockingQueue<>();

        public VoteViewStreamConsumerImpl() throws InterruptedException {
            queue.put(voteMap.values());
        }

        public Collection<Vote> consume(int timeout, TimeUnit unit)
                throws InterruptedException {
            return queue.poll(timeout, unit);
        }

        @Override
        public void produce(final Collection<Vote> votes)
                throws InterruptedException {
            queue.put(votes);
        }
    }

    private class VoteViewChangeConsumerImpl extends VoteViewChangeConsumer
            implements VoteViewProducer {
        private final Lock lock;
        private final Condition cv;

        /**
         * Everything below are protected by the above lock and condition
         * variable.
         */
        private final Map<Long, Vote> consumerVoteMap;
        private boolean initCalled = false;

        /**
         * Default predicate which does not timeout.
         */
        private final Predicate<Collection<Vote>> defaultPredicate
                = new Predicate<Collection<Vote>>() {
            @Override
            public Boolean call(Collection<Vote> param) {
                if (param.size() != consumerVoteMap.size()) {
                    return true;
                }

                for (final Vote vote : param) {
                    if (VoteView.canChangeView(consumerVoteMap, vote)) {
                        return true;
                    }
                }
                return false;
            }
        };

        private Predicate<Collection<Vote>> changePredicate
                = defaultPredicate;

        /**
         * Copy contents of current votes.
         */
        public VoteViewChangeConsumerImpl() {
            consumerVoteMap = new HashMap<>(voteMap);
            lock = new ReentrantLock();
            cv = lock.newCondition();
        }

        /**
         * Blocking version of getting the current vote view.
         * @return null on timeout, else constant collection of vote view
         */
        public Collection<Vote> consume(int timeout, TimeUnit unit)
                throws InterruptedException {
            return consume(timeout, unit, defaultPredicate);
        }

        /**
         * Will return a new set of votes or null if timeout based on
         * predicate.
         * @param timeout
         * @param unit
         * @param changePredicate idempotent and thread safe predicate
         * @return
         * @throws InterruptedException
         */
        public Collection<Vote> consume(
                final int timeout, final TimeUnit unit,
                final Predicate<Collection<Vote>> changePredicate)
                throws InterruptedException {
            NotNull.check(changePredicate, "change predicate is null", LOG);
            lock.lock();
            try {
                if (!initCalled) {
                    initCalled = true;
                    if (changePredicate == defaultPredicate ||
                            changePredicate.call(consumerVoteMap.values())) {
                        return consumerVoteMap.values();
                    }
                }

                // producer will use the change predicate.
                this.changePredicate = changePredicate;
                return consume_(timeout, unit);
            } finally {
                this.changePredicate = null;
                lock.unlock();
            }
        }

        /**
         * Should be called with lock taken
         * @param timeout
         * @param unit
         * @return
         */
        protected Collection<Vote> consume_(
                final int timeout, final TimeUnit unit) {
            try {
                if (cv.await(timeout, unit)) {
                    return Collections.unmodifiableCollection(
                            consumerVoteMap.values());
                } else {
                    return null;
                }
            } catch (InterruptedException exp) {
                LOG.error("change consumer interrupted, exp: {}", exp);
                return null;
            }
        }

        @Override
        public void produce(final Collection<Vote> votes) {
            if (lock.tryLock()) {
                try {
                    if (changePredicate != null &&
                            changePredicate.call(votes)) {
                        produce_(votes);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        private void produce_(final Collection<Vote> votes) {
            consumerVoteMap.clear();
            for (final Vote vote : votes) {
                consumerVoteMap.put(vote.getSid(), vote);
            }

            cvSignal_();
        }

        private void cvSignal_() {
            cv.signal();
        }
    }

    private interface VoteViewProducer {
        void produce(final Collection<Vote> votes) throws InterruptedException;
    }

    protected boolean canChangeView(final Vote vote) {
        return VoteView.canChangeView(this.voteMap, vote);
    }

    protected static boolean canChangeView(final Map<Long, Vote> voteMap,
                                         final Vote vote) {
        final Vote currentVote = voteMap.get(vote.getSid());
        if (currentVote == null || currentVote.isRemove()) {
            // If Remove is set then it wont have any affect.
            return !vote.isRemove();
        } else {
            if (currentVote.match(vote)) {
                // LOG.info("No Change vote: " + vote + " currentVote: "
                        // + currentVote);
                return vote.isRemove();
            } else {
                //LOG.info("Change vote: " + vote + " currentVote: "
                        //+ currentVote);
                return true;
            }
        }
    }

    protected void submitTask(FutureTask task) {
        group.submit(task);
    }
}
