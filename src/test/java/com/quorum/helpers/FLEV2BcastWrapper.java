package com.quorum.helpers;

import com.quorum.ElectionException;
import com.quorum.QuorumPeer;
import com.quorum.Vote;
import com.quorum.VoteViewChange;
import com.quorum.VoteViewConsumerCtrl;
import com.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class FLEV2BcastWrapper extends AbstractFLEV2Wrapper {
    private static final Logger LOG
            = LoggerFactory.getLogger(FLEV2BcastWrapper.class.getClass());
    final QuorumVerifier quorumVerifier;
    final ExecutorService executorService;
    final FLEV2BcastWrapper self;
    public FLEV2BcastWrapper(final long mySid,
                            final QuorumPeer.LearnerType learnerType,
                            final QuorumVerifier quorumVerifier,
                            final VoteViewChange voteViewChange,
                            final VoteViewConsumerCtrl voteViewConsumerCtrl,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit) {
        super(mySid, learnerType, quorumVerifier, voteViewChange,
                voteViewConsumerCtrl, stableTimeout, stableTimeoutUnit);
        this.quorumVerifier = quorumVerifier;
        this.executorService = Executors.newSingleThreadExecutor(
                new ThreadFactory() {
            @Override
            public Thread newThread(Runnable target) {
                final Thread thread = new Thread(target);
                LOG.debug("Creating new worker thread");
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
        });
        this.self = this;
    }

    /**
     * Run the actual lookForLeader API in a different thread.
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
        FutureTask<Vote> futureTask = new FutureTask<>(
                new Callable<Vote>()  {
                    @Override
                    public Vote call() throws ElectionException,
                            InterruptedException,
                            ExecutionException {
                        return self.lookForLeader(
                                getSelfVote().getPeerEpoch(),
                                getSelfVote().getZxid());
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    @Override
    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }

    protected void submitTask(FutureTask task) {
        executorService.submit(task);
    }
}
