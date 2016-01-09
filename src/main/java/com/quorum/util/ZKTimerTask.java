package com.quorum.util;

import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class ZKTimerTask<T> implements Callable<T> {
    private static final Logger LOG
            = LoggerFactory.getLogger(ZKTimerTask.class.getName());
    private final ScheduledExecutorService group;
    private final Long timeoutNanos;
    private final ErrCallback errCb;
    private Future<T> timeoutFuture = null;
    private boolean stopped = true;
    public ZKTimerTask(final ScheduledExecutorService group, final long timeoutMsec,
                       final ErrCallback errCb) {
        this.group = group;
        this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMsec);
        this.errCb = errCb;
    }

    public void start() {
        synchronized (this) {
            start_();
        }
    }

    public void stop() {
        synchronized (this) {
            stop_();
        }
    }

    private void start_() {
        stop_();
        stopped = false;
        timeoutFuture = group.schedule(this,
                timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private void stop_() {
        if (!stopped) {
            LOG.warn("Stopping timer.");
            stopped = true;
            if (timeoutFuture != null) {
                timeoutFuture.cancel(true);
                timeoutFuture = null;
            }
        }
    }

    /**
     * Implement this.
     * @return
     * @throws ChannelException, IOException
     */
    public abstract T doRun() throws Exception;

    private void callOnError(Exception t) throws Exception {
        if (errCb != null) {
            errCb.caughtException(t);
        } else {
            throw t;
        }
    }

    /**
     * Run loop to add or remove the server in the same context where the
     * handlers run.
     */
    public T call_() throws Exception {
        timeoutFuture = group.schedule(this, timeoutNanos,
                TimeUnit.NANOSECONDS);
        try {
            return doRun();
        } catch (Throwable t) {
            if (t instanceof Exception) {
                try {
                    callOnError((Exception)t);
                } catch (Exception exp) {
                    LOG.error("Unexpected exception, shutting down, exp: " + exp);
                    System.exit(-1);
                }
            } else {
                LOG.error("Unexpected exception, shutting down, exp: " + t);
                System.exit(-1);
            }
        }
        return null;
    }

    @Override
    public T call() throws Exception {
        synchronized (this) {
            if (stopped) {
                return null;
            }
            return call_();
        }
    }
}
