package com.quorum;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Consumer interface for a vote view, look at implementations for details.
 */
public abstract class VoteViewStreamConsumer implements VoteViewConsumer {
    /**
     * Implementation 2:
     * Every entry is a vote view, consumer can just use the last entry.
     * @param timeout how long to wait before giving up, in units of unit
     * @param unit a TimeUnit determining how to interpret the timeout parameter
     * @return Vote an incoming vote or null on timeout.
     * @throws InterruptedException
     *
     */
    public abstract Collection<Vote> consume(final int timeout,
                                             final TimeUnit unit)
            throws InterruptedException;
}
