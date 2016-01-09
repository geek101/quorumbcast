package com.quorum.helpers;

import com.quorum.Vote;
import com.quorum.VoteViewChangeConsumer;
import com.quorum.util.Predicate;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class VoteViewChangeConsumerWrapper extends VoteViewChangeConsumer {
    @Override
    public Collection<Vote> consume(int timeout, TimeUnit unit, Predicate<Collection<Vote>> changePredicate) throws InterruptedException {
        return null;
    }

    @Override
    public Collection<Vote> consume(int timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }
}
