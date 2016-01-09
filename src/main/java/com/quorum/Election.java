package com.quorum;

import java.util.concurrent.ExecutionException;

public interface Election {
    Vote lookForLeader() throws ElectionException, InterruptedException,
            ExecutionException;
    Vote lookForLeader(final long peerEpoch,
                       final long zxid) throws ElectionException,
            InterruptedException, ExecutionException;
    void shutdown();
}
