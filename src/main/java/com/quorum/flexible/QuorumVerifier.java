package com.quorum.flexible;

import java.util.HashSet;

public interface QuorumVerifier {
    long getWeight(final long id);
    boolean containsQuorum(final HashSet<Long> set);

    boolean containsQuorumFromCount(final long quorumCount);
}
