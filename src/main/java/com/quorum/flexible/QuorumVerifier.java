package com.quorum.flexible;

import java.util.HashSet;

public interface QuorumVerifier {
    long getWeight(long id);
    boolean containsQuorum(HashSet<Long> set);
}
