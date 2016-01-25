package com.quorum.helpers;

import com.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * Imported from ZK.
 */
public class QuorumMajWrapper implements QuorumVerifier {
    private static final Logger LOG
            = LoggerFactory.getLogger(QuorumMajWrapper.class);

    int half;

    /**
     * Defines a majority to avoid computing it every time.
     *
     * @param n number of servers
     */
    public QuorumMajWrapper(int n){
        this.half = n/2;
    }

    /**
     * Returns weight of 1 by default.
     *
     * @param id
     */
    public long getWeight(long id){
        return (long) 1;
    }

    /**
     * Verifies if a set is a majority.
     */
    public boolean containsQuorum(final HashSet<Long> set){
        return set.size() > half;
    }

    @Override
    public boolean containsQuorumFromCount(final long quorumCount) {
        return quorumCount > half;
    }
}