package com.quorum.helpers;

import com.quorum.QuorumPeer;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class MiscTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(MiscTest.class);
    @Test
    public void testCombinations() {
        Ensemble.printCombinations(2, 3, LOG);
        Ensemble.printCombinations(3, 5, LOG);
    }

    @Test
    public void testPowerSet() {
        Ensemble.printPowerSet(3, LOG);
        Ensemble.printPowerSet(5, LOG);
    }

    @Test
    public void testQuorumMajorityCombinations() {
        Ensemble.printQuorumMajorityCombinations(3, LOG);
        Ensemble.printQuorumMajorityCombinations(5, LOG);
    }

    @Test
    public void testQuorumMajorityServerStateCombinations() {
        Ensemble.printQuorumMajorityServerStateCombinations(3, LOG);
        Ensemble.printQuorumMajorityServerStateCombinations(5, LOG);
    }
}
