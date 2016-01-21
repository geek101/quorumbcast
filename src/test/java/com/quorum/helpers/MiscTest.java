package com.quorum.helpers;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiscTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(MiscTest.class);
    @Test
    public void testCombinations() {
        EnsembleHelpers.printCombinations(2, 3, LOG);
        EnsembleHelpers.printCombinations(3, 5, LOG);
    }

    @Test
    public void testPowerSet() {
        EnsembleHelpers.printPowerSet(3, LOG);
        EnsembleHelpers.printPowerSet(5, LOG);
    }

    @Test
    public void testQuorumMajorityCombinations() {
        EnsembleHelpers.printQuorumMajorityCombinations(3, LOG);
        EnsembleHelpers.printQuorumMajorityCombinations(5, LOG);
    }

    @Test
    public void testQuorumMajorityServerStateCombinations() {
        EnsembleHelpers.printQuorumMajorityServerStateCombinations(3, LOG);
        EnsembleHelpers.printQuorumMajorityServerStateCombinations(5, LOG);
    }
}
