package com.quorum.helpers;

import com.quorum.ElectionException;

import java.util.concurrent.TimeUnit;

public class EnsembleFactory {
    public static Ensemble createEnsemble (
            final String type, final long id, final int quorumSize,
            final int stableTimeout, final TimeUnit stableTimeUnit)
            throws ElectionException {
        if (type.compareToIgnoreCase("mock") == 0) {
            return new MockEnsemble(id, quorumSize, stableTimeout,
                    stableTimeUnit);
        } else if (type.compareToIgnoreCase("mockbcast") == 0) {
            return new EnsembleMockBcast(id, quorumSize, stableTimeout,
                    stableTimeUnit);
        }

        throw new IllegalArgumentException("invalid type: " + type);
    }
}
