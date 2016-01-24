package com.quorum.helpers;

import com.quorum.ElectionException;
import com.quorum.QuorumServer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EnsembleFactory {
    public static Ensemble createEnsemble(
            final String type, final long id, final int quorumSize,
            final int stableTimeout, final TimeUnit stableTimeUnit,
            final List<QuorumServer> servers,
            final Long readTimeoutMsec,
            final Long connectTimeoutMsec,
            final Long keepAliveTimeoutMsec,
            final Integer keepAliveCount,
            final String keyStoreLocation,
            final String keyStorePassword,
            final String trustStoreLocation,
            final String trustStorePassword,
            final String trustStoreCAAlias)
            throws ElectionException {
        if (type.compareToIgnoreCase("mock") == 0) {
            return new MockEnsemble(id, quorumSize, stableTimeout,
                    stableTimeUnit);
        } else if (type.compareToIgnoreCase("mockbcast") == 0) {
            return new EnsembleMockBcast(id, quorumSize, stableTimeout,
                    stableTimeUnit);
        }

        Boolean sslEnabled = null;
        if (type.compareToIgnoreCase("quorumbcast") == 0) {
            sslEnabled = false;
        } else if (type.compareToIgnoreCase("quorumbcast-ssl") == 0) {
            sslEnabled = true;
        }

        if (sslEnabled != null) {
            return new EnsembleVoteView(id, quorumSize, stableTimeout,
                    stableTimeUnit, servers, readTimeoutMsec,
                    connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                    sslEnabled, keyStoreLocation, keyStorePassword,
                    trustStoreLocation, trustStorePassword, trustStoreCAAlias);
        }

        throw new IllegalArgumentException("invalid type: " + type);
    }
}
