package com.quorum.helpers;

import com.quorum.ElectionException;
import com.quorum.QuorumPeer;
import com.quorum.Vote;
import com.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class EnsembleMockBcast extends AbstractEnsemble {
    private static final Logger LOG
            = LoggerFactory.getLogger(EnsembleMockBcast.class);

    private MockQuorumBcast mockQuorumBcast;

    private FLEV2Wrapper fleThatRan;
    public EnsembleMockBcast(final long id, final int quorumSize,
                             final int stableTimeout,
                             final TimeUnit stableTimeoutUnit)
            throws ElectionException {
        super(id, quorumSize, stableTimeout, stableTimeoutUnit);
        mockQuorumBcast = new MockQuorumBcast(getId(), quorumSize);
    }

    public EnsembleMockBcast(final Ensemble parentEnsemble,
                             final int stableTimeout,
                             final TimeUnit stableTimeoutUnit)
            throws ElectionException {
        super(parentEnsemble, stableTimeout, stableTimeoutUnit);
        mockQuorumBcast = new MockQuorumBcast(getId(),
                parentEnsemble.getQuorumSize());
    }

    @Override
    public Ensemble createEnsemble(
            final Ensemble parentEnsemble) throws ElectionException {
        return new EnsembleMockBcast(parentEnsemble,
                stableTimeout, stableTimeoutUnit);
    }

    @Override
    protected void runLookingForSid(final long sid) throws InterruptedException,
            ExecutionException, ElectionException {
        super.runLookingForSid(sid);
        fleThatRan = fles.get(sid);

        for (final FLEV2Wrapper f : fles.values()) {
            if (f.getId() != sid &&
                    f.getState() == QuorumPeer.ServerState.LOOKING
                    && isConnected(f.getId())) {
                futuresForLookingPeers.put(f.getId(),
                        f.runLeaderElection(getVotes()));
            }
        }

        futuresForLookingPeers.put(fleThatRan.getId(),
                        fleThatRan.runLeaderElection(getVotes()));
    }

    @Override
    public FLEV2Wrapper getFleThatRan() {
        return fleThatRan;
    }

    public FLEV2Wrapper disconnect(final long serverSid)
            throws ElectionException {
        if (!fles.containsKey(serverSid)) {
            throw new ElectionException("no server for sid: " + serverSid);
        }

        mockQuorumBcast.disconnectAll(serverSid);
        return fles.get(serverSid);
    }

    public FLEV2Wrapper connect(final long serverSid) throws ElectionException {
        if (!fles.containsKey(serverSid)) {
            throw new ElectionException("no server for sid: " + serverSid);
        }

        mockQuorumBcast.connectAll(serverSid);
        return fles.get(serverSid);
    }

    public boolean isConnected(final long serverSid) {
        return mockQuorumBcast.isConnected(serverSid);
    }

    @Override
    protected FLEV2Wrapper createFLEV2(
            final long sid, final QuorumVerifier quorumVerifier) {
        if (mockQuorumBcast == null) {
            mockQuorumBcast = new MockQuorumBcast(getId(), getQuorumSize());
        }
        return createFLEV2BcastWrapper(sid, quorumVerifier);
    }


    @Override
    protected FLEV2Wrapper copyFLEV2(final FLEV2Wrapper fle,
                                     final Vote vote)
            throws InterruptedException, ExecutionException {
        if (!(fle instanceof FLEV2BcastWrapper)) {
            throw new IllegalArgumentException("fle is not of MockBcast " +
                    "type");
        }
        assert vote != null;
        FLEV2BcastWrapper flev2BcastWrapper = createFLEV2BcastWrapper(fle
                .getId(), ((FLEV2BcastWrapper) fle).getQuorumVerifier());
        flev2BcastWrapper.updateSelfVote(vote).get();
        return flev2BcastWrapper;
    }

    private FLEV2BcastWrapper createFLEV2BcastWrapper(
            final long sid, final QuorumVerifier quorumVerifier) {
        final VoteViewMockBcast voteViewMockBcast
                = new VoteViewMockBcast(sid, mockQuorumBcast);
        return new FLEV2BcastWrapper(sid, QuorumPeer.LearnerType.PARTICIPANT,
                quorumVerifier, voteViewMockBcast,
                voteViewMockBcast, stableTimeout, stableTimeoutUnit);
    }
}
