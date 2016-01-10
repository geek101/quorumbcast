package com.quorum.helpers;

import com.common.X509Exception;
import com.quorum.QuorumBroadcast;
import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.VoteViewChange;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MockQuorumBcast implements QuorumBroadcast {
    private static final Logger LOG
            = LoggerFactory.getLogger(MockQuorumBcast.class);
    final long id;  // id equal to ensemble id
    final int quorumSize;
    final Map<Long, VoteViewChange> voteViewMap;
    final Map<Long, BitSet> meshBitSet;
    public MockQuorumBcast(final long sid, final int quorumSize) {
        this.id = sid;
        this.quorumSize = quorumSize;
        this.voteViewMap = new HashMap<>();
        this.meshBitSet = new HashMap<>();
    }

    @Override
    public long sid() {
        return this.id;
    }

    @Override
    public void broadcast(final Vote vote) {
        synchronized (this) {
            broadcast_(vote);
        }
    }

    public void addVoteViewChange(final VoteViewChange voteView) {
        synchronized (this) {
            if (voteViewMap.containsKey(voteView.getId())) {
                final String errStr = "error registering vote view for sid: " +
                        voteView.getId() + ", already exists";
                LOG.error(errStr);
                throw new RuntimeException(errStr);
            }
            voteViewMap.put(voteView.getId(), voteView);
            final BitSet bset = new BitSet(quorumSize+1);
            meshBitSet.put(voteView.getId(), bset);

            for (final long sid : voteViewMap.keySet()) {
                connect(voteView.getId(), sid);
                // rx other peer votes.
                if (sid != voteView.getId() &&
                        voteViewMap.get(sid) != null) {
                    voteView.msgRx(voteViewMap.get(sid).getSelfVote());
                }
            }

            // broadcast the vote of the new peer to everyone
            if (voteView.getSelfVote() != null) {
                broadcast_(voteView.getSelfVote());
            }
        }
    }

    public void disconnectAll(final long sid) {
        synchronized (this) {
            if (!meshBitSet.containsKey(sid)) {
                final String errStr = "invalid sid for bitset : " + sid
                        + ", quorum size: " + quorumSize;
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
            for (final long targetSid : voteViewMap.keySet()) {
                if (sid != targetSid) {
                    disconnect(sid, targetSid);
                }
            }
        }
    }

    public void connectAll(final long sid) {
        synchronized (this) {
            if (!meshBitSet.containsKey(sid)) {
                final String errStr = "invalid sid for bitset : " + sid
                        + ", quorum size: " + quorumSize;
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
            for (final long targetSid : voteViewMap.keySet()) {
                if (sid != targetSid) {
                    connect(sid, targetSid);
                }
            }
        }
    }

    public void connect(final long sid1, final long sid2) {
        setUnSetHelper(sid1, sid2, true);
    }

    public void disconnect(final long sid1, final long sid2) {
        setUnSetHelper(sid1, sid2, false);
    }

    public boolean isConnected(final long sid) {
        synchronized (this) {
            if (!meshBitSet.containsKey(sid)) {
                final String errStr = "invalid sid for bitset : " + sid
                        + ", quorum size: " + quorumSize;
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }

            final BitSet copy = (BitSet) meshBitSet.get(sid).clone();
            copy.set((int) sid, false);
            return !copy.isEmpty();
        }
    }

    public void orToSet(final long sid, final BitSet set) {
        synchronized (this) {
            if (set.size() != meshBitSet.get(sid).size()) {
                final String errStr = "invalid bitset with size: " + set.size()
                        + ", expected size: " + meshBitSet.get(sid).size();
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
            meshBitSet.get(sid).or(set);
        }
    }

    public boolean connectionExists(final long sid1, final long sid2) {
        synchronized (this) {
            return meshBitSet.get(sid1).get((int) sid2) &&
                    meshBitSet.get(sid2).get((int) sid1);
        }
    }

    private void setUnSetHelper(final long sid1, final long sid2,
                                final boolean pred) {
        synchronized (this) {
            if (!meshBitSet.containsKey(sid1) ||
                    !meshBitSet.containsKey(sid2)) {
                final String errStr = "invalid sids for bitset : " + sid1
                        + ", " + sid2 + ", quorum size: " + quorumSize;
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
            meshBitSet.get(sid1).set((int) sid2, pred);
            meshBitSet.get(sid2).set((int) sid1, pred);
            if (!pred) {
                try {
                    voteViewMap.get(sid1)
                            .msgRx(Vote.createRemoveVote(sid2)).get();
                    voteViewMap.get(sid2)
                            .msgRx(Vote.createRemoveVote(sid1)).get();
                } catch (InterruptedException | ExecutionException exp) {
                    LOG.error("failed to update vote, unhandled exp: " + exp);
                    throw new RuntimeException(exp);
                }
            }
        }
    }

    /**
     * called with lock held.
     * @param vote
     */
    private void broadcast_(final Vote vote) {
        for (final Map.Entry<Long, VoteViewChange> e : voteViewMap.entrySet()) {
            if (e.getKey() != vote.getSid() &&
                    connectionExists(vote.getSid(), e.getKey())) {
                try {
                    e.getValue().msgRx(vote).get();
                } catch (InterruptedException | ExecutionException exp) {
                    LOG.error("failed to update vote, unhandled exp: " + exp);
                    throw new RuntimeException(exp);
                }
            }
        }
    }

    @Override
    public void addServer(QuorumServer server) throws ChannelException {

    }

    @Override
    public void removeServer(QuorumServer server) throws ChannelException {

    }

    @Override
    public void start(Callback<Vote> msgRxCb, String arg1, String arg2, String arg3, String arg4, String arg5) throws IOException, ChannelException, CertificateException, NoSuchAlgorithmException, X509Exception.KeyManagerException, X509Exception.TrustManagerException {

    }

    @Override
    public void shutdown() {
    }

    @Override
    public void runNow() {

    }
}
