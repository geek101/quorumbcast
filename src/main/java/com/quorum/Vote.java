/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.quorum;

import com.quorum.util.NotNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;


public class Vote {
    private static final Logger LOG = LoggerFactory.getLogger(Vote.class);

    final private int version;
    final private long leader;
    final private long zxid;
    final private long electionEpoch;
    final private long peerEpoch;
    final private long sid;
    final private QuorumPeer.ServerState state;

    private boolean removed = false;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x1;
    }

    public Vote(int version,
                long leader,
                long zxid,
                long electionEpoch,
                long peerEpoch,
                long sid,
                QuorumPeer.ServerState state) {
        this.version = version;
        this.leader = leader;
        this.zxid = zxid;
        this.electionEpoch = electionEpoch;
        this.peerEpoch = peerEpoch;
        this.sid = sid;
        this.state = state;
    }

    /**
     * when not called with explicit version arg 0x0 is used.
     * ]
     */
    public Vote(long leader,
                long zxid,
                long sid) {
        this(0x0, leader, zxid, -1, -1, sid, QuorumPeer.ServerState.LOOKING);
    }

    public Vote(long leader,
                long zxid,
                long peerEpoch,
                long sid) {
        this(0x0, leader, zxid, -1, peerEpoch, sid,
                QuorumPeer.ServerState.LOOKING);
    }

    public Vote(long leader,
                long zxid,
                long electionEpoch,
                long peerEpoch,
                long sid) {
        this(0x0, leader, zxid, electionEpoch,
                peerEpoch, sid, QuorumPeer.ServerState.LOOKING);
    }

    public Vote(long leader,
                long zxid,
                long electionEpoch,
                long peerEpoch,
                long sid,
                QuorumPeer.ServerState state) {
        this(0x0, leader, zxid, electionEpoch,
                peerEpoch, sid, state);
    }

    public int getVersion() {
        return version;
    }

    public long getLeader() {
        return leader;
    }

    public long getZxid() {
        return zxid;
    }

    public long getElectionEpoch() {
        return electionEpoch;
    }

    public long getPeerEpoch() {
        return peerEpoch;
    }

    public long getSid() {
        return sid;
    }

    public QuorumPeer.ServerState getState() {
        return state;
    }

    public boolean isRemove() {
        return removed;
    }

    public boolean setRemove() {
        boolean ret = removed;
        removed = true;
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Vote)) {
            return false;
        }
        Vote other = (Vote) o;
        
        
        /*
         * There are two things going on in the logic below.
         * First, we compare votes of servers out of election
         * using only id and peer epoch. Second, if one version
         * is 0x0 and the other isn't, then we only use the
         * leader id. This case is here to enable rolling upgrades.
         * 
         * {@see https://issues.apache.org/jira/browse/ZOOKEEPER-1805}
         */
        if ((state == QuorumPeer.ServerState.LOOKING) ||
                (other.state == QuorumPeer.ServerState.LOOKING)) {
            return (leader == other.leader
                    && zxid == other.zxid
                    && electionEpoch == other.electionEpoch
                    && peerEpoch == other.peerEpoch);
        } else {
            if ((version > 0x0) ^ (other.version > 0x0)) {
                return leader == other.leader;
            } else {
                return (leader == other.leader
                        && peerEpoch == other.peerEpoch);
            }
        }
    }

    /**
     * Used for exact match. Sid match is not done FYI.
     *
     * @param other
     * @return
     */
    public boolean match(Vote other) {
        NotNull.check(other, "vote is null, not supported", LOG);
        return this.version == other.version &&
                this.leader == other.leader &&
                this.zxid == other.zxid &&
                this.electionEpoch == other.electionEpoch &&
                this.peerEpoch == other.peerEpoch &&
                this.state == other.state;
    }

    @Override
    public int hashCode() {
        return (int) (leader & zxid);
    }

    public String toString() {
        return String.format("(leader:%d, zxid:0x%s, sid:%d, peerEpoch:0x%s, " +
                "electionEpoch:%d, state:%s)",
                leader,
                Long.toHexString(zxid),
                sid,
                Long.toHexString(peerEpoch),
                electionEpoch,
                getServerStateStr(this.state));
    }

    public static String getServerStateStr(final QuorumPeer.ServerState
                                            serverState) {
        switch(serverState) {
            case LEADING:
                return "L";
            case FOLLOWING:
                return "F";
            case LOOKING:
                return "K";
            case OBSERVING:
                return "O";
            default:
                throw new IllegalArgumentException("unknown state");
        }
    }

    /**
     * Returns a ByteBuf for Netty to write() avoids another copy.
     * We keep this efficient by writing the msglen at the beginning.
     * TODO: Use pooled buffers.
     *
     * @param state
     * @param leader
     * @param zxid
     * @param electionEpoch
     * @param epoch
     * @return ByteBuf including the msglen at the beginning.
     */
    public static ByteBuf buildMsg(int state, long leader, long zxid,
                                   long electionEpoch, long epoch, long sid,
                                   int version) {
        ByteBuf requestBuffer = Unpooled.buffer(40 + Integer.BYTES);

        /*
         * Building notification packet to send.
         * Write size first then the data.
         */
        requestBuffer.writeInt(Integer.BYTES /* state */ +
                Long.BYTES    /* leader */ +
                Long.BYTES    /* zxid */ +
                Long.BYTES    /* electionEpoch */ +
                Long.BYTES    /* epoch */ +
                Long.BYTES    /* sid */ +
                Integer.BYTES /* CURRENTVERSION */);
        requestBuffer.writeInt(state);
        requestBuffer.writeLong(leader);
        requestBuffer.writeLong(zxid);
        requestBuffer.writeLong(electionEpoch);
        requestBuffer.writeLong(epoch);
        requestBuffer.writeLong(sid);
        requestBuffer.writeInt(version);
        return requestBuffer;
    }

    /**
     * Vote to message on wire.
     *
     * @return
     */
    public ByteBuf buildMsg() {
        return Vote.buildMsg(
                this.state.ordinal(), this.leader, this.zxid,
                this.electionEpoch, this.peerEpoch, this.sid, this.version);
    }

    /**
     * Build a vote given a message, expects msglen at the beginning do
     * not forget to add it.
     *
     * @param b ByteBuf msg
     * @return Vote received
     */
    public static Vote buildVote(ByteBuf b) {
        int remainder = b.readInt();
        if (b.readableBytes() < remainder) {
            LOG.error("Invalid vote received!. Size: {}, expected: {}",
                    b.readableBytes(), remainder);
            return null;
        }

        // State of peer that sent this message
        QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
        switch (b.readInt()) {
            case 0:
                ackstate = QuorumPeer.ServerState.LOOKING;
                break;
            case 1:
                ackstate = QuorumPeer.ServerState.FOLLOWING;
                break;
            case 2:
                ackstate = QuorumPeer.ServerState.LEADING;
                break;
            case 3:
                ackstate = QuorumPeer.ServerState.OBSERVING;
                break;
            default:
                LOG.error("Invalid vote received!. Size: {}, expected: {}",
                        b.readableBytes(), remainder);
                return null;
        }

        long id = b.readLong();            // Leader
        long zxid = b.readLong();
        long electionEpoch = b.readLong();
        long peerEpoch = b.readLong();
        long sid = b.readLong();
        int version = b.readInt();
        return new Vote(version, id, zxid, electionEpoch, peerEpoch, sid,
                ackstate);
    }

    public static Vote createRemoveVote(final long sid) {
        Vote v = new Vote(-1, -1, sid);
        v.setRemove();
        return v;
    }

    public Vote copy() {
        final Vote v = new Vote(this.getVersion(), this.getLeader(),
                this.getZxid(), this.getElectionEpoch(), this.getPeerEpoch(),
                this.getSid(), this.getState());
        if (this.isRemove()) {
            v.setRemove();
        }

        return v;
    }

    /**
     * Used by QuorumPeer to reflect its state
     *
     * @return new Vote
     */
    public Vote setServerState(final QuorumPeer.ServerState serverState) {
        return new Vote(this.getVersion(), this.getLeader(), this.getZxid(),
                this.getElectionEpoch(), this.getPeerEpoch(), this.getSid(),
                serverState);
    }

    /**
     * Used when entering leader election. Vote will elect itself.
     * @param peerEpoch start the vote with last agreed peer epoch
     * @param zxid start the vote with current zxid.
     * @return a new Vote.
     */
    public Vote leaderElectionVote(final long peerEpoch, final long zxid) {
        return new Vote(this.getVersion(), this.getSid(), zxid,
                this.getElectionEpoch()+1, peerEpoch, this.getSid(),
                QuorumPeer.ServerState.LOOKING);
    }
    /**
     * Used by leader election to  increase electionEpoch of other Vote.
     *
     * @return new Vote
     */
    public Vote increaseElectionEpoch() {
        return new Vote(this.getVersion(), this.getLeader(), this.getZxid(),
                this.getElectionEpoch() + 1, this.getPeerEpoch(), this.getSid(),
                this.getState());
    }

    /**
     * Used by leader election to logicalClock of other Vote.
     *
     * @param other Vote of the peer.
     * @return new Vote
     */
    public Vote setElectionEpoch(final Vote other) {
        return new Vote(this.getVersion(), this.getLeader(), this.getZxid(),
                other.getElectionEpoch(), this.getPeerEpoch(), this.getSid(),
                this.getState());
    }

    /**
     * Update our vote to use peer's
     *
     * @param other Vote of the peer
     * @return new Vote
     */
    public Vote catchUpToVote(final Vote other) {
        return new Vote(this.getVersion(), other.getLeader(), other.getZxid(),
                this.getElectionEpoch(), other.getPeerEpoch(), this.getSid(),
                this.getState());
    }

    public Vote catchUpToLeaderVote(final Vote leader,
                                    final QuorumPeer.ServerState serverState) {
        return new Vote(this.getVersion(), leader.getLeader(), leader.getZxid(),
                leader.getElectionEpoch(), leader.getPeerEpoch(), this.getSid(),
                serverState);
    }
    public Vote setSelfAsLeader() {
        return new Vote(this.getVersion(), this.getSid(), this.getZxid(),
                this.getElectionEpoch(), this.getPeerEpoch(), this.getSid(),
                this.getState());
    }

    /**
     * Used by QuorumPeer only to help with setting current vote.
     *
     * @param other
     * @param sid
     * @return
     */
    public static Vote quorumPeerVoteSet(final Vote other, final long sid) {
        return new Vote(other.getVersion(), other.getLeader(), other.getZxid(),
                other.getElectionEpoch(), other.getPeerEpoch(), sid,
                other.getState());
    }

    public Vote makeMeLeader(final Random random) {
        return new Vote(this.getVersion(), this.getSid(),
                random.nextInt(9999999),
                random.nextInt(9999999),
                random.nextInt(9999999), this.getSid(),
                QuorumPeer.ServerState.LEADING);
    }

    public Vote makeMeFollower(final Vote leaderVote, final Random random) {
        return new Vote(this.getVersion(), leaderVote.getLeader(),
                leaderVote.getZxid(),
                leaderVote.getElectionEpoch(),
                leaderVote.getPeerEpoch(), this.getSid(),
                QuorumPeer.ServerState.FOLLOWING);
    }

    public Vote makeMeLooker(final Vote totalOrderVote, final Random random) {
        final long newZxid = totalOrderVote != null ?
                random.nextInt(9999999) % (totalOrderVote.getZxid()+1L) :
                random.nextInt(9999999);

        final long newPeerEpoch = totalOrderVote != null ?
                random.nextInt(9999999) % (totalOrderVote.getPeerEpoch()+1L) :
                random.nextInt(9999999);

        final long electionEpoch = totalOrderVote != null ?
                totalOrderVote.getPeerEpoch() + 1L : 0;
        return new Vote(this.getVersion(), this.getSid(),
                // upto but not higher than leader's Zxid
                newZxid,
                // TODO: duh! deal with this later!!!!.
                electionEpoch,
                // upto but not higher than leader's peerEpoch
                newPeerEpoch,
                this.getSid(), QuorumPeer.ServerState.LOOKING);
    }

    public Vote breakFromLeader() {
        return setServerState(QuorumPeer.ServerState.LOOKING);
    }
}
